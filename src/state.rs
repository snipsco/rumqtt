use error::*;
use mqtt3;
use MqttOptions;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttConnectionStatus {
    /// connection opened, waiting for CONNACK message
    Handshake { initial: bool },
    /// normal running state
    Connected,
    /// client will seek reconnection
    WantConnect { when: Instant },
    /// client has been instructed to disconnect from the API
    WantDisconnect,
    /// client is disconnected and will not seek reconnection for now
    Disconnected,
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    iat: i64,
    exp: i64,
    aud: String,
}

pub struct MqttState {
    opts: MqttOptions,

    // --------  State  ----------
    connection_status: MqttConnectionStatus,
    await_pingresp: bool,
    last_flush: Instant,
    last_pkid: mqtt3::PacketIdentifier,

    // For QoS 1. Stores outgoing publishes
    outgoing_pub: VecDeque<mqtt3::Publish>,
    // clean_session=false will remember subscriptions only till lives.
    // Even so, if broker crashes, all its state will be lost (most brokers).
    // client should resubscribe it comes back up again or else the data will
    // be lost
    subscriptions: HashMap<usize, ::client::Subscription>,
    path_usage: HashMap<String, usize>,
}

/// Design: `MqttState` methods will just modify the state of the object
///         but doesn't do any network operations. Methods will do
///         appropriate returns so that n/w methods or n/w eventloop can
///         operate directly. This abstracts the functionality better
///         so that it's easy to switch between synchronous code, tokio (or)
///         async/await

impl MqttState {
    pub fn new(opts: MqttOptions) -> Self {
        MqttState {
            opts: opts,
            connection_status: MqttConnectionStatus::WantConnect { when: Instant::now() },
            await_pingresp: false,
            last_flush: Instant::now(),
            last_pkid: mqtt3::PacketIdentifier(0),
            outgoing_pub: VecDeque::new(),
            subscriptions: HashMap::new(),
            path_usage: HashMap::new(),
        }
    }

    pub fn opts(&self) -> &MqttOptions {
        &self.opts
    }

    pub fn status(&self) -> MqttConnectionStatus {
        self.connection_status
    }

    pub fn handle_outgoing_connect(&mut self, initial: bool) -> mqtt3::Connect {
        let keep_alive = if let Some(keep_alive) = self.opts.keep_alive {
            keep_alive
        } else {
            0
        };

        self.opts.keep_alive = Some(keep_alive);
        self.connection_status = MqttConnectionStatus::Handshake { initial };

        mqtt3::Connect {
            protocol: mqtt3::Protocol::MQTT(4),
            keep_alive,
            client_id: self.opts.client_id.clone(),
            clean_session: self.opts.clean_session,
            last_will: None,
            username: self.opts.username.clone(),
            password: self.opts.password.clone(),
        }
    }

    fn set_status_after_error(&mut self) {
        use self::MqttConnectionStatus::*;
        use ReconnectOptions::*;
        match (self.connection_status, self.opts.reconnect) {
            (Handshake { initial: true }, Always(d))
            | (Handshake { .. }, AfterFirstSuccess(d))
            | (Connected, AfterFirstSuccess(d))
            | (Connected, Always(d))
            | (WantConnect { .. }, AfterFirstSuccess(d))
            | (WantConnect { .. }, Always(d))
            => self.connection_status = WantConnect { when: Instant::now() + d },
            _ => self.connection_status = Disconnected
        }
    }

    pub fn handle_incoming_connack(&mut self, connack: mqtt3::Connack) -> Result<()> {
        let response = connack.code;
        if response != mqtt3::ConnectReturnCode::Accepted {
            self.set_status_after_error();
            Err(ErrorKind::Connack(response))?
        } else {
            self.connection_status = MqttConnectionStatus::Connected;
            if self.opts.clean_session {
                self.clear_session_info();
            }

            Ok(())
        }
    }

    pub fn handle_reconnection(&mut self) -> Option<(Option<mqtt3::Subscribe>, VecDeque<mqtt3::Publish>)> {
        self.await_pingresp = false;
        self.reset_last_control_at();
        if self.opts.clean_session {
            None
        } else {
            let sub = if self.subscriptions.len() > 0 {
                Some(mqtt3::Subscribe {
                    pid: self.next_pkid(),
                    topics: self.subscriptions.iter().map(|(_id, s)| {
                        ::mqtt3::SubscribeTopic {
                            topic_path: s.topic_path.path.clone(),
                            qos: s.qos,
                        }
                    }).collect(),
                })
            } else {
                None
            };
            Some((sub, self.outgoing_pub.clone()))
        }
    }

    /// Sets next packet id if pkid is None (fresh publish) and adds it to the
    /// outgoing publish queue
    pub fn handle_outgoing_publish(
        &mut self,
        mut publish: mqtt3::Publish,
    ) -> Result<mqtt3::Publish> {
        if publish.payload.len() > self.opts.max_packet_size {
            Err(ErrorKind::PacketSizeLimitExceeded)?
        }
        let publish = match publish.qos {
            mqtt3::QoS::AtMostOnce => publish,
            mqtt3::QoS::AtLeastOnce => {
                // add pkid if None
                let publish = if publish.pid == None {
                    let pkid = self.next_pkid();
                    publish.pid = Some(pkid);
                    publish
                } else {
                    publish
                };

                self.outgoing_pub.push_back(publish.clone());
                publish
            }
            _ => unimplemented!(),
        };

        if self.connection_status == MqttConnectionStatus::Connected {
            self.reset_last_control_at();
            Ok(publish)
        } else {
            Err(ErrorKind::InvalidState.into())
        }
    }

    pub fn handle_incoming_puback(
        &mut self,
        pkid: mqtt3::PacketIdentifier,
    ) -> Result<mqtt3::Publish> {
        if let Some(index) = self.outgoing_pub.iter().position(|x| x.pid == Some(pkid)) {
            Ok(self.outgoing_pub.remove(index).unwrap())
        } else {
            error!("Unsolicited PUBLISH packet: {:?}", pkid);
            Err(ErrorKind::InvalidState.into())
        }
    }

    // return a tuple. tuple.0 is supposed to be send to user through 'notify_tx' while tuple.1
    // should be sent back on network as ack
    pub fn handle_incoming_publish(
        &mut self,
        publish: mqtt3::Publish,
    ) -> Result<(Option<mqtt3::Publish>, Option<mqtt3::Packet>)> {
        let pkid = publish.pid;
        let qos = publish.qos;

        let concrete = ::mqtt3::TopicPath::from_str(&publish.topic_name)?;
        for (_id, sub) in &self.subscriptions {
            if sub.topic_path.is_match(&concrete) {
                (sub.callback)(&publish);
            }
        }

        Ok(match qos {
            mqtt3::QoS::AtMostOnce => (Some(publish), None),
            mqtt3::QoS::AtLeastOnce => (Some(publish), Some(mqtt3::Packet::Puback(pkid.unwrap()))),
            mqtt3::QoS::ExactlyOnce => unimplemented!(),
        })
    }

    // reset the last control packet received time
    pub fn reset_last_control_at(&mut self) {
        self.last_flush = Instant::now();
    }

    // check if pinging is required based on last flush time
    pub fn is_ping_required(&self) -> bool {
        if self.connection_status != MqttConnectionStatus::Connected {
            return false;
        }
        if let Some(keep_alive) = self.opts.keep_alive {
            let keep_alive = Duration::new(f32::ceil(0.9 * f32::from(keep_alive)) as u64, 0);
            self.last_flush.elapsed() > keep_alive
        } else {
            false
        }
    }

    // check when the last control packet/pingreq packet
    // is received and return the status which tells if
    // keep alive time has exceeded
    // NOTE: status will be checked for zero keepalive times also
    pub fn handle_outgoing_ping(&mut self) -> Result<()> {
        let keep_alive = self.opts.keep_alive.expect("No keep alive");

        let elapsed = self.last_flush.elapsed();
        if elapsed >= Duration::new(u64::from(keep_alive + 1), 0) {
            debug!("Might be too late for ping");
            return Err(ErrorKind::InvalidState.into());
        }
        // @ Prevents half open connections. Tcp writes will buffer up
        // with out throwing any error (till a timeout) when internet
        // is down. Eventhough broker closes the socket after timeout,
        // EOF will be known only after reconnection.
        // We need to unbind the socket if there in no pingresp before next ping
        // (What about case when pings aren't sent because of constant publishes
        // ?. A. Tcp write buffer gets filled up and write will be blocked for 10
        // secs and then error out because of timeout.)
        if self.await_pingresp {
            debug!("Already expecting a pingresp");
            return Err(ErrorKind::InvalidState.into());
        }

        if self.connection_status == MqttConnectionStatus::Connected {
            debug!("Expecting pingresp");
            self.last_flush = Instant::now();
            self.await_pingresp = true;
            Ok(())
        } else {
            error!(
                "State = {:?}. Shouldn't ping in this state",
                self.connection_status
            );
            Err(ErrorKind::InvalidState.into())
        }
    }

    pub fn handle_incoming_pingresp(&mut self) {
        self.await_pingresp = false;
    }

    pub fn handle_outgoing_subscribe(
        &mut self,
        subs: Vec<::client::Subscription>,
    ) -> Result<mqtt3::Subscribe> {
        let pkid = self.next_pkid();
        let topics = subs.iter()
            .map(|s| {
                ::mqtt3::SubscribeTopic {
                    topic_path: s.topic_path.path.clone(),
                    qos: s.qos,
                }
            })
            .collect();
        for s in &subs {
            *self.path_usage.entry(s.topic_path.path.clone()).or_insert(0) += 1;
        }
        self.subscriptions.extend(subs.into_iter().map(|it| {
            (it.id, it)
        }));

        if self.connection_status == MqttConnectionStatus::Connected {
            Ok(mqtt3::Subscribe { pid: pkid, topics })
        } else {
            error!(
                "State = {:?}. Shouldn't subscribe in this state",
                self.connection_status
            );
            Err(ErrorKind::InvalidState.into())
        }
    }

    pub fn handle_outgoing_unsubscribe(
        &mut self,
        ids: Vec<usize>,
    ) -> Result<Option<mqtt3::Unsubscribe>> {
        let mut topics = vec![];
        for id in ids {
            if let Some(sub) = self.subscriptions.remove(&id) {
                // we unwrap here because if the value is not there, there is an error in this code
                let mut path_count = self.path_usage.get_mut(&sub.topic_path.path).unwrap();
                *path_count -= 1;
                if *path_count == 0 { topics.push(sub.topic_path.path) }
            }
        }
        if !topics.is_empty() {
            let pkid = self.next_pkid();

            if self.connection_status == MqttConnectionStatus::Connected {
                Ok(Some(mqtt3::Unsubscribe { pid: pkid, topics }))
            } else {
                error!(
                    "State = {:?}. Shouldn't unsubscribe in this state",
                    self.connection_status
                );
                Err(ErrorKind::InvalidState.into())
            }
        } else {
            Ok(None)
        }
    }

    pub fn handle_incoming_suback(&mut self, ack: mqtt3::Suback) -> Result<()> {
        if ack.return_codes
            .iter()
            .any(|v| *v == ::mqtt3::SubscribeReturnCodes::Failure)
            {
                Err(format!("rejected subscription"))?
            };
        Ok(())
    }

    pub fn handle_incoming_unsuback(&mut self, ack: mqtt3::PacketIdentifier) -> Result<()> {
        Ok(())
    }

    pub fn handle_socket_disconnect(&mut self) {
        self.await_pingresp = false;
        self.set_status_after_error();

        // remove all the state
        if self.opts.clean_session {
            self.clear_session_info();
        }
    }

    fn clear_session_info(&mut self) {
        self.outgoing_pub.clear();
    }

    // http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
    fn next_pkid(&mut self) -> mqtt3::PacketIdentifier {
        let mqtt3::PacketIdentifier(mut pkid) = self.last_pkid;
        if pkid == 65_535 {
            pkid = 0;
        }
        self.last_pkid = mqtt3::PacketIdentifier(pkid + 1);
        self.last_pkid
    }
}

#[cfg(test)]
mod test {
    use error::*;
    use mqtt3::*;
    use options::MqttOptions;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use super::{MqttConnectionStatus, MqttState};

    #[test]
    fn next_pkid_roll() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));
        let mut pkt_id = PacketIdentifier(0);
        for _ in 0..65536 {
            pkt_id = mqtt.next_pkid();
        }
        assert_eq!(PacketIdentifier(1), pkt_id);
    }

    #[test]
    fn outgoing_publish_handle_should_set_pkid_correctly_and_add_publish_to_queue_correctly() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));
        mqtt.connection_status = MqttConnectionStatus::Connected;

        // QoS0 Publish
        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            pid: None,
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![1, 2, 3]),
        };

        let publish_out = mqtt.handle_outgoing_publish(publish);
        // pkid shouldn't be added
        assert_eq!(publish_out.unwrap().pid, None);
        // publish shouldn't added to queue
        assert_eq!(mqtt.outgoing_pub.len(), 0);


        // QoS1 Publish
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            pid: None,
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![1, 2, 3]),
        };

        let publish_out = mqtt.handle_outgoing_publish(publish.clone());
        // pkid shouldn't be added
        assert_eq!(publish_out.unwrap().pid, Some(PacketIdentifier(1)));
        // publish shouldn't added to queue
        assert_eq!(mqtt.outgoing_pub.len(), 1);

        let publish_out = mqtt.handle_outgoing_publish(publish.clone());
        // pkid shouldn't be added
        assert_eq!(publish_out.unwrap().pid, Some(PacketIdentifier(2)));
        // publish shouldn't added to queue
        assert_eq!(mqtt.outgoing_pub.len(), 2);
    }

    macro_rules! assert_error_kind {
        ($expr:expr, $kind:path) => {
            match $expr {
                Err(e) => match e {
                    Error($kind, _) => {}
                    e => panic!("Wrong error. Expected {:?}, got {:?}", $kind, e),
                }
                Ok(_) => panic!("Expected error {:?}, got ok.", $kind),
            }
        }
    }

    #[test]
    fn outgoing_publish_handle_should_throw_error_in_invalid_state() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            pid: None,
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![1, 2, 3]),
        };

        let publish_out = mqtt.handle_outgoing_publish(publish);
        assert_error_kind!(publish_out, ErrorKind::InvalidState);
    }

    #[test]
    fn outgoing_publish_handle_should_throw_error_when_packetsize_exceeds_max() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));

        let publish = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            pid: None,
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![0; 101 * 1024]),
        };

        let err = mqtt.handle_outgoing_publish(publish);
        assert_error_kind!(err, ErrorKind::PacketSizeLimitExceeded);
    }

    #[test]
    fn incoming_puback_should_remove_correct_publish_from_queue() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));
        // QoS1 Publish
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            pid: None,
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![1, 2, 3]),
        };

        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish);

        let publish = mqtt.handle_incoming_puback(PacketIdentifier(1)).unwrap();
        assert_eq!(publish.pid, Some(PacketIdentifier(1)));
        assert_eq!(mqtt.outgoing_pub.len(), 2);

        let publish = mqtt.handle_incoming_puback(PacketIdentifier(2)).unwrap();
        assert_eq!(publish.pid, Some(PacketIdentifier(2)));
        assert_eq!(mqtt.outgoing_pub.len(), 1);

        let publish = mqtt.handle_incoming_puback(PacketIdentifier(3)).unwrap();
        assert_eq!(publish.pid, Some(PacketIdentifier(3)));
        assert_eq!(mqtt.outgoing_pub.len(), 0);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_errors_during_invalid_state() {
        // 1. test for invalid state
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));
        mqtt.opts.keep_alive = Some(5);
        thread::sleep(Duration::new(5, 0));
        assert_error_kind!(mqtt.handle_outgoing_ping(), ErrorKind::InvalidState);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_errors_for_no_pingresp() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));
        mqtt.opts.keep_alive = Some(5);
        mqtt.connection_status = MqttConnectionStatus::Connected;
        thread::sleep(Duration::new(5, 0));
        // should ping
        mqtt.handle_outgoing_ping().unwrap();
        thread::sleep(Duration::new(5, 0));
        // should throw error because we didn't get pingresp for previous ping
        assert_error_kind!(mqtt.handle_outgoing_ping(), ErrorKind::InvalidState);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_error_if_ping_time_exceeded() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));
        mqtt.opts.keep_alive = Some(5);
        mqtt.connection_status = MqttConnectionStatus::Connected;
        thread::sleep(Duration::new(7, 0));
        assert_error_kind!(mqtt.handle_outgoing_ping(), ErrorKind::InvalidState);
    }

    #[test]
    fn outgoing_ping_handle_should_succeed_if_pingresp_is_received() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));
        mqtt.opts.keep_alive = Some(5);
        mqtt.connection_status = MqttConnectionStatus::Connected;
        thread::sleep(Duration::new(5, 0));
        // should ping
        mqtt.handle_outgoing_ping().unwrap();
        mqtt.handle_incoming_pingresp();
        thread::sleep(Duration::new(5, 0));
        // should ping
        mqtt.handle_outgoing_ping().unwrap();
    }

    #[test]
    fn disconnect_handle_should_reset_everything_in_clean_session() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883").set_clean_session(true));
        mqtt.await_pingresp = true;
        // QoS1 Publish
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            pid: None,
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![1, 2, 3]),
        };

        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish);

        mqtt.handle_socket_disconnect();
        assert_eq!(mqtt.outgoing_pub.len(), 0);
        match mqtt.connection_status {
            MqttConnectionStatus::WantConnect { .. } => {}
            _ => panic!()
        }
        assert_eq!(mqtt.await_pingresp, false);
    }

    #[test]
    fn disconnect_handle_should_reset_everything_except_queues_in_persistent_session() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));
        mqtt.await_pingresp = true;
        mqtt.opts.clean_session = false;
        // QoS1 Publish
        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            pid: None,
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![1, 2, 3]),
        };

        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish);

        mqtt.handle_socket_disconnect();
        assert_eq!(mqtt.outgoing_pub.len(), 3);
        match mqtt.connection_status {
            MqttConnectionStatus::WantConnect { .. } => {}
            _ => panic!()
        }
        assert_eq!(mqtt.await_pingresp, false);
    }

    #[test]
    fn connection_status_is_valid_while_handling_connect_and_connack_packets() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));

        match mqtt.connection_status {
            MqttConnectionStatus::WantConnect { .. } => {}
            _ => panic!()
        }
        mqtt.handle_outgoing_connect(true);
        assert_eq!(
            mqtt.connection_status,
            MqttConnectionStatus::Handshake { initial: true }
        );

        let connack = Connack {
            session_present: false,
            code: ConnectReturnCode::Accepted,
        };

        mqtt.handle_incoming_connack(connack).unwrap();
        assert_eq!(mqtt.connection_status, MqttConnectionStatus::Connected);

        let connack = Connack {
            session_present: false,
            code: ConnectReturnCode::BadUsernamePassword,
        };

        assert!(mqtt.handle_incoming_connack(connack).is_err());
        match mqtt.connection_status {
            MqttConnectionStatus::WantConnect { .. } => {}
            _ => panic!()
        }
    }

    #[test]
    fn connack_handle_should_not_return_list_of_incomplete_messages_to_be_sent_in_clean_session() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883").set_clean_session(true));

        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            pid: None,
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![1, 2, 3]),
        };

        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish);

        let connack = Connack {
            session_present: false,
            code: ConnectReturnCode::Accepted,
        };

        mqtt.handle_incoming_connack(connack).unwrap();
        assert_eq!(None, mqtt.handle_reconnection());
    }

    #[test]
    fn connack_handle_should_return_list_of_incomplete_messages_to_be_sent_in_persistent_session() {
        let mut mqtt = MqttState::new(MqttOptions::new("test-id", "127.0.0.1:1883"));
        mqtt.opts.clean_session = false;

        let publish = Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            pid: None,
            topic_name: "hello/world".to_owned(),
            payload: Arc::new(vec![1, 2, 3]),
        };

        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish.clone());
        let _ = mqtt.handle_outgoing_publish(publish);

        let connack = Connack {
            session_present: false,
            code: ConnectReturnCode::Accepted,
        };

        if let Ok(_) = mqtt.handle_incoming_connack(connack) {
            if let Some(v) = mqtt.handle_reconnection() {
                assert_eq!(v.1.len(), 3);
            } else {
                panic!("Should return publishes to be retransmitted");
            }
        }
    }
}
