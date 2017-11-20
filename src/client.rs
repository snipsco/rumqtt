use error::*;

use mqtt3::{QoS, ToTopicPath, TopicPath};

use mio_more::channel::*;

use MqttOptions;

#[derive(DebugStub)]
pub enum Command {
    Alive(#[debug_stub = ""] ::std::sync::mpsc::Sender<()>),
    Subscribe(Subscription),
    Publish(Publish),
    Connect,
    Disconnect,
}

pub struct MqttClient {
    nw_request_tx: SyncSender<Command>,
}

impl MqttClient {
    /// Connects to the broker and starts an event loop in a new thread.
    /// Returns 'Command' and handles reqests from it.
    /// Also handles network events, reconnections and retransmissions.
    pub fn start(opts: MqttOptions) -> Result<Self> {
        use options::ReconnectOptions;
        let (commands_tx, commands_rx) = sync_channel(10);
        // This thread handles network reads (coz they are blocking) and
        // and sends them to event loop thread to handle mqtt state.
        let mut connection = ::connection::start(opts, commands_rx)?;
        ::std::thread::spawn(move || loop {
            'inner: loop {
                match connection.turn(None) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Disconnected: ({:?})", e);
                        break 'inner;
                    }
                }
            }
            let d = match connection.state().opts().reconnect {
                ReconnectOptions::Never => break,
                ReconnectOptions::Always(d) | ReconnectOptions::AfterFirstSuccess(d) => {
                    info!("Will try to reconnect in {:?}", d);
                    ::std::thread::sleep(d);
                    d
                }
            };
            loop {
                info!("Try to reconnect");
                match connection.reconnect() {
                    Ok(_) => break,
                    Err(e) => {
                        error!("Will retry to reconnect in {:?} ({:?})", d, e);
                        ::std::thread::sleep(d);
                    }
                }
            }
            info!("Reconnected");
        });

        Ok(MqttClient {
            nw_request_tx: commands_tx,
        })
    }

    pub fn subscribe<T: ToTopicPath>(
        &mut self,
        topic_path: T,
        callback: SubscriptionCallback,
    ) -> Result<SubscriptionBuilder> {
        Ok(SubscriptionBuilder {
            client: self,
            it: Subscription {
                id: None,
                topic_path: topic_path.to_topic_path()?,
                qos: ::mqtt3::QoS::AtMostOnce,
                callback,
            },
        })
    }

    pub fn publish<T: ToTopicPath>(&mut self, topic_path: T) -> Result<PublishBuilder> {
        Ok(PublishBuilder {
            client: self,
            it: Publish {
                topic: topic_path.to_topic_path()?,
                qos: ::mqtt3::QoS::AtMostOnce,
                payload: vec![],
                retain: false,
            },
        })
    }

    pub fn alive(&mut self) -> Result<()> {
        let (tx, rx) = ::std::sync::mpsc::channel();
        self.send_command(Command::Alive(tx))?;
        Ok(rx.recv().map_err(|_| "Client thread looks dead")?)
    }

    fn send_command(&mut self, command: Command) -> Result<()> {
        self.nw_request_tx
            .send(command)
            .map_err(|_| "failed to send mqtt command to client thread")?;
        Ok(())
    }
}

pub type SubscriptionCallback = Box<Fn(&::mqtt3::Publish) + Send>;

#[derive(DebugStub)]
pub struct Subscription {
    pub id: Option<String>,
    pub topic_path: TopicPath,
    pub qos: ::mqtt3::QoS,
    #[debug_stub = ""] pub callback: SubscriptionCallback,
}

#[must_use]
pub struct SubscriptionBuilder<'a> {
    client: &'a mut MqttClient,
    it: Subscription,
}

impl<'a> SubscriptionBuilder<'a> {
    pub fn id<S: ToString>(self, s: S) -> SubscriptionBuilder<'a> {
        let SubscriptionBuilder { client, it } = self;
        SubscriptionBuilder {
            client,
            it: Subscription {
                id: Some(s.to_string()),
                ..it
            },
        }
    }
    pub fn qos(self, qos: QoS) -> SubscriptionBuilder<'a> {
        let SubscriptionBuilder { client, it } = self;
        SubscriptionBuilder {
            client,
            it: Subscription { qos, ..it },
        }
    }
    pub fn send(self) -> Result<()> {
        self.client.send_command(Command::Subscribe(self.it))
    }
}

#[derive(Debug)]
pub struct Publish {
    pub topic: TopicPath,
    pub qos: ::mqtt3::QoS,
    pub payload: Vec<u8>,
    pub retain: bool,
}

#[must_use]
pub struct PublishBuilder<'a> {
    client: &'a mut MqttClient,
    it: Publish,
}

impl<'a> PublishBuilder<'a> {
    pub fn payload(self, payload: Vec<u8>) -> PublishBuilder<'a> {
        let PublishBuilder { client, it } = self;
        PublishBuilder {
            client,
            it: Publish { payload, ..it },
        }
    }
    pub fn qos(self, qos: QoS) -> PublishBuilder<'a> {
        let PublishBuilder { client, it } = self;
        PublishBuilder {
            client,
            it: Publish { qos, ..it },
        }
    }
    pub fn retain(self, retain: bool) -> PublishBuilder<'a> {
        let PublishBuilder { client, it } = self;
        PublishBuilder {
            client,
            it: Publish { retain, ..it },
        }
    }
    pub fn send(self) -> Result<()> {
        self.client.send_command(Command::Publish(self.it))
    }
}
