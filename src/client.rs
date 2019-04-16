use error::*;

use mqtt3::{QoS, ToTopicPath, TopicPath};

use mio_more::channel::*;

use std::ops::DerefMut;

use MqttOptions;

#[allow(unused)]
#[derive(DebugStub)]
pub enum Command {
    Status(#[debug_stub = ""] ::std::sync::mpsc::Sender<::state::MqttConnectionStatus>),
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
        let id = opts.client_id.clone();
        debug!("{}: Client start", id);
        let (commands_tx, commands_rx) = sync_channel(10);
        let callback = opts.get_disconnected_callback();
        let mut connection = ::connection::start(opts, commands_rx)?;
        debug!("{}: Spawning client thread", id);
        ::std::thread::spawn(move || {
            'outer: loop {
                debug!("{}: Entering normal operation loop", id);
                loop {
                    match connection.turn(None) {
                        Ok(_) => {}
                        Err(e) => {
                            error!("{} Disconnected: ({:?})", id, e);
                            break;
                        }
                    }
                }
                debug!("{}: Entering reconnecting loop", id);
                loop {
                    if let ::state::MqttConnectionStatus::WantConnect { when } =
                        connection.state().status()
                    {
                        let now = ::std::time::Instant::now();
                        if now < when {
                            info!("Will try to reconnecct in {} secs.", (when-now).as_secs());
                            ::std::thread::sleep(when-now);
                        }
                    } else {
                        info!("not seeking reconnection");
                        break 'outer;
                    }
                    info!("Try to reconnect");
                    match connection.reconnect() {
                        Ok(_) => {
                            info!("Reconnected");
                            break;
                        }
                        Err(e) => {
                            error!("({:?})", e);
                        }
                    }
                }
            }
            info!("client thread done");
            if let Some(callback) = callback {
                let mut callback = callback.lock().unwrap();
                callback.deref_mut()();
            }
        });

        Ok(MqttClient {
            nw_request_tx: commands_tx,
        })
    }

    pub fn subscribe<T: ToTopicPath>(
        &self,
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

    pub fn publish<T: ToTopicPath>(&self, topic_path: T) -> Result<PublishBuilder> {
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

    pub fn connected(&self) -> bool {
        self.status().map(|s| s == ::state::MqttConnectionStatus::Connected).unwrap_or(false)
    }

    pub fn status(&self) -> Result<::state::MqttConnectionStatus> {
        let (tx, rx) = ::std::sync::mpsc::channel();
        self.send_command(Command::Status(tx))?;
        Ok(rx.recv().map_err(|_| "Client thread looks dead")?)
    }

    fn send_command(&self, command: Command) -> Result<()> {
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
    client: &'a MqttClient,
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
    client: &'a MqttClient,
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
