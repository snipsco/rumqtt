use error::*;

use mqtt3::{QoS, ToTopicPath, TopicPath};

use mio_more::channel::*;

use std::sync::atomic::{AtomicUsize, Ordering};

use MqttOptions;

#[allow(unused)]
#[derive(DebugStub)]
pub enum Command {
    Status(#[debug_stub = ""] ::std::sync::mpsc::Sender<::state::MqttConnectionStatus>),
    Subscribe(Subscription),
    Unsubscribe(SubscriptionToken),
    Publish(Publish),
    Connect,
    Disconnect,
}

pub struct MqttClient {
    nw_request_tx: SyncSender<Command>,
    subscription_id_source: AtomicUsize
}

impl MqttClient {
    /// Connects to the broker and starts an event loop in a new thread.
    /// Returns 'Command' and handles reqests from it.
    /// Also handles network events, reconnections and retransmissions.
    pub fn start(opts: MqttOptions) -> Result<Self> {
        let id = opts.client_id.clone();
        debug!("{}: Client start", id);
        let (commands_tx, commands_rx) = sync_channel(10);
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
        });

        Ok(MqttClient {
            nw_request_tx: commands_tx,
            subscription_id_source: AtomicUsize::new(0),
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
                id: self.subscription_id_source.fetch_add(1, Ordering::Relaxed),
                topic_path: topic_path.to_topic_path()?,
                qos: ::mqtt3::QoS::AtMostOnce,
                callback,
            },
        })
    }

    pub fn unsubscribe(&self, token: SubscriptionToken) -> Result<()> {
        self.send_command(Command::Unsubscribe(token))
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
    pub id: usize,
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
    pub fn qos(self, qos: QoS) -> SubscriptionBuilder<'a> {
        let SubscriptionBuilder { client, it } = self;
        SubscriptionBuilder {
            client,
            it: Subscription { qos, ..it },
        }
    }
    pub fn send(self) -> Result<SubscriptionToken> {
        let token = SubscriptionToken { id: self.it.id};
        self.client.send_command(Command::Subscribe(self.it))?;
        Ok(token)
    }
}

#[derive(Debug)]
pub struct SubscriptionToken {
    pub id: usize
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
