mod state;
mod connection;

use MqttOptions;

use error::*;

use mio_more::channel::*;

#[derive(Debug)]
pub enum Command {
    Subscribe(::Subscription),
    Publish(::Publish),
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
    pub fn start(opts: MqttOptions) -> Self {
        let (commands_tx, commands_rx) = sync_channel(10);

        let nw_commands_tx = commands_tx.clone();
        // This thread handles network reads (coz they are blocking) and
        // and sends them to event loop thread to handle mqtt state.
        ::std::thread::spawn( move || {
                connection::start(opts, commands_rx);
                error!("Network Thread Stopped !!!!!!!!!");
            }
        );

        let client = MqttClient { nw_request_tx: commands_tx };
        client
    }

    pub fn publish(&mut self, topic: &str, qos: ::mqtt3::QoS, payload: Vec<u8>) -> Result<()> {
        self.publish_object(::Publish {
            topic: topic.into(), qos, payload
        })
    }

    pub fn publish_object(&mut self, publish: ::Publish) -> Result<()> {
        self.nw_request_tx.send(Command::Publish(publish));
        Ok(())
    }

    pub fn subscribe_object(&mut self, subscription: ::Subscription) -> Result<()> {
        self.nw_request_tx.send(Command::Subscribe(subscription));
        Ok(())
    }
}
