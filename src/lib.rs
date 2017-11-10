
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_timer;
extern crate mqtt3;
extern crate bytes;
extern crate chrono;
extern crate jsonwebtoken as jwt;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;

mod codec;
mod packet;
mod mqttopts;
mod client;
mod error;

pub type SubscriptionCallback = Box<Fn(::mqtt3::Publish) + Send>;

pub struct Subscription {
    pub id: Option<String>,
    pub topic: mqtt3::SubscribeTopic,
    pub callback: SubscriptionCallback,
}

#[derive(Debug)]
pub struct Publish {
    pub topic: String,
    pub qos: ::mqtt3::QoS,
    pub payload: Vec<u8>,
}

impl ::std::fmt::Debug for Subscription {
    fn fmt(&self, fmt: &mut ::std::fmt::Formatter) -> Result<(), ::std::fmt::Error> {
        let topic = format!("{:?}", self.topic);
        write!(fmt, "Subscription({:?}, {:?})", self.id.as_ref().unwrap_or(&topic), &topic)
    }
}

// expose to other crates
pub use mqttopts::{MqttOptions, ReconnectOptions, SecurityOptions};
pub use client::MqttClient;
pub use mqtt3::QoS;
