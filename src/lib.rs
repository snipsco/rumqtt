extern crate chrono;
#[macro_use]
extern crate debug_stub_derive;
extern crate dns_lookup;
#[macro_use]
extern crate error_chain;
extern crate jsonwebtoken as jwt;
#[macro_use]
extern crate log;
extern crate mio;
extern crate mio_more;
extern crate mqtt3;
#[macro_use]
extern crate serde_derive;

mod packet;
mod mqttopts;
mod client;
mod error;

// expose to other crates
pub use mqttopts::{MqttOptions, ReconnectOptions, SecurityOptions};
pub use client::MqttClient;
pub use mqtt3::{Message, Publish, QoS, ToTopicPath, TopicPath};
