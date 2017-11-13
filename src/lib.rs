extern crate dns_lookup;
extern crate mio;
extern crate mio_more;
extern crate mqtt3;
extern crate chrono;
extern crate jsonwebtoken as jwt;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;

mod packet;
mod mqttopts;
mod client;
mod error;

// expose to other crates
pub use mqttopts::{MqttOptions, ReconnectOptions, SecurityOptions};
pub use client::MqttClient;
pub use mqtt3::QoS;
