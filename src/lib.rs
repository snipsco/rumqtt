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

#[allow(unused_doc_comment)]
mod error {
    error_chain! {
        foreign_links {
            Io(::std::io::Error);
            Mqtt3(::mqtt3::Error);
            SyncMpsc(::std::sync::mpsc::TryRecvError);
        }
        errors {
            InvalidState {
                description("invalid state")
                display("invalid state")}
            }
    }
}

mod client;
mod connection;
mod options;
mod state;

pub use options::{MqttOptions, ReconnectOptions, SecurityOptions};
pub use client::MqttClient;
pub use mqtt3::{Message, Publish, QoS, ToTopicPath, TopicPath};
