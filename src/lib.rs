extern crate chrono;
#[macro_use]
extern crate debug_stub_derive;
extern crate dns_lookup;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate mio;
extern crate mio_more;
extern crate mqtt3;
extern crate rustls;
#[macro_use]
extern crate serde_derive;
extern crate webpki;
extern crate webpki_roots;

#[allow(unused_doc_comment)]
pub mod error {
    error_chain! {
        foreign_links {
            Io(::std::io::Error);
            Mqtt3(::mqtt3::Error);
            SyncMpsc(::std::sync::mpsc::TryRecvError);
            Rustls(::rustls::TLSError);
        }
        errors {
            InvalidState
            PacketSizeLimitExceeded
            Connack(code: ::mqtt3::ConnectReturnCode) {
                description("mqtt negotiation failed")
                display("mqtt negogiation failed with return code: {:?}", code)
            }

        }
    }
}

mod client;
mod connection;
mod options;
mod state;

pub use rustls::ClientConfig as RustlsConfig;
pub use options::{MqttOptions, ReconnectOptions, TlsOptions};
pub use client::MqttClient;
pub use mqtt3::{Message, Publish, QoS, ToTopicPath, TopicPath};
