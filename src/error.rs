use std::result;
use std::io;
use std::sync::mpsc::{TryRecvError, TrySendError, SendError};
use std::net::TcpStream;

use mqtt3::{self, ConnectReturnCode};
use persist;

use openssl;
use connection::NetworkRequest;

pub type SslError = openssl::error::ErrorStack;
pub type HandShakeError = openssl::ssl::HandshakeError<TcpStream>;
pub type Result<T> = result::Result<T, Error>;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Io(err: io::Error) {
            from()
            description("io error")
            display("I/O error: {}", err)
            cause(err)
        }
        TrySend(err: TrySendError<NetworkRequest>) {
            from()
        }
        NoConnectionThread
        TryRecv(err: TryRecvError) {
            from()
        }
        Send(err: SendError<NetworkRequest>) {
            from()
        }
        Mqtt3(err: mqtt3::Error) {
            from()
        }
        Persist(err: persist::Error) {
            from()
        }
        ConnectionAbort
        HandshakeFailed
        InvalidState
        InvalidPacket
        InvalidTopic(topic: String)
        MqttPacket
        PingTimeout
        AwaitPingResp
        Reconnect
        Ssl(e: SslError) {
            from()
        }
        Handshake(e: HandShakeError) {
            from()
        }
        MqttConnectionRefused(e: ConnectReturnCode) {
            from()
        }
    }
}
