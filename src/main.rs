extern crate bytes;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate mqtt;
extern crate tokio_core;
extern crate tokio_io;

use std::io;

use bytes::BytesMut;
use futures::{Future, Stream};
use tokio_io::AsyncRead;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Core;

error_chain! {
    foreign_links {
        Io(io::Error);
    }
}

pub struct MqttCodec();

impl tokio_io::codec::Encoder for MqttCodec {
    type Item = mqtt::packet::VariablePacket;
    type Error = Error;
    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<()> {
        use mqtt::Encodable;
        use bytes::BufMut;
        item.encode(&mut dst.writer()).map_err(|e| {
            format!("can not encode packet: {:?}", e)
        })?;
        Ok(())
    }
}

impl tokio_io::codec::Decoder for MqttCodec {
    type Item = mqtt::packet::VariablePacket;
    type Error = Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        use mqtt::Decodable;
        let mut maybe_length = 0usize;
        for i in 0..4 {
            if i + 1 > src.len() {
                return Ok(None);
            }
            let byte = src[i + 1];
            maybe_length |= (byte as usize & 0x7F) << (7 * i);
            if byte & 0x80 == 0 {
                maybe_length = 2 + i + maybe_length;
                break;
            }
        }
        if maybe_length > src.len() {
            return Ok(None);
        }
        let packet = src.split_to(maybe_length);
        let decoded = mqtt::packet::VariablePacket::decode(&mut packet.as_ref())
            .map_err(|e| format!("can not decode packet: {:?}", e))?;
        Ok(Some(decoded))
    }
}


fn main() {
    use futures::Sink;
    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let addr = "127.0.0.1:1883".parse().unwrap();
    let done = TcpStream::connect(&addr, &handle)
        .from_err::<Error>()
        .and_then(|s: TcpStream| {
            let (sink, stream) = s.framed(MqttCodec()).split();
            let connect_packet =
                mqtt::packet::connect::ConnectPacket::new("MQTT".to_owned(), "foobar".to_owned());
            let sink = sink.send(mqtt::packet::VariablePacket::from(connect_packet))
                .map(|_| {
                    println!("write connection");
                });
            let stream = stream
                .for_each(|a| {
                    println!("read a message {:?}", a);
                    futures::future::ok(())
                })
                .from_err();
            Box::new(sink.join(stream))
        });

    println!("Starting loop");
    let _ = core.run(done).unwrap();
    println!("Done loop");
}
