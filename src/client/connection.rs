use std::net::SocketAddr;
use std::thread;
use std::cell::RefCell;
use std::rc::Rc;
use std::io;
use std::time::Duration;

use mqtt3;
use codec::MqttCodec;
use MqttOptions;
use client::state::MqttState;
use ReconnectOptions;
use error::*;
use client::Command;

use std::error::Error;

use futures::stream::{Stream, SplitStream};
use futures::sync::mpsc::{Sender, Receiver};
use tokio_core::reactor::Core;
use futures::prelude::*;

use tokio_core::net::TcpStream;
//use tokio_timer::Timer;
use tokio_io::AsyncRead;
use tokio_io::codec::Framed;

pub fn start(opts: MqttOptions, commands_tx: Sender<Command>, commands_rx: Receiver<Command>) {
    let mut mqtt_state = MqttState::new(opts.clone());

    outgoing = 

    'reconnect: loop {
        let mut framed = match mqtt_connect(&mut mqtt_state, &opts, &mut reactor) {
            Ok(framed) => framed,
            Err(e) => {
                error!("Connection error = {:?}", e);
                match opts.reconnect {
                    ReconnectOptions::Never => break 'reconnect,
                    ReconnectOptions::AfterFirstSuccess(d) if !mqtt_state.initial_connect() => {
                        info!("Will retry connecting again in {} seconds", d);
                        thread::sleep(Duration::new(u64::from(d), 0));
                        continue 'reconnect;
                    }
                    ReconnectOptions::AfterFirstSuccess(_) => break 'reconnect,
                    ReconnectOptions::Always(d) => {
                        info!("Will retry connecting again in {} seconds", d);
                        thread::sleep(Duration::new(u64::from(d), 0));
                        continue 'reconnect;
                    }
                }
            }
        };
        let (sndr, rcvr) = framed.split();
        reactor.run(framed)
    }
}

type Fut<T> = Future<Item = T, Error = ::error::Error>;
/*
struct ConnectionState<'a> {
    mqtt_state: &'a mut MqttState,
    commands_rx: Rc<RefCell<Receiver<Command>>>,
    net_outgoing: Rc<RefCell<::futures::stream::SplitSink<Framed<TcpStream, MqttCodec>>>>,
}

impl<'a> ConnectionState<'a> {
    fn run(&mut self, mut reactor: Core) {
        let commands_rx = Rc::clone(&self.commands_rx);
        let coms = commands_rx.borrow_mut();
        let commands = coms.for_each(|c| self.handle_command(c).map_err(|_| ()));
        reactor.run(commands).into_future();
    }
    */

    fn handle_command(outgoing: command: Command) -> Box<Fut<()>> {
        use futures::future::result;
        use error::Error;
        debug!("deal with command: {:?}", command);
        match command {
            Command::Publish(publish) => {
                let publish = mqtt3::Publish {
                    topic_name: publish.topic,
                    dup: false,
                    pid: None,
                    qos: publish.qos,
                    retain: false,
                    payload: ::std::sync::Arc::new(publish.payload),
                };
                let mut outgoing = Rc::clone(&self.net_outgoing);
                let fut = result(self.mqtt_state.handle_outgoing_publish(publish))
                    .from_err::<Error>()
                    .and_then(move |packet| {
                        let mut out = outgoing.borrow_mut();
                        result(out.start_send(mqtt3::Packet::Publish(packet))).from_err()
                    })
                    .map(|_| ());
                Box::new(fut)
            }
            /*
            Command::Ping => {
                let _ping = mqtt_state.borrow_mut().handle_outgoing_ping()?;
                Ok(mqtt3::Packet::Pingreq)
            }
            */
            /*
            Command::Subscribe(subs) => {
                Box::new(result(self.mqtt_state.handle_outgoing_subscribe(vec!(subs.topic))).and_then(|packet|
                    self.net_outgoing.start_send(::mqtt3::Packet::Subscribe(packet)).into_future().from_err().map(|_| ())
                ))
            },
            */
            /*
            Command::Disconnect => {
                mqtt_state.borrow_mut().handle_disconnect();
                Ok(mqtt3::Packet::Disconnect)
            },
            */
            /*
            Command::Puback(pkid) => Ok(mqtt3::Packet::Puback(pkid)),
            */
            _ => unimplemented!(),
        }
    }
//}


fn mqtt_connect(
    mqtt_state: &mut MqttState,
    opts: &MqttOptions,
    reactor: &mut Core,
) -> Result<Framed<TcpStream, MqttCodec>> {
    // NOTE: make sure that dns resolution happens during reconnection to handle changes in server ip
    println!("opts: {:?}", opts.broker_addr);
    let addr: SocketAddr = opts.broker_addr.as_str().parse().unwrap();
    println!("addr: {:?}", addr);

    // TODO: Add TLS support with client authentication (ca = roots.pem for iotcore)

    let connection = TcpStream::connect(&addr, &reactor.handle());
    let framed = reactor.run(connection)?.framed(MqttCodec);
    let connect = mqtt_state.handle_outgoing_connect();
    let framed = reactor.run(framed.send(mqtt3::Packet::Connect(connect)))?;
    Ok(framed)
    /*
    match packet.unwrap() {
        mqtt3::Packet::Connack(connack) => {
            mqtt_state.borrow_mut().handle_incoming_connack(connack)?;
            Ok(frame)
        }
        _ => unimplemented!(),
    }
    */
}

/*
#[async]
fn ping_timer(mqtt_state: Rc<RefCell<MqttState>>, mut commands_tx: Sender<Command>, keep_alive: u16) -> io::Result<()> {
    let timer = Timer::default();
    let interval = timer.interval(Duration::new(u64::from(keep_alive), 0));

    #[async]
    for _t in interval {
        if mqtt_state.borrow().is_ping_required() {
            debug!("Ping timer fire");
            commands_tx = await!(
                commands_tx.send(Command::Ping).or_else(|e| {
                    Err(io::Error::new(ErrorKind::Other, e.description()))
                })
            )?;
        }
    }

    Ok(())
}
*/

fn incoming_network_packet_handler(
    mqtt_state: Rc<RefCell<MqttState>>,
    receiver: SplitStream<Framed<TcpStream, MqttCodec>>,
    mut commands_tx: Sender<Command>,
) -> Box<Future<Item = (), Error = ::error::Error>> {
    let fut = receiver
        .for_each(move |message| {
            info!("incoming n/w message = {:?}", message);
            match message {
                mqtt3::Packet::Connack(connack) => {
                    mqtt_state.borrow_mut().handle_incoming_connack(connack);
                }
                mqtt3::Packet::Puback(ack) => {
                    /*
                if let Err(e) = notifier.try_send(Packet::Puback(ack)) {
                    error!("Puback notification send failed. Error = {:?}", e);
                }
                */
                    // ignore unsolicited ack errors
                    let _ = mqtt_state.borrow_mut().handle_incoming_puback(ack);
                }
                mqtt3::Packet::Pingresp => {
                    mqtt_state.borrow_mut().handle_incoming_pingresp();
                }
                mqtt3::Packet::Publish(publish) => {
                    let (publish, ack) = mqtt_state.borrow_mut().handle_incoming_publish(publish);
                    /*
                if let Some(publish) = publish {
                    if let Err(e) = notifier.try_send(Packet::Publish(publish)) {
                        error!("Publish notification send failed. Error = {:?}", e);
                    }
                }
                */
                    /*
                if let Some(ack) = ack {
                    match ack {
                        Packet::Puback(pkid) => {
                            commands_tx = await!(commands_tx.send(Command::Puback(pkid))).unwrap();
                        }
                        _ => unimplemented!()
                    };
                }
                */
                }
                /*
            Packet::Suback(suback) => {
                if let Err(e) = notifier.try_send(Packet::Suback(suback)) {
                    error!("Suback notification send failed. Error = {:?}", e);
                }
            }
            */
                _ => unimplemented!(),
            };
            Ok(())
        })
        .into_future()
        .from_err::<::error::Error>()
        .and_then(|_| {
            error!("Network reciever stopped. Sending disconnect request");
            commands_tx.send(Command::Disconnect).map_err(|e| {
                e.description().into()
            })
        })
        .map(|_| ());
    Box::new(fut)
}
