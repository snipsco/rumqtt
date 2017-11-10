use std::net::SocketAddr;
use std::thread;
use std::cell::RefCell;
use std::rc::Rc;
use std::io;
use std::time::Duration;

use mqtt3;
use mio;
use mio_more::channel::*;

use MqttOptions;
use client::state::MqttState;
use ReconnectOptions;
use error::*;
use client::Command;

use std::error::Error;

pub fn start(opts: MqttOptions, commands_rx: Receiver<Command>) -> Result<()> {
    let mut mqtt_state = MqttState::new(opts);
    let mut connection = ConnectionState::connect(mqtt_state, commands_rx)?;
    while connection.state().status() != ::client::state::MqttConnectionStatus::Connected {
        connection.turn()?
    }
    loop {
        connection.turn().unwrap();
    }

//    ::std::thread::spawn(move || loop { connection.turn().unwrap() } );

    Ok(())

    /*
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
    */
}

const SOCKET_TOKEN: mio::Token = mio::Token(0);
const COMMANDS_TOKEN: mio::Token = mio::Token(1);

struct ConnectionState {
    mqtt_state: MqttState,
    commands_rx: Receiver<Command>,
    outgoing_packets_tx: Sender<mqtt3::Packet>,
    outgoing_packets_rx: Receiver<mqtt3::Packet>,
    outgoing_bytes: ::std::io::Cursor<Vec<u8>>,
    incoming_bytes: ::std::io::Cursor<Vec<u8>>,
    connection: mio::net::TcpStream,
    poll: mio::Poll,
}

impl ConnectionState {
    fn connect( mut mqtt_state: MqttState, commands_rx: Receiver<Command>) -> Result<ConnectionState> {
        // NOTE: make sure that dns resolution happens during reconnection to handle changes in server ip
        let addr: SocketAddr = mqtt_state.opts().broker_addr.as_str().parse().unwrap();

        // TODO: Add TLS support with client authentication (ca = roots.pem for iotcore)

        let connection = mio::net::TcpStream::connect(&addr)?;
        let (outgoing_packets_tx, outgoing_packets_rx) = channel();
        outgoing_packets_tx.send(mqtt3::Packet::Connect(mqtt_state.handle_outgoing_connect()));
        let state = ConnectionState {
            mqtt_state,
            commands_rx,
            connection,
            outgoing_packets_tx,
            outgoing_packets_rx,
            outgoing_bytes: ::std::io::Cursor::new(vec![]),
            incoming_bytes: ::std::io::Cursor::new(vec![]),
            poll: mio::Poll::new()?,
        };
        state.poll.register(&state.connection, SOCKET_TOKEN, ::mio::Ready::all(), ::mio::PollOpt::edge())?;
        state.poll.register(&state.commands_rx, COMMANDS_TOKEN, ::mio::Ready::all(), ::mio::PollOpt::edge())?;

        Ok(state)
    }

    fn state(&self) -> &MqttState {
        &self.mqtt_state
    }

    fn turn(&mut self) -> Result<()> {
        let mut events = mio::Events::with_capacity(1024);
        self.poll.poll(&mut events, None)?;
        for event in events.iter() {
            debug!("event: {:?}", event);
            if event.token() == SOCKET_TOKEN && event.kind().is_readable() {
                self.turn_incoming()?;
            }
            if self.state().status() == ::client::state::MqttConnectionStatus::Connected && event.token() == COMMANDS_TOKEN {
                self.turn_command()?;
            }
            if event.token() == SOCKET_TOKEN && event.kind().is_writable() {
                self.turn_outgoing()?;
            }
        }
        Ok(())
    }

    fn turn_command(&mut self) -> Result<()> {
        loop {
            match self.commands_rx.try_recv() {
                Ok(command) => self.handle_command(command)?,
                Err(::std::sync::mpsc::TryRecvError::Empty) => break,
                Err(e) => Err(e)?,
            }
        }
        Ok(())
    }

    fn turn_outgoing(&mut self) -> Result<()> {
        use mqtt3::MqttWrite;
        use std::io::{Read, Write};
        loop {
            if self.outgoing_bytes.position() == self.outgoing_bytes.get_ref().len() as u64 {
                match self.outgoing_packets_rx.try_recv() {
                    Ok(packet) => {
                        debug!("encoding packet {:?}", packet);
                        self.outgoing_bytes.set_position(0);
                        self.outgoing_bytes.get_mut().clear();
                        self.outgoing_bytes.write_packet(&packet)?;
                        self.outgoing_bytes.set_position(0);
                    }
                    Err(::std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(e) => Err(e)?,
                }
            }
            debug!("outgoing buffer is {}", self.outgoing_bytes.get_ref().len());
            let mut buf = [0; 128];
            let read = self.outgoing_bytes.read(&mut buf)?;
            if read > 0 {
                let pos = self.outgoing_bytes.position();
                match self.connection.write(&buf[0..read]) {
                    Ok(written) => {
                        debug!("wrote {:?}", written);
                        self.outgoing_bytes.set_position(
                            pos - read as u64 + written as u64,
                        )
                    }
                    Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => {
                        self.outgoing_bytes.set_position(pos - read as u64);
                        debug!("writing to socket would block");
                        break;
                    }
                    Err(e) => Err(e)?,
                }
            }
        }
        Ok(())
    }

    fn whole_packet(buf: &[u8]) -> Option<usize> {
        let mut maybe_length = 0usize;
        for i in 0..4 {
            if i+1 > buf.len() {
                return None
            }
            let byte = buf[i+1];
            maybe_length |= (byte as usize & 0x7F) << (7* i);
            if byte & 0x80 == 0 {
                maybe_length = 2 + i + maybe_length;
                break;
            }
        }
        debug!("wanted: {}, have: {}", maybe_length, buf.len());
        if maybe_length <= buf.len() {
            Some(maybe_length)
        } else {
            None
        }
    }

    fn turn_incoming(&mut self) -> Result<()> {
        use std::io::{ Read, Write };
        use mqtt3::MqttRead;
        let mut buf = [0; 128];
        loop {
            match self.connection.read(&mut buf) {
                Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => {
                    debug!("reading from socket would block");
                    break;
                }
                Ok(read) => {
                    debug!("read {} from socket", read);
                    self.incoming_bytes.write(&buf[0..read])?;
                    while let Some(packet_size) = Self::whole_packet(self.incoming_bytes.get_ref()) {
                        self.incoming_bytes.set_position(0);
                        let packet = self.incoming_bytes.read_packet()?;
                        let remaining = {
                            let mut vec = self.incoming_bytes.get_mut();
                            let remaining = vec.len() - packet_size;
                            for i in 0..remaining {
                                vec[i] = vec[i+packet_size]
                            }
                            vec.truncate(remaining);
                            remaining
                        };
                        self.incoming_bytes.set_position(remaining as u64);
                        debug!("received: {:?}", packet);
                        self.handle_incoming_packet(packet)?
                    }
                }
                Err(e) => Err(e)?
            }
        }
        Ok(())
    }

    fn handle_incoming_packet(&mut self, packet: ::mqtt3::Packet) -> Result<()> {
        match packet {
            mqtt3::Packet::Connack(connack) => {
                self.mqtt_state.handle_incoming_connack(connack)?;
                self.turn_command()?;
            }
            mqtt3::Packet::Suback(suback) => self.mqtt_state.handle_incoming_suback(suback)?,
            mqtt3::Packet::Publish(publish) => {
                let (_, server) = self.mqtt_state.handle_incoming_publish(publish)?;
                if let Some(server) = server {
                    self.send_packet(server)?
                }
            }
            mqtt3::Packet::Puback(ack) => {
                self.mqtt_state.handle_incoming_puback(ack)?;
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    fn handle_command(&mut self, command: Command) -> Result<()> {
        debug!("handle {:?}", command);
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
                let packet = self.mqtt_state.handle_outgoing_publish(publish)?;
                self.send_packet(mqtt3::Packet::Publish(packet))?
            },
            Command::Subscribe(sub) => {
                let packet = self.mqtt_state.handle_outgoing_subscribe(vec!(sub))?;
                self.send_packet(mqtt3::Packet::Subscribe(packet))?
            },
            _ => unimplemented!()
        };
        Ok(())
    }

    fn send_packet(&mut self, packet: ::mqtt3::Packet) -> Result<()> {
        self.outgoing_packets_tx.send(packet).map_err(|e| format!("mqtt3 internal send error: {:?}", e))?;
        self.turn_outgoing()
    }
}
        /*
            mqtt3::Packet::Puback(ack) => {
            if let Err(e) = notifier.try_send(Packet::Puback(ack)) {
                error!("Puback notification send failed. Error = {:?}", e);
            }
                // ignore unsolicited ack errors
                let _ = mqtt_state.borrow_mut().handle_incoming_puback(ack);
            }
            mqtt3::Packet::Pingresp => {
                mqtt_state.borrow_mut().handle_incoming_pingresp();
            }
            mqtt3::Packet::Publish(publish) => {
                let (publish, ack) = mqtt_state.borrow_mut().handle_incoming_publish(publish);
            if let Some(publish) = publish {
                if let Err(e) = notifier.try_send(Packet::Publish(publish)) {
                    error!("Publish notification send failed. Error = {:?}", e);
                }
            }
            if let Some(ack) = ack {
                match ack {
                    Packet::Puback(pkid) => {
                        commands_tx = await!(commands_tx.send(Command::Puback(pkid))).unwrap();
                    }
                    _ => unimplemented!()
                };
            }
            }
        Packet::Suback(suback) => {
            if let Err(e) = notifier.try_send(Packet::Suback(suback)) {
                error!("Suback notification send failed. Error = {:?}", e);
            }
        }
        };
        Ok(())
    })
    }
}

*/
/*

    fn run(&mut self, mut reactor: Core) {
        let commands_rx = Rc::clone(&self.commands_rx);
        let coms = commands_rx.borrow_mut();
        let commands = coms.for_each(|c| self.handle_command(c).map_err(|_| ()));
        reactor.run(commands).into_future();
    }

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
    opts: &MqttOptions
) -> Result<TcpStream> {
    // NOTE: make sure that dns resolution happens during reconnection to handle changes in server ip
    println!("opts: {:?}", opts.broker_addr);
    let addr: SocketAddr = opts.broker_addr.as_str().parse().unwrap();
    println!("addr: {:?}", addr);

    // TODO: Add TLS support with client authentication (ca = roots.pem for iotcore)

    let connection = mio::core::TcpStream::connect(&addr);
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
*/
