use std::net::SocketAddr;
use std::time::{Duration, Instant};

use mqtt3;
use mio;
use mio_more::channel::*;

use MqttOptions;
use client::state::MqttState;
use ReconnectOptions;
use error::*;
use client::Command;

pub fn start(opts: MqttOptions, commands_rx: Receiver<Command>) -> Result<ConnectionState> {
    let mqtt_state = MqttState::new(opts);
    let mut connection = ConnectionState::connect(mqtt_state, commands_rx)?;
    connection.wait_for_connack()?;
    Ok(connection)

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

pub struct ConnectionState {
    mqtt_state: MqttState,
    commands_rx: Receiver<Command>,
    out_packets_tx: Sender<mqtt3::Packet>,
    out_packets_rx: Receiver<mqtt3::Packet>,
    out_buffer: ::std::io::Cursor<Vec<u8>>,
    in_buffer: Vec<u8>,
    in_read: usize,
    connection: mio::net::TcpStream,
    poll: mio::Poll,
}

impl ConnectionState {
    fn connect(
        mut mqtt_state: MqttState,
        commands_rx: Receiver<Command>,
    ) -> Result<ConnectionState> {
        let connection = {
            let broker = &mqtt_state.opts().broker_addr;
            let (host, port) = if broker.contains(":") {
                let mut tokens = mqtt_state.opts().broker_addr.split(":");
                let host = tokens.next().unwrap();
                let port = tokens
                    .next()
                    .unwrap()
                    .parse()
                    .map_err(|_| "Failed to parse port number")?;
                (host, Some(port))
            } else {
                (&**broker, None)
            };
            let ips = ::dns_lookup::lookup_host(host)?;
            ips.into_iter()
                .filter_map(|ip| {
                    let addr: SocketAddr = SocketAddr::new(ip, port.unwrap_or(1883));
                    match mio::net::TcpStream::connect(&addr) {
                        Ok(ok) => Some(ok),
                        Err(e) => {
                            error!("Failed to connect to {:?} ({:?})", ip, e);
                            None
                        }
                    }
                })
                .next()
                .ok_or("Failed to connect to broker")?
        };

        // TODO: Add TLS support with client authentication (ca = roots.pem for iotcore)

        let (out_packets_tx, out_packets_rx) = channel();
        out_packets_tx
            .send(mqtt3::Packet::Connect(mqtt_state.handle_outgoing_connect()))
            .map_err(|_| "failed to send mqtt command to client thread")?;
        let state = ConnectionState {
            mqtt_state,
            commands_rx,
            connection,
            out_packets_tx,
            out_packets_rx,
            out_buffer: ::std::io::Cursor::new(vec![]),
            in_buffer: vec![0; 256],
            in_read: 0,
            poll: mio::Poll::new()?,
        };
        state.poll.register(
            &state.connection,
            SOCKET_TOKEN,
            mio::Ready::readable() | mio::Ready::writable(),
            mio::PollOpt::edge(),
        )?;
        state.poll.register(
            &state.commands_rx,
            COMMANDS_TOKEN,
            mio::Ready::readable(),
            mio::PollOpt::edge(),
        )?;
        Ok(state)
    }

    pub fn wait_for_connack(&mut self) -> Result<()> {
        let time_limit = Instant::now() + self.state().opts().mqtt_connection_timeout;
        while self.state().status() != ::client::state::MqttConnectionStatus::Connected {
            let now = Instant::now();
            if now > time_limit {
                Err(format!(
                    "Timeout: no connack after {:?}",
                    self.state().opts().mqtt_connection_timeout
                ))?
            }
            self.turn(Some(time_limit - now))?
        }
        Ok(())
    }

    fn state(&self) -> &MqttState {
        &self.mqtt_state
    }

    pub fn turn(&mut self, timeout: Option<Duration>) -> Result<()> {
        let mut events = mio::Events::with_capacity(1024);
        self.poll
            .poll(&mut events, timeout.or(Some(Duration::from_secs(1))))?;
        for event in events.iter() {
            debug!("event: {:?}", event);
            if event.token() == SOCKET_TOKEN && event.readiness().is_readable() {
                self.turn_incoming()?;
            }
            if self.state().status() == ::client::state::MqttConnectionStatus::Connected
                && event.token() == COMMANDS_TOKEN
            {
                self.turn_command()?;
            }
            if event.token() == SOCKET_TOKEN && event.readiness().is_writable() {
                self.turn_outgoing()?;
            }
        }
        self.consider_ping()?;
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
        use std::io::Write;
        loop {
            if self.out_buffer.position() == self.out_buffer.get_ref().len() as u64 {
                match self.out_packets_rx.try_recv() {
                    Ok(packet) => {
                        debug!("encoding packet {:?}", packet);
                        self.out_buffer.set_position(0);
                        self.out_buffer.get_mut().clear();
                        self.out_buffer.write_packet(&packet)?;
                        self.out_buffer.set_position(0);
                    }
                    Err(::std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(e) => Err(e)?,
                }
            }
            debug!("outgoing buffer is {}", self.out_buffer.get_ref().len());
            if self.out_buffer.get_ref().len() as u64 > self.out_buffer.position() {
                match self.connection.write(&self.out_buffer.get_ref()) {
                    Ok(written) => {
                        debug!("wrote {:?}", written);
                        let pos = self.out_buffer.position();
                        self.out_buffer.set_position(pos + written as u64)
                    }
                    Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => {
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
            if i + 1 > buf.len() {
                return None;
            }
            let byte = buf[i + 1];
            maybe_length |= (byte as usize & 0x7F) << (7 * i);
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
        use std::io::Read;
        use mqtt3::MqttRead;
        loop {
            if self.in_read == self.in_buffer.len() {
                self.in_buffer.resize(self.in_read + 128, 0);
            }
            let read = self.connection.read(&mut self.in_buffer[self.in_read..]);
            debug!("read: {:?}", read);
            match read {
                Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => {
                    debug!("reading from socket would block");
                    break;
                }
                Ok(0) => Err("disconnected")?,
                Ok(read) => {
                    self.in_read += read;
                    let mut used = 0;
                    while let Some(packet_size) =
                        Self::whole_packet(&self.in_buffer[used..self.in_read])
                    {
                        let packet = (&self.in_buffer[used..]).read_packet()?;
                        used += packet_size;
                        debug!("received: {:?}", packet);
                        self.handle_incoming_packet(packet)?
                    }
                    if used > 0 {
                        if used < self.in_read {
                            for i in 0..(used - self.in_read) {
                                self.in_buffer[i] = self.in_buffer[i + used]
                            }
                        }
                        self.in_read -= used;
                    }
                }
                Err(e) => Err(e)?,
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
            mqtt3::Packet::Pingresp => {
                self.mqtt_state.handle_incoming_pingresp();
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    fn consider_ping(&mut self) -> Result<()> {
        if self.state().is_ping_required() {
            self.mqtt_state.handle_outgoing_ping()?;
            self.send_packet(mqtt3::Packet::Pingreq)?;
        }
        Ok(())
    }

    fn handle_command(&mut self, command: Command) -> Result<()> {
        match command {
            Command::Publish(publish) => {
                let publish = mqtt3::Publish {
                    topic_name: publish.topic.path(),
                    dup: false,
                    pid: None,
                    qos: publish.qos,
                    retain: publish.retain,
                    payload: ::std::sync::Arc::new(publish.payload),
                };
                let packet = self.mqtt_state.handle_outgoing_publish(publish)?;
                self.send_packet(mqtt3::Packet::Publish(packet))?
            }
            Command::Subscribe(sub) => {
                let packet = self.mqtt_state.handle_outgoing_subscribe(vec![sub])?;
                self.send_packet(mqtt3::Packet::Subscribe(packet))?
            }
            Command::Alive(tx) => {
                let _ = tx.send(());
            }
            Command::Connect => unimplemented!(),
            Command::Disconnect => unimplemented!(),
        };
        Ok(())
    }

    fn send_packet(&mut self, packet: ::mqtt3::Packet) -> Result<()> {
        self.out_packets_tx
            .send(packet)
            .map_err(|e| format!("mqtt3 internal send error: {:?}", e))?;
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
