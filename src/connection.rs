use std::net::SocketAddr;
use std::time::{Duration, Instant};

use mqtt3;
use mio;
use mio_more::channel::*;

use MqttOptions;
use state::MqttState;
use error::*;
use client::Command;

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

pub fn start(opts: MqttOptions, commands_rx: Receiver<Command>) -> Result<ConnectionState> {
    let mqtt_state = MqttState::new(opts);
    let mut connection = ConnectionState::new(mqtt_state, commands_rx)?;
    connection.wait_for_connack()?;
    debug!("Conenction established");
    Ok(connection)
}


impl ConnectionState {
    fn tcp_connect(broker: &str) -> Result<::mio::tcp::TcpStream> {
        let (host, port) = if broker.contains(":") {
            let mut tokens = broker.split(":");
            let host = tokens.next().unwrap();
            let port = tokens
                .next()
                .unwrap()
                .parse()
                .map_err(|_| "Failed to parse port number")?;
            (host, Some(port))
        } else {
            (broker, None)
        };
        debug!("Connecting to broker:{} -> {}:{:?}", broker, host, port);
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
            .ok_or("Failed to connect to broker".into())
    }

    // TODO: Add TLS support with client authentication (ca = roots.pem for iotcore)
    fn new(mut mqtt_state: MqttState, commands_rx: Receiver<Command>) -> Result<ConnectionState> {
        let connection = Self::tcp_connect(&mqtt_state.opts().broker_addr)?;
        let (out_packets_tx, out_packets_rx) = channel();
        out_packets_tx
            .send(mqtt3::Packet::Connect(
                mqtt_state.handle_outgoing_connect(true),
            ))
            .unwrap(); // checked: brand new channel
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
            &state.commands_rx,
            COMMANDS_TOKEN,
            mio::Ready::readable(),
            mio::PollOpt::edge(),
        )?;
        state.poll.register(
            &state.connection,
            SOCKET_TOKEN,
            mio::Ready::readable() | mio::Ready::writable(),
            mio::PollOpt::edge(),
        )?;
        Ok(state)
    }

    pub fn reconnect(&mut self) -> Result<()> {
        let connection = Self::tcp_connect(&self.mqtt_state.opts().broker_addr)?;
        let (out_packets_tx, out_packets_rx) = channel();
        debug!("deregistering");
        self.poll.deregister(&self.connection)?;
        self.poll.register(
            &connection,
            SOCKET_TOKEN,
            mio::Ready::readable() | mio::Ready::writable(),
            mio::PollOpt::edge(),
        )?;
        self.connection = connection;
        self.out_packets_tx = out_packets_tx;
        self.out_packets_rx = out_packets_rx;
        self.out_buffer = ::std::io::Cursor::new(vec![]);
        self.in_buffer = vec![0; 256];
        self.in_read = 0;
        self.out_packets_tx
            .send(mqtt3::Packet::Connect(
                self.mqtt_state.handle_outgoing_connect(false),
            ))
            .unwrap(); // checked: brand new channel.
        Ok(())
    }

    pub fn turn(&mut self, timeout: Option<Duration>) -> Result<()> {
        let mut events = mio::Events::with_capacity(1024);
        self.poll
            .poll(&mut events, timeout.or(Some(Duration::from_secs(1))))?;
        debug!("exit poll: {:?} events", events.len());
        for event in events.iter() {
            debug!("event: {:?}", event);
            if event.token() == SOCKET_TOKEN && event.readiness().is_readable() {
                self.turn_incoming()?;
            }
            if event.token() == COMMANDS_TOKEN {
                self.turn_command()?;
            }
            if event.token() == SOCKET_TOKEN && event.readiness().is_writable() {
                self.turn_outgoing()?;
            }
        }
        self.consider_ping()?;
        Ok(())
    }

    pub fn state(&self) -> &MqttState {
        &self.mqtt_state
    }

    pub fn wait_for_connack(&mut self) -> Result<()> {
        let time_limit = Instant::now() + self.state().opts().mqtt_connection_timeout;
        while self.state().status() != ::state::MqttConnectionStatus::Connected {
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
                Ok(0) => {
                    self.mqtt_state.handle_socket_disconnect();
                    Err("socket closed")?
                }
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
                            for i in 0..(self.in_read - used) {
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
                if let Some(msgs) = self.mqtt_state.handle_reconnection() {
                    for msg in msgs {
                        self.send_packet(mqtt3::Packet::Publish(msg))?;
                    }
                }
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
        debug!("Enter consider ping");
        if self.state().is_ping_required() {
            debug!("Handling ping");
            self.mqtt_state.handle_outgoing_ping()?;
            debug!("Sending ping");
            self.send_packet(mqtt3::Packet::Pingreq)?;
        }
        debug!("Done consider ping");
        Ok(())
    }

    fn handle_command(&mut self, command: Command) -> Result<()> {
        debug!("handle command : {:?}", command);
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
            Command::Status(tx) => {
                let _ = tx.send(self.state().status());
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
