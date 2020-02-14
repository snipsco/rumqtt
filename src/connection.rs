use std::io::{ Read, Write};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use mqtt3;
use mio;
use mio_more::channel::*;

use rustls::{ ClientSession, Session };

use crate::MqttOptions;
use crate::state::MqttState;
use crate::error::*;
use crate::options;
use crate::client::Command;

pub enum Connection {
    Tcp {
        out_buffer: Vec<u8>,
        out_written: usize,
        connection: mio::net::TcpStream,
    },
    Tls {
        tls_session: ClientSession,
        connection: mio::net::TcpStream,
    }
}

impl Connection {
    fn wrap(connection: mio::net::TcpStream, tls:Option<&options::TlsOptions>) -> Result<Connection> {
        if let Some(ref tls) = tls.as_ref() {
            let hostname = webpki::DNSNameRef::try_from_ascii_str(&tls.hostname)
                .map_err(|_| ErrorKind::InvalidDnsName)?;
            let tls_session = ClientSession::new(&::std::sync::Arc::new(tls.to_rustls_config()?), hostname);
            Ok(Connection::Tls {
                connection,
                tls_session,
            })
        } else {
            Ok(Connection::Tcp {
                connection,
                out_buffer: Vec::with_capacity(128),
                out_written: 0,
            })
        }
    }
    fn enqueue_data(&mut self, buffer:&[u8]) -> Result<()> {
        match self {
            &mut Connection::Tcp {ref mut out_buffer, ..} => {
                out_buffer.extend(buffer);
            }
            &mut Connection::Tls {ref mut tls_session, ..} => {
                tls_session.write(buffer)?;
            }
        };
        trace!("Wants send ? {:?}", self.wants_send());
        Ok(())
    }
    fn manage_result(it: ::std::io::Result<usize>) -> Result<usize> {
        match it {
            Ok(0) => Err("socket closed")?,
            Ok(n) => Ok(n),
            Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => Err(e.into()),
        }
    }
    fn recv_data(&mut self, buffer:&mut [u8]) -> Result<usize> {
        match self {
            &mut Connection::Tcp {ref mut connection, ..} => Self::manage_result(connection.read(buffer)),
            &mut Connection::Tls {ref mut tls_session, ref mut connection} => {
                if tls_session.wants_read() {
                    if Self::manage_result(tls_session.read_tls(connection))? == 0 {
                        trace!("recv_data: read_tls returned 0");
                        return Ok(0)
                    }
                }
                tls_session.process_new_packets()?;
                if tls_session.wants_write() {
                    tls_session.write_tls(connection)?;
                }
                let read = tls_session.read(buffer)?;
                trace!("recv_data: tls_session.read returned {:?}", read);
                Ok(read)
            }
        }
    }
    fn send_data(&mut self) -> Result<usize> {
        trace!("Trying to send bytes");
        match self {
            &mut Connection::Tcp {ref mut out_buffer, ref mut connection, ref mut out_written} => {
                trace!("out_buffer: {}", out_buffer.len());
                trace!("out_written: {}", out_written);
                let written = Self::manage_result(connection.write(&out_buffer[*out_written..]))?;
                trace!("written more: {}", written);
                *out_written += written;
                if out_buffer.len() == *out_written {
                    out_buffer.clear();
                    *out_written = 0;
                }
                Ok(written)
            }
            &mut Connection::Tls {ref mut tls_session, ref mut connection} => 
                Self::manage_result(tls_session.write_tls(connection))
        }
    }
    fn wants_send(&self) -> bool {
        match self {
            &Connection::Tcp {ref out_buffer, ref out_written, ..} => out_buffer.len() > *out_written,
            &Connection::Tls {ref tls_session, ..} => tls_session.wants_write(),
        }
    }
    fn mio_connection(&mut self) -> &mut mio::net::TcpStream {
        match self {
            &mut Connection::Tcp {ref mut connection, ..} => connection,
            &mut Connection::Tls {ref mut connection, ..} => connection,
        }
    }
}

const SOCKET_TOKEN: mio::Token = mio::Token(0);
const COMMANDS_TOKEN: mio::Token = mio::Token(1);

pub struct ConnectionState {
    mqtt_state: MqttState,
    commands_rx: Receiver<Command>,
    out_packets_tx: Sender<mqtt3::Packet>,
    out_packets_rx: Receiver<mqtt3::Packet>,
    in_buffer: Vec<u8>,
    in_read: usize,
    poll: mio::Poll,
    connection: Connection,
}

pub fn start(opts: MqttOptions, commands_rx: Receiver<Command>) -> Result<ConnectionState> {
    info!("{}: Connection start", opts.client_id);
    let mqtt_state = MqttState::new(opts);
    let mut connection = ConnectionState::new(mqtt_state, commands_rx)?;
    connection.wait_for_connack()?;
    info!("{}: Connection established", connection.id());
    Ok(connection)
}

impl ConnectionState {
    fn id(&self) -> &str {
        &self.mqtt_state.opts().client_id
    }

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
        trace!("Connecting to broker:{} -> {}:{:?}", broker, host, port);
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

    fn new(mut mqtt_state: MqttState, commands_rx: Receiver<Command>) -> Result<ConnectionState> {
        debug!("{} new connection", mqtt_state.opts().client_id);
        let poll = mio::Poll::new()?;
        let connection = Self::tcp_connect(&mqtt_state.opts().broker_addr)?;
        let (out_packets_tx, out_packets_rx) = channel();
        out_packets_tx
            .send(mqtt3::Packet::Connect(
                mqtt_state.handle_outgoing_connect(true),
            ))
            .unwrap(); // checked: brand new channel
        poll.register(
            &commands_rx,
            COMMANDS_TOKEN,
            mio::Ready::readable(),
            mio::PollOpt::edge(),
        )?;
        poll.register(
            &connection,
            SOCKET_TOKEN,
            mio::Ready::readable() | mio::Ready::writable(),
            mio::PollOpt::edge(),
        )?;
        let state = ConnectionState {
            connection: Connection::wrap(connection, mqtt_state.opts().tls.as_ref())?,
            mqtt_state,
            commands_rx,
            out_packets_tx,
            out_packets_rx,
            in_buffer: vec![0; 256],
            in_read: 0,
            poll,
        };
        Ok(state)
    }

    pub fn reconnect(&mut self) -> Result<()> {
        let connection = Self::tcp_connect(&self.mqtt_state.opts().broker_addr)?;
        let (out_packets_tx, out_packets_rx) = channel();
        self.poll.deregister(self.connection.mio_connection())?;
        self.poll.register(
            &connection,
            SOCKET_TOKEN,
            mio::Ready::readable() | mio::Ready::writable(),
            mio::PollOpt::edge(),
        )?;
        self.connection = Connection::wrap(connection, self.mqtt_state.opts().tls.as_ref())?;
        self.out_packets_tx = out_packets_tx;
        self.out_packets_rx = out_packets_rx;
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
        for event in events.iter() {
            trace!("event: {:?}", event);
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
        while self.state().status() != crate::state::MqttConnectionStatus::Connected {
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
        loop {
            while self.connection.wants_send() {
                match self.connection.send_data() {
                    Ok(0) => return Ok(()),
                    Ok(_) => {/* try to send more */} ,
                    Err(e) => {
                        self.mqtt_state.handle_socket_disconnect();
                        return Err(e);
                    }
                }
            }
            match self.out_packets_rx.try_recv() {
                Ok(packet) => {
                    use mqtt3::MqttWrite;
                    debug!("Send: {:?}", packet);
                    let mut buf = ::std::io::Cursor::new(vec!());
                    buf.write_packet(&packet)?;
                    self.connection.enqueue_data(&buf.into_inner())?
                }
                Err(::std::sync::mpsc::TryRecvError::Empty) => {return Ok(())},
                Err(e) => Err(e)?,
            }
        }
    }

    fn whole_packet(buf: &[u8]) -> Option<usize> {
        let mut maybe_length = 0usize;
        for i in 0..4 {
            if i + 1 >= buf.len() {
                return None;
            }
            let byte = buf[i + 1];
            maybe_length |= (byte as usize & 0x7F) << (7 * i);
            if byte & 0x80 == 0 {
                maybe_length = 2 + i + maybe_length;
                break;
            }
        }
        trace!("wanted: {}, have: {}", maybe_length, buf.len());
        if maybe_length <= buf.len() {
            Some(maybe_length)
        } else {
            None
        }
    }

    fn turn_incoming(&mut self) -> Result<()> {
        use mqtt3::MqttRead;
        trace!("incoming");
        loop {
            trace!("incoming loop");
            if self.in_read == self.in_buffer.len() {
                trace!("resize {} - {}", self.in_buffer.len(), self.in_read * 2);
                self.in_buffer.resize(self.in_read * 2, 0);
            }
            let read = self.connection.recv_data(&mut self.in_buffer[self.in_read..]);
            trace!("read: {:?}", read);
            match read {
                Err(e) => {
                    self.mqtt_state.handle_socket_disconnect();
                    return Err(e)
                }
                Ok(0) => return Ok(()),
                Ok(read) => {
                    self.in_read += read;
                    let mut used = 0;
                    while let Some(packet_size) =
                        Self::whole_packet(&self.in_buffer[used..self.in_read])
                    {
                        let packet = (&self.in_buffer[used..]).read_packet()?;
                        used += packet_size;
                        match packet {
                            mqtt3::Packet::Publish(ref msg) => debug!("{} Received message on {}", self.id(), msg.topic_name),
                            ref msg => debug!("{} Received control message {:?}", self.id(), msg),
                        };
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
            }
        }
    }

    fn handle_incoming_packet(&mut self, packet: ::mqtt3::Packet) -> Result<()> {
        match packet {
            mqtt3::Packet::Connack(connack) => {
                trace!("Handle incoming connack");
                self.mqtt_state.handle_incoming_connack(connack)?;
                if let Some(state) = self.mqtt_state.handle_reconnection() {
                    trace!("State refresh after reco: {:?}", state);
                    for msg in state.0 {
                        self.send_packet(mqtt3::Packet::Subscribe(msg))?;
                    }
                    for msg in state.1 {
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
        trace!("Enter consider ping");
        if self.state().is_ping_required() {
            trace!("Handling ping");
            self.mqtt_state.handle_outgoing_ping()?;
            debug!("Sending ping");
            self.send_packet(mqtt3::Packet::Pingreq)?;
        }
        trace!("Done consider ping");
        Ok(())
    }

    fn handle_command(&mut self, command: Command) -> Result<()> {
        trace!("handle command : {:?}", command);
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
