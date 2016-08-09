use std::net::TcpStream;
use std::io::{self, Read, Write, BufReader};
use std::fs::File;
use std::net::Shutdown;
use std::path::Path;
use std::sync::{Arc, Mutex};

use rustls::{self, Session};
use error::{Error, Result};

pub struct TlsStream {
    stream: TcpStream,
    pub tls_session: Arc<Mutex<rustls::ClientSession>>,
}

impl TlsStream {
    pub fn new_session<P>(stream: TcpStream, hostname: &str, cafile: P) -> Result<Self>
        where P: AsRef<Path>
    {
        // Create new tls config and add ca
        let mut config = rustls::ClientConfig::new();
        let certfile = try!(File::open(cafile));
        let mut reader = BufReader::new(certfile);
        config.root_store.add_pem_file(&mut reader).unwrap();

        let config = Arc::new(config);
        // TlsStream with tcp stream and tls session
        let tls_stream = TlsStream {
            stream: stream,
            // NOTE: Hostname should match to server address or else --> Decode Error
            tls_session: Arc::new(Mutex::new(rustls::ClientSession::new(&config, hostname))),
        };
        Ok(tls_stream)
    }
}

pub enum NetworkStream {
    Tcp(TcpStream),
    Tls(TlsStream),
    None,
}

impl NetworkStream {
    pub fn get_ref(&self) -> io::Result<&TcpStream> {
        match *self {
            NetworkStream::Tcp(ref s) => Ok(s),
            NetworkStream::Tls(ref s) => Ok(&s.stream),
            NetworkStream::None => Err(io::Error::new(io::ErrorKind::Other, "No stream!")),
        }
    }

    pub fn try_clone(&mut self) -> Result<Self> {
        match *self {
            NetworkStream::Tcp(ref s) => Ok(NetworkStream::Tcp(try!(s.try_clone()))),
            NetworkStream::Tls(ref s) => {
                let tls = s.tls_session.clone();
                let stream = try!(try!(self.get_ref()).try_clone());

                let tls_stream = TlsStream {
                    stream: stream,
                    // NOTE: Hostname should match to server address or else --> Decode Error
                    tls_session: tls,
                };
                Ok(NetworkStream::Tls(tls_stream))
            }
            NetworkStream::None => Err(Error::Io(io::Error::new(io::ErrorKind::Other, "No Tls stream!"))),
        }
    }

    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match *self {
            NetworkStream::Tcp(ref s) => s.shutdown(how),
            NetworkStream::Tls(ref s) => s.stream.shutdown(how),
            NetworkStream::None => Err(io::Error::new(io::ErrorKind::Other, "No stream!")),
        }
    }
}

impl Read for NetworkStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            NetworkStream::Tcp(ref mut s) => s.read(buf),
            NetworkStream::Tls(ref mut s) => {
                let mut tls_session = s.tls_session.lock().unwrap();
                while tls_session.wants_read() {
                    match tls_session.read_tls(&mut s.stream) {
                        Ok(_) => {
                            match tls_session.process_new_packets() {
                                Ok(_) => (),
                                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, format!("{:?}", e))),
                            }
                            while tls_session.wants_write() {
                                try!(tls_session.write_tls(&mut s.stream));
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
                tls_session.read(buf)
            }
            NetworkStream::None => Err(io::Error::new(io::ErrorKind::Other, "No stream!")),
        }
    }
}

impl Write for NetworkStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            NetworkStream::Tcp(ref mut s) => s.write(buf),
            NetworkStream::Tls(ref mut s) => {
                let mut tls_session = s.tls_session.lock().unwrap();
                let res = tls_session.write(buf);
                while tls_session.wants_write() {
                    try!(tls_session.write_tls(&mut s.stream));
                }
                res
            }
            NetworkStream::None => Err(io::Error::new(io::ErrorKind::Other, "No stream!")),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            NetworkStream::Tcp(ref mut s) => s.flush(),
            NetworkStream::Tls(ref mut s) => {
                let mut tls_session = s.tls_session.lock().unwrap();
                tls_session.flush()
            }
            NetworkStream::None => Err(io::Error::new(io::ErrorKind::Other, "No stream!")),
        }
    }
}
