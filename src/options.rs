use std::time::Duration;
use std::{ fs, io, path };
use std::sync::{Arc, Mutex};
use crate::error::Result;

use crate::RustlsConfig;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ReconnectOptions {
    Never,
    AfterFirstSuccess(Duration),
    Always(Duration),
}

impl Default for ReconnectOptions {
    fn default() -> ReconnectOptions {
        ReconnectOptions::AfterFirstSuccess(Duration::from_secs(10))
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct TlsOptions {
    pub hostname: String,
    pub disable_root_store: bool,
    pub cafile: Vec<path::PathBuf>,
    pub capath: Vec<path::PathBuf>,
    pub client_certs_key: Option<(path::PathBuf,path::PathBuf)>
}

impl TlsOptions {
    pub fn new(hostname: String) -> TlsOptions {
        TlsOptions { hostname, disable_root_store: false, cafile: vec!(), capath: vec!(), client_certs_key: None }
    }

    pub fn to_rustls_config(&self) -> Result<RustlsConfig> {
        let mut it = RustlsConfig::new();
        fn add_cafile(it:&mut RustlsConfig, cafile: &path::Path) -> Result<()> {
            let mut f = io::BufReader::new(fs::File::open(cafile)?);
            it.root_store.add_pem_file(&mut f)
                .map_err(|_| format!("Could not read pem file: {:?}", cafile))?;
            Ok(())
        }
        for cafile in &self.cafile {
            add_cafile(&mut it, cafile)?;
        }
        for capath in &self.capath {
            for cafile in fs::read_dir(capath)? {
                add_cafile(&mut it, &cafile?.path())?;
            }
        }
        if !self.disable_root_store {
            it.root_store.add_server_trust_anchors(&::webpki_roots::TLS_SERVER_ROOTS);
        }
        if let Some((ref c, ref k)) = self.client_certs_key {
            let mut f = io::BufReader::new(fs::File::open(c)?);
            let certs = ::rustls::internal::pemfile::certs(&mut f)
                .map_err(|_| format!("Could not read client cert pem file: {:?}", c))?;
            let mut key = io::BufReader::new(fs::File::open(k)?);
            let mut key = ::rustls::internal::pemfile::pkcs8_private_keys(&mut key)
                .map_err(|_| format!("Could not read client key file: {:?}", k))?;
            let key = key.remove(0);
            it.set_single_client_cert(certs, key);
        }
        Ok(it)
    }
}

#[derive(Clone,Default)]
pub struct MqttOptions {
    /// broker address that you want to connect to
    pub broker_addr: String,
    /// keep alive time to send pingreq to broker when the connection is idle
    pub keep_alive: Option<u16>,
    /// clean (or) persistent session
    pub clean_session: bool,
    /// client identifier
    pub client_id: String,
    /// MQTT username
    pub username: Option<String>,
    /// MQTT password
    pub password: Option<String>,
    /// time left for server to send a connection acknowlegment
    pub mqtt_connection_timeout: Duration,
    /// reconnection options
    pub reconnect: ReconnectOptions,
    /// maximum packet size
    pub max_packet_size: usize,
    /// mqtt will
    pub last_will: Option<::mqtt3::LastWill>,
    /// TLS configuration
    pub tls: Option<TlsOptions>,
    error_callback: Option<Arc<Mutex<FnMut() + Send>>>,
}

impl MqttOptions {
    pub fn new<S1: Into<String>, S2: Into<String>>(id: S1, addr: S2) -> MqttOptions {
        // TODO: Validate client id. Shouldn't be empty or start with spaces
        // TODO: Validate if addr is proper address type
        MqttOptions {
            broker_addr: addr.into(),
            keep_alive: Some(10),
            clean_session: false,
            client_id: id.into(),
            username: None,
            password: None,
            mqtt_connection_timeout: Duration::from_secs(5),
            reconnect: ReconnectOptions::AfterFirstSuccess(Duration::from_secs(10)),
            max_packet_size: 100 * 1024,
            last_will: None,
            tls: None,
            error_callback: None,
        }
    }

    /// Set number of seconds after which client should ping the broker
    /// if there is no other data exchange
    pub fn set_keep_alive(mut self, secs: u16) -> Self {
        if secs < 5 {
            panic!("Keep alives should be greater than 5 secs");
        }

        self.keep_alive = Some(secs);
        self
    }

    /// Set packet size limit (in Kilo Bytes)
    pub fn set_max_packet_size(mut self, sz: usize) -> Self {
        self.max_packet_size = sz * 1024;
        self
    }

    /// `clean_session = true` removes all the state from queues & instructs the broker
    /// to clean all the client state when client disconnects.
    ///
    /// When set `false`, broker will hold the client state and performs pending
    /// operations on the client when reconnection with same `client_id`
    /// happens. Local queue state is also held to retransmit packets after reconnection.
    ///
    /// So **make sure that you manually set `client_id` when `clean_session` is false**
    pub fn set_clean_session(mut self, clean_session: bool) -> Self {
        self.clean_session = clean_session;
        self
    }

    /// Time interval after which client should retry for new
    /// connection if there are any disconnections. By default, no retry will happen
    pub fn set_reconnect_opts(mut self, opts: ReconnectOptions) -> Self {
        self.reconnect = opts;
        self
    }

    /// Set tls option
    /// Supports tls client cert
    pub fn set_tls_opts(mut self, opts: Option<TlsOptions>) -> Self {
        self.tls = opts;
        self
    }

    /// Set MQTT last will
    /// This message will be emit by the broker on disconnect.
    pub fn set_last_will(mut self, will: Option<::mqtt3::LastWill>) -> Self {
        self.last_will = will;
        self
    }

    /// Set the disconnected callback.
    /// This callback will be called when the client has given up and will not try any
    /// reconnection.
    pub fn set_disconnected_callback<F: 'static + FnMut() + Send>(mut self, callback: F) -> Self {
        self.error_callback = Some(Arc::new(Mutex::new(callback)));
        self
    }

    /// Get a reference of the callback
    pub fn get_disconnected_callback(&self) -> Option<Arc<Mutex<FnMut() + Send>>> {
        self.error_callback.as_ref().map(Arc::clone)
    }
}
