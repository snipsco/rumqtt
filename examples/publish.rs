extern crate loggerv;
extern crate rumqtt;

use std::thread;
use std::time::Duration;

use rumqtt::{MqttClient, MqttOptions, ReconnectOptions};

fn main() {
    loggerv::init_with_verbosity(1).unwrap();
    let mqtt_opts = MqttOptions::new("rumqtt-core", "127.0.0.1:1883")
        .set_reconnect_opts(ReconnectOptions::Always(10));

    let mut client = MqttClient::start(mqtt_opts).unwrap();

    for _ in 0..100 {
        client
            .publish("hello/world")
            .unwrap()
            .payload(vec![1, 2, 3])
            .send()
            .unwrap();
        thread::sleep(Duration::new(1, 0));
    }

    thread::sleep(Duration::new(60, 0));
}
