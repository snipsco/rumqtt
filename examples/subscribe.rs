extern crate mqtt3;
extern crate rumqtt;
extern crate loggerv;

use std::thread;
use std::time::Duration;

use rumqtt::{MqttOptions, ReconnectOptions, MqttClient, QoS};

fn main() {
    loggerv::init_with_verbosity(1).unwrap();
    let mqtt_opts = MqttOptions::new("rumqtt-core", "127.0.0.1:1883")
        .set_reconnect_opts(ReconnectOptions::AfterFirstSuccess(10));

    let mut client = MqttClient::start(mqtt_opts).unwrap();

    client.subscribe("hello/world",
        Box::new(move |msg| println!("Received {:?}", msg))).unwrap();

    for _ in 0..100 {
        client.publish("hello/world", QoS::AtLeastOnce, vec![1, 2, 3]).unwrap();
        thread::sleep(Duration::new(1, 0));
    }

    thread::sleep(Duration::new(60, 0));
}
