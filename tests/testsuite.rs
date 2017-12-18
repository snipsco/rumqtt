#![allow(unused_variables)]
#[macro_use]
extern crate log;
extern crate loggerv;
extern crate mqtt3;
extern crate rumqtt;

use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use mqtt3::{MqttRead, MqttWrite};
use rumqtt::{MqttClient, MqttOptions, QoS, ReconnectOptions};

const MOSQUITTO_ADDR: &'static str = "test.mosquitto.org:1883";
const MOSQUITTO_TLS_ADDR: &'static str = "test.mosquitto.org:8883";

/// Shouldn't try to reconnect if there is a connection problem
/// during initial tcp connect.
#[test]
fn inital_tcp_connect_failure() {
    // env_logger::init().unwrap();
    // TODO: Bugfix. Client hanging when connecting to broker.hivemq.com:9999
    let client_options = MqttOptions::new("me", "localhost:9999");

    // Connects to a broker and returns a `request`
    assert!(MqttClient::start(client_options).is_err());
}

fn spawn_silent_server(port: u16) {
    use std::io::Read;
    let server = ::std::net::TcpListener::bind(("localhost", port)).unwrap();
    ::std::thread::spawn(move || {
        let mut stream = server.incoming().next().unwrap().unwrap().bytes();
        while let Some(_) = stream.next() {}
    });
}

// After connecting to tcp, should timeout error if it didn't receive CONNACK
#[test]
fn initial_mqtt_connect_timeout_failure() {
    // loggerv::init_with_level(log::LogLevel::Debug);
    // TODO: Change the host to remote host and fix blocks
    spawn_silent_server(19999);
    let client_options = MqttOptions::new("me", "localhost:19999");
    let result = MqttClient::start(client_options);
    assert!(result.is_err());
    assert!(result.err().unwrap().description().contains("connack"));
}

fn spawn_server_after(port: u16, delay: Duration) {
    ::std::thread::spawn(move || {
        ::std::thread::sleep(delay);
        let server = ::std::net::TcpListener::bind(("localhost", port)).unwrap();
        let mut stream = server.incoming().next().unwrap().unwrap();
        let connect_packet = stream.read_packet().unwrap();
        stream
            .write_packet(&mqtt3::Packet::Connack(mqtt3::Connack {
                session_present: false,
                code: mqtt3::ConnectReturnCode::Accepted,
            }))
            .unwrap();
    });
}

// Should reconnect at initial connection if reconnect set to Always
#[test]
#[ignore]
fn initial_mqtt_reconnect() {
    // loggerv::init_with_level(log::LogLevel::Debug);
    spawn_server_after(19991, Duration::from_secs(2));
    let client_options = MqttOptions::new("reco", "localhost:1991")
        .set_reconnect_opts(ReconnectOptions::Always(Duration::from_secs(3)));

    // Connects to a broker and returns a `request`
    let result = MqttClient::start(client_options);
    assert!(result.is_ok());
}

// Shouldn't try to reconnect if there is a connection problem
// during initial mqtt connect.
#[test]
fn inital_mqtt_connect_failure() {
    let client_options = MqttOptions::new("connect", "broken.mosquitto.org:8883")
//        .set_reconnect(5);
        ;

    // Connects to a broker and returns a `request`
    let client = MqttClient::start(client_options);
    assert!(client.is_err());
}

#[test]
fn basic_publishes_and_subscribes() {
    // loggerv::init_with_level(log::LogLevel::Debug);
    let client_options = MqttOptions::new("pubsub", MOSQUITTO_ADDR);
    let count = Arc::new(AtomicUsize::new(0));
    let final_count = count.clone();
    let count = count.clone();

    let mut request = MqttClient::start(client_options).expect("Coudn't start");
    info!("Started");
    request
        .subscribe(
            "test/basic",
            Box::new(move |_| {
                count.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .unwrap()
        .send()
        .unwrap();
    info!("subbed");

    let payload = format!("hello rust");
    request
        .publish("test/basic")
        .unwrap()
        .payload(payload.clone().into_bytes())
        .send()
        .unwrap();
    request
        .publish("test/basic")
        .unwrap()
        .qos(QoS::AtLeastOnce)
        .payload(payload.clone().into_bytes())
        .send()
        .unwrap();
    request
        .publish("test/basic")
        .unwrap()
        .qos(QoS::AtLeastOnce)
        .payload(payload.clone().into_bytes())
        .send()
        .unwrap();
    /*
    request.publish("test/basic", QoS::ExactlyOnce, payload.clone().into_bytes())
        .unwrap();
    */
    thread::sleep(Duration::new(3, 0));

    assert_eq!(3, final_count.load(Ordering::SeqCst));
}

#[test]
fn alive() {
    // loggerv::init_with_level(log::LogLevel::Debug);
    let client_options = MqttOptions::new("keep-alive", MOSQUITTO_ADDR).set_keep_alive(5);
    let mut request = MqttClient::start(client_options).expect("Coudn't start");
    assert!(request.connected());
    std::thread::sleep(std::time::Duration::from_secs(10));
    assert!(request.connected());
}

fn server_that_drops_connection_after_three_secs(port: u16) {
    ::std::thread::spawn(move || {
        let server = ::std::net::TcpListener::bind(("localhost", port)).unwrap();
        for stream in server.incoming() {
            debug!("accepting connection");
            let mut stream = stream.unwrap();
            let connect_packet = stream.read_packet();
            stream
                .write_packet(&mqtt3::Packet::Connack(mqtt3::Connack {
                    session_present: false,
                    code: mqtt3::ConnectReturnCode::Accepted,
                }))
                .unwrap();
            std::thread::sleep(std::time::Duration::from_secs(3));
            debug!("dropping connection");
        }
    });
}

#[test]
fn detect_disconnection() {
    // loggerv::init_with_level(log::LogLevel::Debug).unwrap();
    debug!("kokoo");
    server_that_drops_connection_after_three_secs(19993);
    let client_options =
        MqttOptions::new("deco", "localhost:19993").set_reconnect_opts(ReconnectOptions::Never);
    let mut request = MqttClient::start(client_options).expect("Coudn't start");
    assert!(request.connected());
    std::thread::sleep(std::time::Duration::from_secs(5));
    debug!("alive?");
    assert!(!request.connected());
}

#[test]
fn reconnection_on_drop() {
    // loggerv::init_with_level(log::LogLevel::Debug).unwrap();
    server_that_drops_connection_after_three_secs(19994);
    let client_options = MqttOptions::new("reco", "localhost:19994")
        .set_reconnect_opts(ReconnectOptions::Always(Duration::from_secs(1)));
    let mut request = MqttClient::start(client_options).expect("Coudn't start");
    assert!(request.connected());
    std::thread::sleep(std::time::Duration::from_secs(5));
    assert!(request.connected());
}

/*
#[test]
fn simple_reconnection() {
    // env_logger::init().unwrap();
    let client_options = MqttOptions::new()
        .set_keep_alive(5)
        .set_reconnect(5)
        .set_client_id("test-reconnect-client")
        .set_broker(MOSQUITTO_ADDR);

    // Message count
    let count = Arc::new(AtomicUsize::new(0));
    let final_count = count.clone();
    let count = count.clone();

    let counter_cb = move |message| {
        count.fetch_add(1, Ordering::SeqCst);
        // println!("message --> {:?}", message);
    };

    let msg_callback = MqttCallback::new().on_message(counter_cb);

    // Connects to a broker and returns a `request`
    let mut request = MqttClient::start(client_options, Some(msg_callback)).expect("Coudn't start");

    // Register message callback and subscribe
    let topics = vec![("test/reconnect", QoS::Level2)];
    request.subscribe(topics).expect("Subcription failure");

    request.disconnect().unwrap();
    // Wait for reconnection and publish
    thread::sleep(Duration::new(10, 0));

    let payload = format!("hello rust");
    request.publish("test/reconnect", QoS::Level1, payload.clone().into_bytes())
        .unwrap();

    // Wait for count to be incremented by callback
    thread::sleep(Duration::new(5, 0));
    assert!(1 == final_count.load(Ordering::SeqCst));
}
*/

/*
#[test]
fn acked_message() {
    let client_options = MqttOptions::new("test-acked-message")
        .set_reconnect(5)
        .set_client_id("test-acked-message")
        .set_broker(MOSQUITTO_ADDR);

    let cb = |m: Message| {
        let ref payload = *m.payload;
        let ref userdata = *m.userdata.unwrap();
        let payload = String::from_utf8(payload.clone()).unwrap();
        let userdata = String::from_utf8(userdata.clone()).unwrap();
        assert_eq!("MYUNIQUEMESSAGE".to_string(), payload);
        assert_eq!("MYUNIQUEUSERDATA".to_string(), userdata);
    };

    // Connects to a broker and returns a `request`
    let mut request = MqttClient::start(client_options);
    request.subscribe_object(Subscription{
    }).expect("Couldn't start");
    request.userdata_publish("test/qos1/ack",
                          QoS::Level1,
                          "MYUNIQUEMESSAGE".to_string().into_bytes(),
                          "MYUNIQUEUSERDATA".to_string().into_bytes())
        .unwrap();
    thread::sleep(Duration::new(1, 0));
}
*/

/*
#[test]
fn will() {
    let count = Arc::new(AtomicUsize::new(0));
    let final_count = count.clone();

    let client_options_1 = MqttOptions::new("test-will-c1", MOSQUITTO_ADDR);
    let mut c1 = MqttClient::start(client_options_1).expect("Coudn't start");

    let client_options_2 = MqttOptions::new("test-will-c2", MOSQUITTO_ADDR);
    let mut c2 = MqttClient::start(client_options_2).expect("Coudn't start");

    c1.disconnect();
    c2.subscribe("test/will", Box::new(move |p| final_count.fetch_add(1, Ordering::SeqCst))).send();

    // let client1 = MqttClient::start(client_options1, None).expect("Coudn't
    // start");
    // let mut client2 = MqttClient::start(client_options2,
    // Some(callback)).expect("Coudn't start");

    // client2.subscribe(vec![("test/will", QoS::Level0)]).unwrap();

    // TODO: Now we are waiting for cli-2 subscriber to finish before
    // disconnecting
    // cli-1. Make an sync version of subscribe()

    thread::sleep(Duration::new(1, 0));

    // LWT doesn't work on graceful disconnects
    // client1.disconnect();
    // client1.shutdown().unwrap();

    // Wait for last will publish
    // thread::sleep(Duration::new(5, 0));
    // assert!(1 == final_count.load(Ordering::SeqCst));
}
*/


/// Broker should retain published message on a topic and
/// INSTANTLY publish them to new subscritions
#[test]
fn retained_messages() {
    let client_options = MqttOptions::new("retain", MOSQUITTO_ADDR);
    let mut client = MqttClient::start(client_options).expect("Coudn't start");
    let (tx, rx) = std::sync::mpsc::channel();

    client
        .publish("test/retain")
        .unwrap()
        .payload(b"hello rust".to_vec())
        .retain(true)
        .send()
        .unwrap();

    thread::sleep(Duration::new(3, 0));

    let client_options = MqttOptions::new("retain_2", MOSQUITTO_ADDR);
    let mut client_2 = MqttClient::start(client_options).expect("Coudn't start");
    client_2
        .subscribe(
            "test/retain",
            Box::new(move |msg| {
                tx.send(msg.payload.clone()).unwrap();
            }),
        )
        .unwrap()
        .send()
        .unwrap();

    thread::sleep(Duration::new(3, 0));

    let result = rx.recv();
    println!("result: {:?}", result);
    assert_eq!(*result.unwrap(), b"hello rust");
}

/*

// TODO: Add functionality to handle noreconnect option. This test case is
// panicking
// with out set_reconnect
#[test]
fn qos0_stress_publish() {
    let client_options = MqttOptions::new()
        .set_reconnect(3)
        .set_client_id("qos0-stress-publish")
        .set_broker(BROKER_ADDRESS);

    let count = Arc::new(AtomicUsize::new(0));
    let final_count = count.clone();
    let count = count.clone();

    let cb = move |m: Message| {
        count.fetch_add(1, Ordering::SeqCst);
    };

    let callback = MqttCallback::new().on_message(cb);

    let mut client = MqttClient::start(client_options, Some(callback)).expect("Coudn't start");

    client.subscribe(vec![("test/qos0/stress", QoS::Level2)]).expect("Subcription failure");

    for i in 0..1000 {
        let payload = format!("{}. hello rust", i);
        client.publish("test/qos0/stress", QoS::Level0, payload.clone().into_bytes()).unwrap();
        thread::sleep(Duration::new(0, 10000));
    }

    thread::sleep(Duration::new(10, 0));
    println!("QoS0 Final Count = {:?}", final_count.load(Ordering::SeqCst));
    assert!(950 <= final_count.load(Ordering::SeqCst));
}

#[test]
fn simple_qos1_stress_publish() {
    // env_logger::init().unwrap();
    let client_options = MqttOptions::new()
        .set_reconnect(3)
        .set_client_id("qos1-stress-publish")
        .set_pub_q_len(50)
        .set_broker(BROKER_ADDRESS);

    // TODO: Alert!!! Mosquitto seems to be unable to publish fast (loosing
    // messsages
    // with mosquitto broker. local and remote)

    let count = Arc::new(AtomicUsize::new(0));
    let final_count = count.clone();
    let count = count.clone();

    let cb = move |m: Message| {
        count.fetch_add(1, Ordering::SeqCst);
    };

    let callback = MqttCallback::new().on_publish(cb);

    let mut client = MqttClient::start(client_options, Some(callback)).expect("Coudn't start");

    for i in 0..1000 {
        let payload = format!("{}. hello rust", i);
        client.publish("test/qos1/stress", QoS::Level1, payload.clone().into_bytes()).unwrap();
    }

    thread::sleep(Duration::new(10, 0));
    println!("QoS1 Final Count = {:?}", final_count.load(Ordering::SeqCst));
    assert!(1000 <= final_count.load(Ordering::SeqCst));
}

#[test]
/// This test tests if all packets are being published after reconnections
/// NOTE: Previous tests used subscribes to same topic to decide if all the
/// publishes are successful. When reconnections are involved, some publishes
/// might happen before subscription is successful. You can verify this by
/// keeping prints at CONNACK, SUBACK & _PUBLISH(). After connection is
/// successful
/// you'll see some publishes before SUBACK.
fn qos1_stress_publish_with_reconnections() {
    // env_logger::init().unwrap();
    let client_options = MqttOptions::new()
        .set_reconnect(3)
        .set_client_id("qos1-stress-reconnect-publish")
        .set_clean_session(false)
        .set_pub_q_len(50)
        .set_broker(BROKER_ADDRESS);

    let count = Arc::new(AtomicUsize::new(0));
    let final_count = count.clone();
    let count = count.clone();

    let cb = move |m: Message| {
        count.fetch_add(1, Ordering::SeqCst);
    };

    let callback = MqttCallback::new().on_publish(cb);

    let mut client = MqttClient::start(client_options, Some(callback)).expect("Coudn't start");

    for i in 0..1000 {
        let payload = format!("{}. hello rust", i);
        if i == 100 || i == 500 || i == 900 {
            let _ = client.disconnect();
        }
        client.publish("test/qos1/reconnection_stress", QoS::Level1, payload.clone().into_bytes()).unwrap();
    }

    thread::sleep(Duration::new(30, 0));
    println!("QoS1 Final Count = {:?}", final_count.load(Ordering::SeqCst));
    assert!(1000 <= final_count.load(Ordering::SeqCst));
}

#[test]
#[ignore] // this test does not pass if the server drops initial pub msgs
fn simple_qos2_stress_publish() {
    //env_logger::init().unwrap();
    let client_options = MqttOptions::new()
        .set_reconnect(3)
        .set_client_id("qos2-stress-publish")
        .set_broker(BROKER_ADDRESS);

    let count = Arc::new(AtomicUsize::new(0));
    let final_count = count.clone();
    let count = count.clone();

    let cb = move |m: Message| {
        count.fetch_add(1, Ordering::SeqCst);
    };

    let callback = MqttCallback::new().on_publish(cb);

    let mut client = MqttClient::start(client_options, Some(callback)).expect("Coudn't start");


    for i in 0..1000 {
        let payload = format!("{}. hello rust", i);
        client.publish("test/qos2/stress", QoS::Level2, payload.clone().into_bytes()).unwrap();
    }

    thread::sleep(Duration::new(30, 0));
    println!("QoS2 Final Count = {:?}", final_count.load(Ordering::SeqCst));
    assert!(1000 == final_count.load(Ordering::SeqCst));
}

#[test]
#[ignore] // this test does not pass if the server drops initial pub msgs
fn qos2_stress_publish_with_reconnections() {
    // env_logger::init().unwrap();
    let client_options = MqttOptions::new()
        .set_reconnect(3)
        .set_clean_session(false)
        .set_client_id("qos2-stress-reconnect-publish")
        .set_broker(BROKER_ADDRESS);

    let count = Arc::new(AtomicUsize::new(0));
    let final_count = count.clone();
    let count = count.clone();

    let cb = move |m: Message| {
        count.fetch_add(1, Ordering::SeqCst);
    };

    let callback = MqttCallback::new().on_publish(cb);

    let mut client = MqttClient::start(client_options, Some(callback)).expect("Coudn't start");


    for i in 0..1000 {
        let payload = format!("{}. hello rust", i);
        if i == 40 || i == 500 || i == 900 {
            let _ = client.disconnect();
        }
        client.publish("test/qos2/reconnection_stress", QoS::Level2, payload.clone().into_bytes()).unwrap();
    }

    thread::sleep(Duration::new(30, 0));
    println!("QoS2 Final Count = {:?}", final_count.load(Ordering::SeqCst));
    assert!(1000 == final_count.load(Ordering::SeqCst));
}
*/

#[test]
fn tls() {
    // loggerv::init_with_level(log::LogLevel::Debug);
    let mut ssl = rumqtt::RustlsConfig::new();
    // let cafile = include_bytes!("mosquitto.org.ca.crt");
    //let cafile = include_bytes!("mosquitto.org.ca.crt");
    let cafile = include_bytes!("../test-ca/ca.cert");
    let mut pcafile:&[u8] = &cafile[..];
    ssl.root_store.add_pem_file(&mut pcafile).unwrap();
    let client_options = MqttOptions::new("keep-alive", "localhost:8883")
        .set_keep_alive(5)
        .set_tls_opts(Some(rumqtt::TlsOptions::new("localhost".into(), ssl)));
    let mut request = MqttClient::start(client_options).expect("Coudn't start");
    let count = Arc::new(AtomicUsize::new(0));
    let final_count = count.clone();
    let count = count.clone();
    info!("Started");
    request
        .subscribe(
            "test/basic",
            Box::new(move |_| {
                count.fetch_add(1, Ordering::SeqCst);
            }),
        )
        .unwrap()
        .send()
        .unwrap();
    info!("subbed");

    let payload = format!("hello rust");
    request
        .publish("test/basic")
        .unwrap()
        .payload(payload.clone().into_bytes())
        .send()
        .unwrap();
    request
        .publish("test/basic")
        .unwrap()
        .qos(QoS::AtLeastOnce)
        .payload(payload.clone().into_bytes())
        .send()
        .unwrap();
    request
        .publish("test/basic")
        .unwrap()
        .qos(QoS::AtLeastOnce)
        .payload(payload.clone().into_bytes())
        .send()
        .unwrap();
    /*
    request.publish("test/basic", QoS::ExactlyOnce, payload.clone().into_bytes())
        .unwrap();
    */
    thread::sleep(Duration::new(3, 0));

    assert_eq!(3, final_count.load(Ordering::SeqCst));
    assert!(request.connected());
}

/*
// NOTE: POTENTIAL MOSQUITTO BUG
// client publishing 1..40 and disconnect and 40..46(with errors) before read
// triggered
// broker receives 1..40 but sends acks for only 1..20  (don't know why)
// client reconnects and sends 21..46 again and received pubrecs (qos2 publish
// queue empty)
// broker now sends pubrecs from 21..X resulting in unsolicited records

// doesn't seem to be a problem with qos1
// emqttd doesn't have this issue

// #[test]
// fn qos2_stress_publish_with_reconnections() {
//     env_logger::init().unwrap();
//     let client_options = MqttOptions::new()
//                                     .set_keep_alive(5)
//                                     .set_reconnect(3)
//                                     .set_clean_session(false)
//
// .set_client_id("qos2-stress-reconnect-publish")
//                                     .set_broker(BROKER_ADDRESS);

//     let count = Arc::new(AtomicUsize::new(0));
//     let final_count = count.clone();
//     let count = count.clone();

// let request = MqttClient::new(client_options).publish_callback(move
// |message| {
//         count.fetch_add(1, Ordering::SeqCst);
// // println!("{}. message --> {:?}", count.load(Ordering::SeqCst),
// message);
//     }).start().expect("Coudn't start");

//     for i in 0..50 {
//         let payload = format!("{}. hello rust", i);
//         if i == 40 || i == 500 || i == 900 {
//             let _ = request.disconnect();
//         }
// request.publish("test/qos2/reconnection_stress",  QoS::Level2,
// payload.clone().into_bytes()).unwrap();
//     }

//     thread::sleep(Duration::new(10, 0));
//     println!("QoS2 Final Count = {:?}", final_count.load(Ordering::SeqCst));
//     assert!(50 == final_count.load(Ordering::SeqCst));
// }
*/
