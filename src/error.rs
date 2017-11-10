#![allow(unused_doc_comment)]

error_chain! {
    foreign_links {
        Io(::std::io::Error);
        Mqtt3(::mqtt3::Error);
        SyncMpsc(::std::sync::mpsc::TryRecvError);
    }
    errors {
        InvalidState {
            description("invalid state")
            display("invalid state")}
        }
}

/*
quick_error! {
    #[derive(Debug)]
    pub enum Error {
        MpscSend(e: SendError<Request>) {
            from()
        }
        ZeroSubscriptions
        PacketSizeLimitExceeded
    }
}

quick_error! {
    #[derive(Debug, PartialEq)]
    pub enum StateError {
        PingError(e: PingError) {
            from()
        }
        Publish(e: PublishError) {
            from()
        }
        SubscribeError(e: SubscribeError) {
            from()
        }
        PubackError(e: PubackError) {
            from()
        }
    }
}

quick_error! {
    #[derive(Debug, PartialEq)]
    pub enum PingError {
        // when last ping response isn't received
        AwaitPingResp
        // client not in connected state
        InvalidState
        // did not ping with in time
        Timeout
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum ConnectError {
        MqttConnectionRefused(e: mqtt3::ConnectReturnCode) {
            from()
        }
        Io(e: IoError) {
            from()
        }
    }
}

quick_error! {
    #[derive(Debug, PartialEq)]
    pub enum PublishError {
        InvalidState
    }
}

quick_error! {
    #[derive(Debug, PartialEq)]
    pub enum PubackError {
        Unsolicited
    }
}

quick_error! {
    #[derive(Debug, PartialEq)]
    pub enum SubscribeError {
        InvalidState
    }
}

*/

// quick_error! {
//     #[derive(Debug, PartialEq)]
//     pub enum SubackError {
//         // TODO: Add semi rejected error is some of the subscriptions are accepted
//         Rejected
//     }
// }
