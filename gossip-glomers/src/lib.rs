use std::{
    io::{stdin, stdout, BufRead, StdoutLock, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{sync_channel, Receiver},
    },
    thread::spawn,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub struct Node<'a> {
    receiver: Receiver<Msg>,
    pub id: String,
    pub node_ids: Vec<String>,
    msg_id: AtomicU64,
    stdout: StdoutLock<'a>,
    buf: Vec<u8>,
}

impl<'a> Node<'a> {
    pub fn new() -> Self {
        let (sender, receiver) = sync_channel(1);
        spawn(move || {
            let mut stdin = stdin().lock();
            let mut buf = String::with_capacity(1024);
            loop {
                buf.clear();
                stdin.read_line(&mut buf).unwrap();
                let msg: Msg = serde_json::from_str(&buf).unwrap();
                sender.send(msg).unwrap()
            }
        });

        let init = receiver.recv().unwrap();
        assert_eq!(init.body["type"].as_str(), Some("init"));

        let mut tmp = Self {
            id: init.body["node_id"]
                .as_str()
                .map(|it| it.to_string())
                .unwrap(),
            node_ids: init.body["node_ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|it| it.as_str().map(|it| it.to_string()).unwrap())
                .collect(),
            receiver,
            msg_id: AtomicU64::new(0),
            stdout: stdout().lock(),
            buf: Vec::with_capacity(1024),
        };
        eprintln!("{} : {init:?}", tmp.id);
        tmp.reply(
            &init,
            json!({
                "type": "init_ok"
            }),
        );
        tmp
    }

    pub fn recv(&self) -> Msg {
        let msg = self.receiver.recv().unwrap();
        eprintln!("{} <{msg:?}", self.id);
        return msg;
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Option<Msg> {
        self.receiver.recv_timeout(timeout).ok()
    }

    pub fn rpc(&mut self, dest: String, mut body: Value) -> u64 {
        let msg_id = self.msg_id.fetch_add(1, Ordering::SeqCst);
        body["msg_id"] = msg_id.into();
        self.send(dest, body);
        msg_id
    }

    pub fn reply(&mut self, to: &Msg, mut body: Value) {
        body["in_reply_to"] = to.body["msg_id"].clone();
        self.send(to.src.clone(), body);
    }

    fn send(&mut self, dest: String, body: Value) {
        let msg = Msg {
            src: self.id.clone(),
            dest,
            body,
        };
        self.buf.clear();
        serde_json::to_writer(&mut self.buf, &msg).unwrap();
        self.buf.push(b'\n');
        self.stdout.write_all(&self.buf).unwrap();
        eprintln!("{} > {msg:?}", self.id);
    }

    pub fn read(&mut self, kv: KV, key: &str) -> u64 {
        self.rpc(kv.id().to_string(), json!({"type": "read", "key": key}))
    }

    pub fn write(&mut self, kv: KV, key: &str, value: Value) -> u64 {
        self.rpc(
            kv.id().to_string(),
            json!({"type": "write", "key": key, "value": value}),
        )
    }

    pub fn cap(
        &mut self,
        kv: KV,
        key: &str,
        from: Value,
        to: Value,
        create_if_not_exists: bool,
    ) -> u64 {
        self.rpc(kv.id().to_string(), json!({"type": "cas", "key": key, "from": from, "to": to, "create_if_not_exists": create_if_not_exists}))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Msg {
    pub src: String,
    pub dest: String,
    pub body: serde_json::Value,
}

#[derive(Clone, Copy)]
pub enum KV {
    Lin,
    Seq,
    LWW,
}

impl KV {
    pub fn id(self) -> &'static str {
        match self {
            KV::Lin => "lin-kv",
            KV::Seq => "seq-kv",
            KV::LWW => "lww-kv",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Error {
    Timeout = 0,
    NodeNotFound = 1,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TxnConflict = 30,
}

impl Into<u64> for Error {
    fn into(self) -> u64 {
        match self {
            Error::Timeout => 0,
            Error::NodeNotFound => 1,
            Error::NotSupported => 10,
            Error::TemporarilyUnavailable => 11,
            Error::MalformedRequest => 12,
            Error::Crash => 13,
            Error::Abort => 14,
            Error::KeyDoesNotExist => 20,
            Error::KeyAlreadyExists => 21,
            Error::PreconditionFailed => 22,
            Error::TxnConflict => 30,
        }
    }
}

impl TryFrom<u64> for Error {
    type Error = u64;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Timeout,
            1 => Self::NodeNotFound,
            10 => Self::NotSupported,
            11 => Self::TemporarilyUnavailable,
            12 => Self::MalformedRequest,
            13 => Self::Crash,
            14 => Self::Abort,
            20 => Self::KeyDoesNotExist,
            21 => Self::KeyAlreadyExists,
            22 => Self::PreconditionFailed,
            30 => Self::TxnConflict,
            unknown => return Err(unknown),
        })
    }
}
