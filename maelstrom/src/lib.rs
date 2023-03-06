use std::{
    collections::BTreeMap,
    io::{stdin, stdout, BufRead, Write},
    sync::{
        atomic::AtomicU64,
        mpsc::{sync_channel, Receiver, SyncSender},
    },
    thread::spawn,
    time::Duration,
};

use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

pub struct Node {
    sender: Mutex<SyncSender<Msg>>,
    receiver: Mutex<Receiver<Msg>>,
    id_counter: AtomicU64,
    pending: Mutex<BTreeMap<u64, oneshot::Sender<Result<Msg, Err>>>>,
    pub id: String,
    pub node_ids: Vec<String>,
}

impl Node {
    pub fn new() -> Self {
        let receiver = Self::receiver();
        let sender = Self::sender();

        let init = receiver.recv().unwrap();
        assert_eq!(init.body["type"].as_str(), Some("init"));

        let tmp = Self {
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
            id_counter: AtomicU64::new(0),
            receiver: Mutex::new(receiver),
            sender: Mutex::new(sender),
            pending: Mutex::new(BTreeMap::new()),
        };
        tmp.reply(
            &init,
            json!({
                "type": "init_ok"
            }),
        );
        tmp
    }

    fn receiver() -> Receiver<Msg> {
        let (sender, receiver) = sync_channel(1);
        spawn(move || {
            let mut stdin = stdin().lock();
            let mut buf = String::with_capacity(1024);
            loop {
                buf.clear();
                stdin.read_line(&mut buf).unwrap();
                let msg: Msg = serde_json::from_str(&buf).unwrap();
                eprintln!("{} < {} : {}", msg.dest, msg.src, msg.body);
                sender.send(msg).unwrap();
            }
        });
        receiver
    }

    fn sender() -> SyncSender<Msg> {
        let (sender, receiver) = sync_channel(1);
        spawn(move || {
            let mut stdout = stdout().lock();
            let mut buf = Vec::with_capacity(1024);
            loop {
                let msg: Msg = receiver.recv().unwrap();
                buf.clear();
                serde_json::to_writer(&mut buf, &msg).unwrap();
                buf.push(b'\n');
                stdout.write_all(&buf).unwrap();
                eprintln!("{} > {} : {}", msg.src, msg.dest, msg.body);
            }
        });
        sender
    }

    fn next_id(&self) -> u64 {
        self.id_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn other_ids(&self) -> impl Iterator<Item = &String> {
        self.node_ids.iter().filter(|it| **it != self.id)
    }

    pub fn reply(&self, to: &Msg, mut body: Value) {
        body["in_reply_to"] = to.body["msg_id"].as_u64().unwrap().into();
        self.send(to.src.clone(), body);
    }

    fn send(&self, dest: String, body: Value) {
        let msg = Msg {
            src: self.id.clone(),
            dest,
            body,
        };
        self.sender.lock().send(msg).unwrap();
    }

    pub fn rpc(&self, dest: String, mut body: Value) -> Result<Msg, Err> {
        let id = self.next_id();
        // Register this thread for wakeup on RPC response
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().insert(id, sender);
        // Send RPC request
        body["msg_id"] = id.into();
        self.send(dest, body);

        if let Ok(result) = receiver.recv_timeout(Duration::from_secs(1)) {
            // Received response
            result
        } else {
            // Unregister itself
            self.pending.lock().remove(&id);
            // Check for the last instant reception
            if let Ok(result) = receiver.try_recv() {
                result
            } else {
                Err(Err::Timeout)
            }
        }
    }

    pub fn read<M: DeserializeOwned>(&self, kv: KV, key: &str) -> Result<M, Err> {
        self.rpc(kv.id().to_string(), json!({"type": "read", "key": key}))
            .map(|mut m| serde_json::from_value(m.body["value"].take()).unwrap())
    }

    pub fn write<M: Serialize>(&self, kv: KV, key: &str, value: M) -> Result<(), Err> {
        self.rpc(
            kv.id().to_string(),
            json!({"type": "write", "key": key, "value": value}),
        )
        .map(|_| ())
    }

    pub fn cap<A: Serialize, B: Serialize>(
        &self,
        kv: KV,
        key: &str,
        from: A,
        to: B,
        create_if_not_exists: bool,
    ) -> Result<(), Err> {
        self.rpc(kv.id().to_string(), json!({"type": "cas", "key": key, "from": from, "to": to, "create_if_not_exists": create_if_not_exists})).map(|_| ())
    }

    pub fn run<'a>(&'a self, lambda: impl Fn(Msg) + Send + Sync + 'a) {
        std::thread::scope(|s| {
            let receiver = self.receiver.lock();
            loop {
                let msg = receiver.recv().unwrap();
                if let Some(msg_id) = msg.body["in_reply_to"].as_u64() {
                    if let Some(task) = self.pending.lock().remove(&msg_id) {
                        if msg.body["type"].as_str().unwrap() == "error" {
                            let code = msg.body["code"].as_u64().unwrap();
                            task.send(Err(Err::try_from(code).unwrap())).unwrap();
                        } else {
                            task.send(Ok(msg)).unwrap();
                        }
                    }
                } else {
                    s.spawn(|| lambda(msg));
                }
            }
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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
pub enum Err {
    Timeout,
    NodeNotFound,
    NotSupported,
    TemporarilyUnavailable,
    MalformedRequest,
    Crash,
    Abort,
    KeyDoesNotExist,
    KeyAlreadyExists,
    PreconditionFailed,
    TxnConflict,
}

impl Err {
    pub fn msg(self) -> Value {
        let code = match self {
            Err::Timeout => 0,
            Err::NodeNotFound => 1,
            Err::NotSupported => 10,
            Err::TemporarilyUnavailable => 11,
            Err::MalformedRequest => 12,
            Err::Crash => 13,
            Err::Abort => 14,
            Err::KeyDoesNotExist => 20,
            Err::KeyAlreadyExists => 21,
            Err::PreconditionFailed => 22,
            Err::TxnConflict => 30,
        };
        json!({
            "type": "error",
            "code": code
        })
    }
}

impl TryFrom<u64> for Err {
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
