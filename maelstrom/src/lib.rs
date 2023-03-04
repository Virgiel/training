use std::{
    collections::BTreeMap,
    io::{stdin, stdout, BufRead, Write},
    sync::{
        atomic::AtomicU64,
        mpsc::{sync_channel, Receiver, SyncSender},
    },
    thread::spawn,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

pub struct Node {
    sender: Mutex<SyncSender<Msg>>,
    receiver: Mutex<Receiver<Msg>>,
    id_counter: AtomicU64,
    tasks: Mutex<TaskMap<oneshot::Sender<Result<Msg, Err>>>>,
    pub id: String,
    pub node_ids: Vec<String>,
}

impl Node {
    pub fn new() -> Self {
        let receiver = Self::receiver();

        let init = receiver.recv().unwrap();
        assert_eq!(init.body["type"].as_str(), Some("init"));
        let id = init.body["node_id"]
            .as_str()
            .map(|it| it.to_string())
            .unwrap();
        let sender = Self::sender(id);

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
            tasks: Mutex::new(TaskMap::new()),
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

    fn receiver() -> Receiver<Msg> {
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
        receiver
    }

    fn sender(id: String) -> SyncSender<Msg> {
        let (sender, receiver) = sync_channel(1);
        spawn(move || {
            let mut stdout = stdout().lock();
            let mut buf = Vec::with_capacity(1024);
            loop {
                let msg = receiver.recv().unwrap();
                buf.clear();
                serde_json::to_writer(&mut buf, &msg).unwrap();
                buf.push(b'\n');
                stdout.write_all(&buf).unwrap();
                eprintln!("{} > {msg:?}", id);
            }
        });
        sender
    }

    fn next_id(&self) -> u64 {
        self.id_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    pub fn reply(&self, to: &Msg, mut body: Value) {
        body["in_reply_to"] = to.body["msg_id"].clone();
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
        body["msg_id"] = id.into();
        let (sender, receiver) = oneshot::channel();
        self.tasks
            .lock()
            .add(sender, id, Instant::now() + Duration::from_secs(1));
        self.send(dest, body);
        receiver.recv().unwrap()
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

    pub fn run(&self, lambda: impl Fn(&Node, Msg) + Send + Sync) {
        std::thread::scope(|s| {
            let receiver = self.receiver.lock();
            loop {
                let now = Instant::now();
                let deadline = {
                    let mut map = self.tasks.lock();

                    // Handle task that reached their deadlines

                    if map.next_deadline().unwrap_or(now) < now {
                        let task = map.pop();
                        task.send(Err(Err::Timeout)).unwrap();
                    }
                    map.next_deadline()
                };

                let msg = match deadline {
                    Some(deadline) => receiver.recv_timeout(deadline.duration_since(now)).ok(),
                    None => receiver.recv().ok(),
                };
                if let Some(msg) = msg {
                    eprintln!("{} < {msg:?}", self.id);
                    if let Some(msg_id) = msg.body["in_reply_to"].as_u64() {
                        if let Some(task) = self.tasks.lock().get(msg_id) {
                            if msg.body["type"].as_str().unwrap() == "error" {
                                let code = msg.body["code"].as_u64().unwrap();
                                task.send(Err(Err::try_from(code).unwrap())).unwrap();
                            } else {
                                task.send(Ok(msg)).unwrap();
                            }
                        }
                    } else {
                        s.spawn(|| lambda(self, msg));
                    }
                }
            }
        })
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
pub enum Err {
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

struct TaskMap<S> {
    task_map: BTreeMap<(Instant, u64), S>,
    id_map: BTreeMap<u64, Instant>,
}

impl<S> TaskMap<S> {
    pub const fn new() -> Self {
        Self {
            task_map: BTreeMap::new(),
            id_map: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, task: S, id: u64, deadline: Instant) {
        self.task_map.insert((deadline, id), task);
        self.id_map.insert(id, deadline);
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.task_map.first_key_value().map(|((t, _), _)| *t)
    }

    pub fn pop(&mut self) -> S {
        let ((_, msg_id), task) = self.task_map.pop_first().unwrap();
        self.id_map.remove(&msg_id);
        task
    }

    pub fn get(&mut self, msg_id: u64) -> Option<S> {
        let deadline = self.id_map.remove(&msg_id)?;
        self.task_map.remove(&(deadline, msg_id))
    }
}
