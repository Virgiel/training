use std::{
    collections::BTreeMap,
    io::{stdin, stdout, BufRead, StdoutLock, Write},
    sync::mpsc::{sync_channel, Receiver},
    thread::spawn,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub struct Node<'a, S> {
    receiver: Receiver<Msg>,
    pub id: String,
    pub node_ids: Vec<String>,
    id_counter: u64, // Unique per node id for msg and task id
    stdout: StdoutLock<'a>,
    state: S,
    buf: Vec<u8>,
    map: TaskMap<S>,
}

impl<'a, S> Node<'a, S> {
    pub fn new(state: S) -> Self {
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
            map: TaskMap::new(),
            stdout: stdout().lock(),
            buf: Vec::with_capacity(1024),
            id_counter: 0,
            state,
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

    pub fn next_id(&mut self) -> u64 {
        self.id_counter += 1;
        self.id_counter
    }

    pub fn run(&mut self) -> Msg {
        loop {
            // Handle task that reached their deadlines
            let now = Instant::now();

            if self.map.next_deadline().unwrap_or(now) < now {
                let task = self.map.pop();
                task(self, Err(Error::Timeout));
            }
            let msg = match self.map.next_deadline() {
                Some(deadline) => self
                    .receiver
                    .recv_timeout(deadline.duration_since(now))
                    .ok(),
                None => self.receiver.recv().ok(),
            };
            if let Some(msg) = msg {
                eprintln!("{} < {msg:?}", self.id);
                if let Some(msg_id) = msg.body["in_reply_to"].as_u64() {
                    if let Some(task) = self.map.get(msg_id) {
                        if msg.body["type"].as_str().unwrap() == "error" {
                            let code = msg.body["code"].as_u64().unwrap();
                            task(self, Err(Error::try_from(code).unwrap()));
                        } else {
                            task(self, Ok(msg));
                        }
                        continue;
                    }
                }
                return msg;
            }
        }
    }

    pub fn spawn(&mut self, task: Task<S>, deadline: Instant) {
        let id = self.next_id();
        self.map.add(task, id, deadline)
    }

    pub fn rpc(&mut self, dest: String, mut body: Value, task: Task<S>) {
        let id = self.next_id();
        self.map
            .add(task, id, Instant::now() + Duration::from_secs(1));
        body["msg_id"] = id.into();
        self.send(dest, body);
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

    pub fn read(&mut self, kv: KV, key: &str, task: Task<S>) {
        self.rpc(
            kv.id().to_string(),
            json!({"type": "read", "key": key}),
            task,
        )
    }

    pub fn write(&mut self, kv: KV, key: &str, value: Value, task: Task<S>) {
        self.rpc(
            kv.id().to_string(),
            json!({"type": "write", "key": key, "value": value}),
            task,
        )
    }

    pub fn cap(
        &mut self,
        kv: KV,
        key: &str,
        from: Value,
        to: Value,
        create_if_not_exists: bool,
        task: Task<S>,
    ) {
        self.rpc(kv.id().to_string(), json!({"type": "cas", "key": key, "from": from, "to": to, "create_if_not_exists": create_if_not_exists}), task)
    }

    pub fn state(&self) -> &S {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut S {
        &mut self.state
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

type Task<S> = Box<dyn FnOnce(&mut Node<S>, Result<Msg, Error>)>;

struct TaskMap<S> {
    task_map: BTreeMap<(Instant, u64), Task<S>>,
    id_map: BTreeMap<u64, Instant>,
}

impl<S> TaskMap<S> {
    pub fn new() -> Self {
        Self {
            task_map: BTreeMap::new(),
            id_map: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, task: Task<S>, id: u64, deadline: Instant) {
        self.task_map.insert((deadline, id), task);
        self.id_map.insert(id, deadline);
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.task_map.first_key_value().map(|((t, _), _)| *t)
    }

    pub fn pop(&mut self) -> Task<S> {
        let ((_, msg_id), task) = self.task_map.pop_first().unwrap();
        self.id_map.remove(&msg_id);
        task
    }

    pub fn get(&mut self, msg_id: u64) -> Option<Task<S>> {
        let deadline = self.id_map.remove(&msg_id)?;
        self.task_map.remove(&(deadline, msg_id))
    }
}
