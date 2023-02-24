use std::collections::BTreeMap;

use gossip_glomers::{Error, Msg, Node, KV};
use serde_json::{json, Value};

pub struct PollTask {
    req: Msg,
    offsets: BTreeMap<String, u64>,
    key: String,
    off: u64,
    msgs: BTreeMap<String, Vec<(u64, u64)>>,
}

impl PollTask {
    pub fn new(node: &mut Node, req: Msg) -> Option<(u64, Self)> {
        let offsets: BTreeMap<String, u64> =
            serde_json::from_value(req.body["offsets"].clone()).unwrap();
        let mut tmp = Self {
            req,
            offsets,
            key: String::new(),
            off: 0,
            msgs: BTreeMap::new(),
        };
        match tmp.next_log(node) {
            Some(id) => Some((id, tmp)),
            None => None,
        }
    }

    fn read(&self, node: &mut Node) -> u64 {
        node.read(KV::Lin, &format!("{}_{}", self.key, self.off))
    }

    pub fn next(&mut self, node: &mut Node, value: u64) -> u64 {
        self.msgs
            .entry(self.key.clone())
            .or_default()
            .push((self.off, value));
        self.off += 1;
        self.read(node)
    }

    pub fn next_log(&mut self, node: &mut Node) -> Option<u64> {
        if let Some((key, off)) = self.offsets.pop_first() {
            self.key = key;
            self.off = off;
            Some(self.read(node))
        } else {
            node.reply(&self.req, json!({"type": "poll_ok", "msgs": self.msgs}));
            None
        }
    }
}

pub struct SendTask {
    req: Msg,
    key: String,
    off: u64,
}

impl SendTask {
    pub fn new(node: &mut Node, req: Msg) -> (u64, Self) {
        let key = req.body["key"].as_str().unwrap().to_owned();
        let mut tmp = Self { req, key, off: 0 };
        (tmp.read(node), tmp)
    }

    pub fn read(&mut self, node: &mut Node) -> u64 {
        node.read(KV::Lin, &self.key)
    }

    pub fn increment(&mut self, node: &mut Node, prev: Option<u64>) -> u64 {
        let (from, off) = prev.map(|n| (n.into(), n + 1)).unwrap_or((Value::Null, 0));
        self.off = off;
        node.cap(KV::Lin, &self.key, from, off.into(), prev.is_none())
    }

    pub fn write(&mut self, node: &mut Node) -> u64 {
        let db = format!("{}_{}", self.key, self.off);
        node.write(KV::Lin, &db, self.req.body["msg"].clone())
    }

    pub fn reply(&mut self, node: &mut Node) {
        node.reply(&self.req, json!({"type": "send_ok", "offset": self.off}))
    }

    pub fn on_error(&mut self, node: &mut Node, err: Error) -> u64 {
        match err {
            Error::KeyDoesNotExist => self.increment(node, None),
            Error::PreconditionFailed => self.read(node),
            _ => unreachable!(),
        }
    }
}

pub enum Task {
    Send(SendTask),
    Poll(PollTask),
}

impl Task {
    pub fn send(node: &mut Node, req: Msg) -> (u64, Self) {
        let (id, it) = SendTask::new(node, req);
        (id, Task::Send(it))
    }

    pub fn poll(node: &mut Node, req: Msg) -> Option<(u64, Self)> {
        PollTask::new(node, req).map(|(id, poll)| (id, Task::Poll(poll)))
    }

    pub fn on_read(&mut self, node: &mut Node, value: u64) -> Option<u64> {
        match self {
            Self::Send(it) => Some(it.increment(node, Some(value))),
            Self::Poll(it) => Some(it.next(node, value)),
        }
    }

    pub fn on_cas(&mut self, node: &mut Node) -> Option<u64> {
        match self {
            Self::Send(it) => Some(it.write(node)),
            Self::Poll(_) => unreachable!(),
        }
    }

    pub fn on_write(&mut self, node: &mut Node) -> Option<u64> {
        match self {
            Self::Send(it) => {
                it.reply(node);
                None
            }
            Self::Poll(_) => unreachable!(),
        }
    }

    pub fn on_error(&mut self, node: &mut Node, err: Error) -> Option<u64> {
        match self {
            Self::Send(it) => Some(it.on_error(node, err)),
            Self::Poll(it) => it.next_log(node),
        }
    }
}

fn main() {
    let mut node = Node::new();
    let mut commit: BTreeMap<String, u64> = BTreeMap::new();
    let mut pending: BTreeMap<u64, Task> = BTreeMap::new();

    loop {
        if let Some(mut msg) = Some(node.recv()) {
            match msg.body["type"].as_str().unwrap() {
                "send" => {
                    let (id, task) = Task::send(&mut node, msg);
                    pending.insert(id, task);
                }
                "poll" => {
                    if let Some((id, task)) = Task::poll(&mut node, msg) {
                        pending.insert(id, task);
                    }
                }
                "commit_offsets" => {
                    let offsets: BTreeMap<String, u64> =
                        serde_json::from_value(msg.body["offsets"].take()).unwrap();
                    for (k, offset) in offsets {
                        *commit.entry(k).or_default() = offset;
                    }
                    node.reply(
                        &msg,
                        json!({
                            "type": "commit_offsets_ok"
                        }),
                    );
                }
                "list_committed_offsets" => {
                    let keys: Vec<String> =
                        serde_json::from_value(msg.body["keys"].take()).unwrap();
                    let offsets: BTreeMap<&String, &u64> = keys
                        .iter()
                        .map(|s| (s, commit.get(s).unwrap_or(&0)))
                        .collect();

                    node.reply(
                        &msg,
                        json!({
                            "type": "list_committed_offsets_ok",
                            "offsets": offsets
                        }),
                    );
                }
                "read_ok" => {
                    let msg_id = msg.body["in_reply_to"].as_u64().unwrap();
                    let mut task = pending.remove(&msg_id).unwrap();
                    if let Some(msg_id) =
                        task.on_read(&mut node, msg.body["value"].as_u64().unwrap())
                    {
                        pending.insert(msg_id, task);
                    }
                }
                "cas_ok" => {
                    let msg_id = msg.body["in_reply_to"].as_u64().unwrap();
                    let mut task = pending.remove(&msg_id).unwrap();
                    if let Some(msg_id) = task.on_cas(&mut node) {
                        pending.insert(msg_id, task);
                    }
                }
                "write_ok" => {
                    let msg_id = msg.body["in_reply_to"].as_u64().unwrap();
                    let mut task = pending.remove(&msg_id).unwrap();
                    if let Some(msg_id) = task.on_write(&mut node) {
                        pending.insert(msg_id, task);
                    }
                }
                "error" => {
                    let msg_id = msg.body["in_reply_to"].as_u64().unwrap();
                    let code = msg.body["code"].as_u64().unwrap();
                    let err = Error::try_from(code).unwrap();
                    let mut task = pending.remove(&msg_id).unwrap();
                    if let Some(msg_id) = task.on_error(&mut node, err) {
                        pending.insert(msg_id, task);
                    }
                }
                ty => unreachable!("msg type {ty}"),
            }
        }
    }
}
