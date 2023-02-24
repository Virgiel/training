use std::collections::BTreeMap;

use gossip_glomers::{Msg, Node, KV};
use serde_json::{json, Value};

fn main() {
    let mut node = Node::new(());
    let mut commit: BTreeMap<String, u64> = BTreeMap::new();

    loop {
        let mut msg = node.run();
        match msg.body["type"].as_str().unwrap() {
            "send" => {
                fn task(node: &mut Node<()>, req: Msg) {
                    let key = req.body["key"].as_str().unwrap().to_owned();
                    node.read(
                        KV::Lin,
                        &key.clone(),
                        Box::new(move |node, result| {
                            let prev = result.ok().map(|msg| msg.body["value"].as_u64().unwrap());
                            let (from, off) =
                                prev.map(|n| (n.into(), n + 1)).unwrap_or((Value::Null, 0));
                            node.cap(
                                KV::Lin,
                                &key.clone(),
                                from,
                                off.into(),
                                prev.is_none(),
                                Box::new(move |node, result| match result {
                                    Ok(_) => {
                                        let db = format!("{}_{}", key, off);
                                        node.write(
                                            KV::Lin,
                                            &db,
                                            req.body["msg"].clone(),
                                            Box::new(move |node, _| {
                                                node.reply(
                                                    &req,
                                                    json!({"type": "send_ok", "offset": off}),
                                                )
                                            }),
                                        )
                                    }
                                    Err(_) => task(node, req),
                                }),
                            )
                        }),
                    )
                }
                task(&mut node, msg);
            }
            "poll" => {
                pub struct PollTask {
                    req: Msg,
                    offsets: BTreeMap<String, u64>,
                    key: String,
                    off: u64,
                    msgs: BTreeMap<String, Vec<(u64, u64)>>,
                }

                impl PollTask {
                    pub fn run(node: &mut Node<()>, req: Msg) {
                        let offsets: BTreeMap<String, u64> =
                            serde_json::from_value(req.body["offsets"].clone()).unwrap();
                        Self {
                            req,
                            offsets,
                            key: String::new(),
                            off: 0,
                            msgs: BTreeMap::new(),
                        }
                        .next_log(node);
                    }

                    fn read(self, node: &mut Node<()>) {
                        node.read(
                            KV::Lin,
                            &format!("{}_{}", self.key, self.off),
                            Box::new(|node, result| match result {
                                Ok(msg) => {
                                    let value = msg.body["value"].as_u64().unwrap();
                                    self.next(node, value)
                                }
                                Err(_) => self.next_log(node),
                            }),
                        )
                    }

                    pub fn next(mut self, node: &mut Node<()>, value: u64) {
                        self.msgs
                            .entry(self.key.clone())
                            .or_default()
                            .push((self.off, value));
                        self.off += 1;
                        self.read(node)
                    }

                    pub fn next_log(mut self, node: &mut Node<()>) {
                        if let Some((key, off)) = self.offsets.pop_first() {
                            self.key = key;
                            self.off = off;
                            self.read(node)
                        } else {
                            node.reply(&self.req, json!({"type": "poll_ok", "msgs": self.msgs}));
                        }
                    }
                }
                PollTask::run(&mut node, msg)
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
                let keys: Vec<String> = serde_json::from_value(msg.body["keys"].take()).unwrap();
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
            ty => unreachable!("msg type {ty}"),
        }
    }
}
