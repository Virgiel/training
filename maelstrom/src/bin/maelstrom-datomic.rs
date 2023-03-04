use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::{AtomicU64, Ordering::SeqCst},
    thread::scope,
};

use gossip_glomers::{Msg, Node, KV};
use serde_json::{json, Value};

struct IdGen {
    node: String,
    counter: AtomicU64,
}

impl IdGen {
    pub fn new(node: &Node) -> Self {
        Self {
            node: node.id.clone(),
            counter: AtomicU64::new(0),
        }
    }

    pub fn next(&self) -> String {
        let curr = self.counter.fetch_add(1, SeqCst);
        format!("{}_{curr}", self.node)
    }
}

type Txn = Vec<(String, u64, Value)>;

pub struct Transactor {
    gen: IdGen,
    root: Option<String>,
    mem: BTreeMap<u64, String>,
}

impl Transactor {
    pub fn new(node: &Node) -> Self {
        Self {
            gen: IdGen::new(node),
            root: None,
            mem: BTreeMap::new(),
        }
    }

    pub fn run(&mut self, node: &Node, mut txns: Vec<Txn>) -> Vec<Txn> {
        let mut cache: BTreeMap<String, Option<Vec<u64>>> = BTreeMap::new();
        // List all read and written id for RPC parallelism
        let (read_id, write_id) = {
            let mut read_id = BTreeSet::new();
            let mut write_id = BTreeMap::new();

            for (ty, k, _) in txns.iter().flatten() {
                // Only register read for value we have not written
                if !write_id.contains_key(k) {
                    read_id.insert(*k);
                }
                if ty == "append" {
                    // Use only one new id per written key
                    write_id.entry(*k).or_insert_with(|| self.gen.next());
                }
            }
            (read_id, write_id)
        };
        let new_root = self.gen.next();
        loop {
            // Refresh root if another node have committed a transaction
            let db_root = node.read(KV::Lin, "root").ok();
            if db_root != self.root {
                self.root = db_root;
                self.mem = self
                    .root
                    .as_ref()
                    .map(|r| node.read(KV::Lin, r).unwrap())
                    .unwrap_or_default()
            }

            // Batch read
            scope(|s| {
                let reads: Vec<_> = read_id
                    .iter()
                    // Only read uncached keys present in the database
                    .filter_map(|i| self.mem.get(i).filter(|s| !cache.contains_key(*s)))
                    .map(|id| (id, s.spawn(|| node.read(KV::Lin, id).ok())))
                    .collect();
                for (k, v) in reads {
                    let v = v.join().unwrap();
                    cache.insert(k.clone(), v);
                }
            });

            // Run transactions
            for (ty, k, v) in txns.iter_mut().flatten() {
                let value = self.mem.get(k).and_then(|id| cache[id].clone());
                match ty.as_str() {
                    "r" => {
                        *v = json!(value);
                    }
                    "append" => {
                        let mut prev = value.unwrap_or_default();
                        prev.push(v.as_u64().unwrap());
                        let id = &write_id[k];
                        self.mem.insert(*k, id.clone());
                        cache.insert(id.clone(), Some(prev));
                    }
                    _ => unreachable!(),
                }
            }

            // Try to commit
            if write_id.is_empty() {
                // Read only transactions, only check for root change
                if self.root == node.read(KV::Lin, "root").ok() {
                    return txns;
                }
            } else {
                // Batch writes
                scope(|s| {
                    for k in write_id.keys() {
                        s.spawn(|| {
                            let id = &self.mem[k];
                            let v = &cache[id];
                            node.write(KV::Lin, id, v).unwrap();
                        });
                        s.spawn(|| {
                            node.write(KV::Lin, &new_root, &self.mem).unwrap();
                        });
                    }
                });
                let ok = node
                    .cap(KV::Lin, "root", &self.root, &new_root, true)
                    .is_ok();
                if ok {
                    self.root = Some(new_root);
                    return txns;
                }
            };
        }
    }
}

fn main() {
    let node = Node::new();
    let (sender, receiver) = std::sync::mpsc::sync_channel::<Msg>(100);
    scope(|s| {
        let node = &node;
        s.spawn(move || {
            let mut msgs = Vec::with_capacity(100);
            let mut actor = Transactor::new(node);
            loop {
                msgs.push(receiver.recv().unwrap());
                while let Ok(new) = receiver.try_recv() {
                    msgs.push(new);
                }
                let txns = msgs
                    .iter_mut()
                    .map(|m| serde_json::from_value(m.body["txn"].take()).unwrap())
                    .collect();
                let result = actor.run(node, txns);
                for (msg, result) in msgs.drain(..).zip(result.into_iter()) {
                    node.reply(
                        &msg,
                        json!({
                            "type": "txn_ok",
                            "txn": result
                        }),
                    );
                }
            }
        });
        node.run(|_, msg| match msg.body["type"].as_str().unwrap() {
            "txn" => sender.send(msg).unwrap(),
            ty => unreachable!("msg type {ty}"),
        });
    });
}
