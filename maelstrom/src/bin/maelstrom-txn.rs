use std::collections::BTreeMap;

use gossip_glomers::{Node, KV};
use parking_lot::RwLock;
use serde_json::json;

struct Store {
    prev: BTreeMap<u64, u64>,
    mem: BTreeMap<u64, u64>,
    dirty: bool,
}

impl Store {
    pub fn init(cache: &RwLock<BTreeMap<u64, u64>>) -> Self {
        let cache = cache.read();
        Self {
            prev: cache.clone(),
            mem: cache.clone(),
            dirty: false,
        }
    }

    pub fn load(&mut self, node: &Node) {
        self.prev = node.read(KV::Lin, "head").unwrap_or_default();
        self.mem = self.prev.clone();
    }

    pub fn read(&mut self, k: u64) -> Option<u64> {
        self.mem.get(&k).cloned()
    }

    pub fn write(&mut self, k: u64, v: u64) {
        self.mem.insert(k, v);
        self.dirty = true;
    }

    pub fn commit(&self, node: &Node, cache: &RwLock<BTreeMap<u64, u64>>) -> bool {
        if self.dirty {
            if node
                .cap(KV::Lin, "head", &self.prev, &self.mem, true)
                .is_ok()
            {
                *cache.write() = self.mem.clone();
                true
            } else {
                false
            }
        } else {
            true // Skip readonly transaction
        }
    }
}

fn main() {
    let cache: RwLock<BTreeMap<u64, u64>> = RwLock::new(BTreeMap::new());

    let node = &Node::new();
    node.run(|mut msg| match msg.body["type"].as_str().unwrap() {
        "txn" => {
            let mut txn = msg.body["txn"].take();
            let mut store = Store::init(&cache);
            loop {
                for tx in txn.as_array_mut().unwrap() {
                    let vec = tx.as_array_mut().unwrap();
                    let ty = vec[0].as_str().unwrap();
                    let k = vec[1].as_u64().unwrap();
                    match ty {
                        "r" => {
                            vec[2] = json!(store.read(k));
                        }
                        "w" => store.write(k, vec[2].as_u64().unwrap()),
                        _ => unreachable!(),
                    }
                }
                if store.commit(node, &cache) {
                    break;
                }
                store.load(node);
            }

            node.reply(
                &msg,
                json!({
                    "type": "txn_ok",
                    "txn": txn
                }),
            );
        }
        ty => unreachable!("msg type {ty}"),
    });
}
