use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering::SeqCst},
};

use gossip_glomers::{Node, KV};
use serde_json::json;

struct Store {
    mem: BTreeMap<u64, u64>,
    head: Option<String>,
    unique_id: String,
}

impl Store {
    pub fn init(node: &Node, unique_id: String) -> Self {
        let head: Option<String> = node.read(KV::Lin, "head").ok();
        let mem = head
            .as_ref()
            .map(|k| node.read(KV::Lin, k).unwrap())
            .unwrap_or_default();
        Self {
            head,
            mem,
            unique_id,
        }
    }

    pub fn read(&mut self, k: u64) -> Option<u64> {
        self.mem.get(&k).cloned()
    }

    pub fn write(&mut self, k: u64, v: u64) {
        self.mem.insert(k, v);
    }

    pub fn commit(self, node: &Node) -> bool {
        node.write(KV::Lin, &self.unique_id, self.mem).unwrap();
        node.cap(KV::Lin, "head", self.head, &self.unique_id, true)
            .is_ok()
    }
}

fn main() {
    let counter = AtomicU64::new(0);
    Node::new().run(|node, mut msg| match msg.body["type"].as_str().unwrap() {
        "txn" => {
            let curr = counter.fetch_add(1, SeqCst);
            let unique_id = format!("{}{}", node.id, curr);

            let mut txn = msg.body["txn"].take();
            loop {
                let mut store = Store::init(node, unique_id.clone());
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
                if store.commit(node) {
                    break;
                }
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
