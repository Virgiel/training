use std::{collections::BTreeMap, thread::scope};

use gossip_glomers::{Node, KV};
use parking_lot::Mutex;
use serde_json::json;

fn main() {
    let commit: Mutex<BTreeMap<String, u64>> = Mutex::new(BTreeMap::new());
    let node = &Node::new();
    node.run(|mut msg| match msg.body["type"].as_str().unwrap() {
        "send" => {
            let key = msg.body["key"].as_str().unwrap().to_owned();
            loop {
                let result = node.read(KV::Lin, &key);
                let prev: Option<u64> = result.ok();
                let (from, off) = prev.map(|n| (Some(n), n + 1)).unwrap_or((None, 0));
                let result = node.cap(KV::Lin, &key, from, off, prev.is_none());
                if result.is_ok() {
                    let db = format!("{}_{}", key, off);
                    node.write(KV::Lin, &db, &msg.body["msg"]).ok();
                    node.reply(&msg, json!({"type": "send_ok", "offset": off}));
                    break;
                }
            }
        }
        "poll" => {
            let offsets: BTreeMap<String, u64> =
                serde_json::from_value(msg.body["offsets"].take()).unwrap();
            let msgs: BTreeMap<&String, Vec<(u64, u64)>> = scope(|s| {
                offsets
                    .iter()
                    .map(|(k, off)| {
                        s.spawn(move || {
                            let mut off = *off;
                            let mut msgs = Vec::new();
                            while let Ok(msg) = node.read(KV::Lin, &format!("{k}_{off}")) {
                                msgs.push((off, msg));
                                off += 1;
                            }
                            (k, msgs)
                        })
                    })
                    .map(|h| h.join().unwrap())
                    .collect()
            });
            node.reply(&msg, json!({"type": "poll_ok", "msgs": msgs}));
        }
        "commit_offsets" => {
            let offsets: BTreeMap<String, u64> =
                serde_json::from_value(msg.body["offsets"].take()).unwrap();
            {
                let mut lock = commit.lock();
                for (k, offset) in offsets {
                    *lock.entry(k).or_default() = offset;
                }
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
            let offsets: BTreeMap<&String, u64> = {
                let lock = commit.lock();
                keys.iter()
                    .map(|s| (s, *lock.get(s).unwrap_or(&0)))
                    .collect()
            };
            node.reply(
                &msg,
                json!({
                    "type": "list_committed_offsets_ok",
                    "offsets": offsets
                }),
            );
        }
        ty => unreachable!("msg type {ty}"),
    });
}
