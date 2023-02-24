use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use gossip_glomers::Node;
use serde_json::json;

fn main() {
    let mut node = Node::new();
    let mut msgs = BTreeSet::new();
    let mut neighbours: Vec<String> = Vec::new();
    let mut pending: Vec<(Instant, u64, Vec<(String, u64)>)> = Vec::new();
    const TIMEOUT: Duration = Duration::from_millis(1000);

    loop {
        // Resend after 1s timeout
        let now = Instant::now();
        for (instant, message, metadata) in pending.iter_mut() {
            if now.duration_since(*instant) > TIMEOUT {
                for (id, msg_id) in metadata {
                    *msg_id = node.rpc(
                        id.clone(),
                        json!({
                            "type": "broadcast",
                            "message": message
                        }),
                    );
                }
                *instant = now;
            }
        }
        let msg = pending
            .iter()
            .map(|(i, _, _)| now.duration_since(*i))
            .min()
            .and_then(|t| node.recv_timeout(t))
            .or_else(|| Some(node.recv()));

        if let Some(mut msg) = msg {
            match msg.body["type"].as_str().unwrap() {
                "broadcast" => {
                    let message = msg.body["message"].as_u64().unwrap();
                    if msgs.insert(message) {
                        let mut tmp = Vec::new();
                        for id in &neighbours {
                            if *id != msg.src {
                                let msg_id = node.rpc(
                                    id.clone(),
                                    json!({
                                        "type": "broadcast",
                                        "message": message
                                    }),
                                );
                                tmp.push((id.clone(), msg_id));
                            }
                        }
                        pending.push((Instant::now(), message, tmp));
                    }
                    node.reply(
                        &msg,
                        json!({
                            "type": "broadcast_ok"
                        }),
                    );
                }
                "broadcast_ok" => {
                    let msg_id = msg.body["in_reply_to"].as_u64().unwrap();
                    pending.retain_mut(|(_, _, metadata)| {
                        metadata.retain(|(_, id)| *id != msg_id);
                        !metadata.is_empty()
                    })
                }
                "read" => {
                    let messages = msgs.clone();
                    node.reply(
                        &msg,
                        json!({
                            "type": "read_ok",
                            "messages": messages
                        }),
                    )
                }
                "topology" => {
                    neighbours =
                        serde_json::from_value(msg.body["topology"][&node.id].take()).unwrap();
                    node.reply(
                        &msg,
                        json!({
                            "type": "topology_ok"
                        }),
                    )
                }
                "error" => {
                    panic!("{msg:?}");
                }
                ty => unreachable!("msg type {ty}"),
            }
        }
    }
}
