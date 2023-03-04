use gossip_glomers::Node;
use serde_json::json;

fn main() {
    Node::new().run(|node, msg| match msg.body["type"].as_str().unwrap() {
        "echo" => node.reply(
            &msg,
            json!({
                "type": "echo_ok",
                "echo": msg.body["echo"]
            }),
        ),
        ty => unreachable!("msg type {ty}"),
    });
}
