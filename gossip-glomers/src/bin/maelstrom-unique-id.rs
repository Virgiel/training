use gossip_glomers::Node;
use serde_json::json;

fn main() {
    let mut node = Node::new(());
    let mut counter = 0;
    loop {
        let msg = node.run();
        match msg.body["type"].as_str().unwrap() {
            "generate" => {
                let id = format!("{}{}", node.id, counter);
                counter += 1;
                node.reply(
                    &msg,
                    json!({
                        "type": "generate_ok",
                        "id": id
                    }),
                )
            }
            ty => unreachable!("msg type {ty}"),
        }
    }
}
