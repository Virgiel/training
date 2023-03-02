kafka2: install
    cd ./maelstrom && ./maelstrom test -w kafka --bin ~/.cargo/bin/maelstrom-kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

kafka: install
    cd ./maelstrom && ./maelstrom test -w kafka --bin ~/.cargo/bin/maelstrom-kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000

counter: install
    cd ./maelstrom && ./maelstrom test -w g-counter --bin ~/.cargo/bin/maelstrom-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition

broadcast: install
    cd ./maelstrom && ./maelstrom test -w broadcast --bin ~/.cargo/bin/maelstrom-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 --nemesis partition 

generate: install
    cd ./maelstrom && ./maelstrom test -w unique-ids --bin ~/.cargo/bin/maelstrom-unique-id --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

echo: install
    cd ./maelstrom && ./maelstrom test -w echo --bin ~/.cargo/bin/maelstrom-echo --node-count 1 --time-limit 10

all: echo generate broadcast counter kafka2

install:
    cargo install --path ./gossip-glomers

debug:
    cd ./maelstrom && ./maelstrom serve