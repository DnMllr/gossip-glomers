

echo: build
  (RUST_LOG=maelstrom cd ./maelstrom && ./maelstrom test -w echo --bin ../target/release/echo --node-count 1 --time-limit 10  --log-stderr)

unique-id: build
  (RUST_LOG=maelstrom cd ./maelstrom && ./maelstrom test -w unique-ids --bin ../target/release/unique-id --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition  --log-stderr)

broadcast-1: build
  (RUST_LOG=maelstrom cd ./maelstrom && ./maelstrom test -w broadcast --bin ../target/release/broadcast --node-count 1 --time-limit 20 --rate 10 --log-stderr)

build:
  cargo build --release

debug:
  (cd ./maelstrom && ./maelstrom serve)