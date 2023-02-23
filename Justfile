

echo: build
  (RUST_LOG=maelstrom cd ./maelstrom && ./maelstrom test -w echo --bin ../target/release/echo --node-count 1 --time-limit 10  --log-stderr)

build:
  cargo build --release

debug:
  (cd ./maelstrom && ./maelstrom serve)