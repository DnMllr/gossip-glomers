# A Rust implementation of fly.io's [Gossip Glomers](https://fly.io/blog/gossip-glomers/)

Uses [Just](https://just.systems/). `just -l` will list the currently available recipes. This repo includes a copy of the maelstrom `.jar`.

## Crates

### maelstrom

Implements an asynchronous client in Rust for the [maelstrom](https://github.com/jepsen-io/maelstrom) tool. The basic protocol is defined in [messages.rs](./crates/maelstrom/src/messages.rs), while each available [workload](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md) is defined under [workloads](./crates/maelstrom/src/workloads/). The [client](./crates/maelstrom/src/client.rs) can be created with `Client::connect`.

### echo

Application defining a node which can perform the [echo](https://fly.io/dist-sys/1/) workload.

### unique-id

Application defining a node which can perform the [unique-id](https://fly.io/dist-sys/2/) generation task. The generated ID has the following structure: 4 bytes representing the node's ID according to `maelstrom`, 4 bytes of a counter, 4 random bytes, and an 8 byte timestamp.

### broadcast

Application for a fault tolerant and performant [broadcast](https://fly.io/dist-sys/3a/) node.

## TODO

Remaining challenges
