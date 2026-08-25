# telepathos-rs

Rust home of the steering plane: the meta agent, lane registry, and the
go-between service for Hermes.

Layering mirrors pi-mono's philosophy — core logic as libraries, product
assembly at the edges, providers injected into the loop rather than hardwired:

| crate | role | pi-mono analog |
|---|---|---|
| `proto` | wire frame types + defensive parsing | `packages/protocol` |
| `lanes` | lane registry, persistence, meta grammar | session model |
| `steering` | agent loop, typed tools, injectable `Provider` | `packages/agent` |
| `telepathosd` | binary: lane HTTP API, future relay connector | product assembly |

## Invariants (hold the line)

- The steering agent's tool surface is typed tools only. No bash, no fs.
- `parse_meta` requires registry evidence before intercepting lane names —
  collision safety with coding speech is structural.
- Wire types here MUST stay in lockstep with `server/src/protocol.ts`
  and `android/.../Protocol.kt`. All three compilers are the test suite.

## Build

```sh
cargo build          # debug
cargo test           # unit + integration tests, no network needed
cargo run -p telepathosd
```
