# deshred-vs-regular-grpc-latency-bench

A small Rust benchmark for comparing the arrival latency of Solana transaction updates delivered through regular Yellowstone gRPC and Yellowstone deshred gRPC.

The binary connects to a single Yellowstone endpoint, subscribes to the same transaction filter over both streams, matches updates by transaction signature, and prints latency statistics after a fixed run window.

## What it measures

For each matched transaction signature, the program records:

- when the deshred stream delivered the update
- when the regular gRPC stream delivered the update
- the difference between the two timestamps in nanoseconds

At the end of the run it reports:

- total updates seen from each stream
- how many transactions were matched across both streams
- how often deshred arrived earlier vs regular gRPC
- min, max, average, median, P50, P90, and P99 latency

## Usage

```bash
cargo run --release -- --url <YELLOWSTONE_URL> --token <YELLOWSTONE_TOKEN>
```

Example:

```bash
cargo run --release -- \
  --url https://example.rpcpool.com \
  --token your-token-here
```

Optional:

- `--time <seconds>`: how long to keep both streams open before printing results. Defaults to `30`.

## How it works

The benchmark subscribes to transactions involving the program id baked into the binary and compares the timestamps of the first matching update seen on each stream. The current filter is defined in `src/main.rs` and is set to the test program id used for the benchmark.

## Output interpretation

A positive latency means the deshred stream arrived first. A negative latency means the regular gRPC stream arrived first.

The program prints statistics at the end of the measurement window, but it can still show partial counts if only one stream has received matching transactions.
