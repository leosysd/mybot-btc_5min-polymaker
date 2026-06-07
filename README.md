# mybot-btc_5min-polymaker

Low-latency Rust skeleton for a BTC 5-minute binary market maker.

The design is multi-process on purpose:

- `collector` publishes fresh market frames.
- `quote-engine` calculates fair value, two-sided quotes, and inventory skew.
- `order-gateway` owns the hot order path. It is dry-run only in this scaffold.
- `risk-ledger` accounts fills, inventory, and PnL scenarios.
- `supervisor` starts and restarts the workers.

Hot-path IPC uses Unix datagram sockets under `run/sockets/`. JSONL files are audit logs only.

## Run

```bash
cp .env.example .env
cargo build --release
./target/release/polymaker clean
./target/release/polymaker supervisor --seconds 15
```

Inspect:

```bash
ls run
tail -n 5 run/quotes.jsonl
tail -n 5 run/fills.jsonl
cat run/inventory.json
```

Stop:

```bash
./target/release/polymaker stop
```

## Why This Shape

The lowest practical live latency comes from keeping the order gateway isolated and warm. The quote engine should never wait on logs, dashboards, or data collectors. Unix datagram IPC adds tiny local overhead while preventing a slow worker from dragging the hot order path down.

This first version intentionally keeps real trading disabled. The live Polymarket SDK should be connected inside `order-gateway`, after the socket pipeline and risk controls are verified in dry-run.
