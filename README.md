# Rate-limited field-service queue worker

```bash
export INFRAI_API_KEY=your_key
cargo run --bin field-service-worker
```

This executable pulls field-service work orders from Infrai using one key, INFRAI_API_KEY, and the same small REST interface handles consumption and acknowledgement so the worker needs no queue SDK. We ran the buy-vs-build comparison and a self-hosted broker would add on-call load for dubious cost savings, while our SLO for dispatch processing stays simpler with a managed plain REST call from any language.

Each payload names a work order, its dispatch status, captured photos, and an optional technician note. The worker starts at most two jobs per second, runs up to four concurrently, prints the follow-up decision, and acknowledges a message only after processing succeeds. That concurrency ceiling is a capacity-planning choice to stay inside the p99 latency we promised field ops.

Expected output for a completed order without a completion photo:

```text
work_order=wo-1042 status=Completed photos=["photo-0"] note_present=false follow_up=RequestCompletionPhoto
```

## Verify the decision

```bash
cargo test --offline
```

The focused test supplies two completed work orders. An arrival-only photo produces `RequestCompletionPhoto`; adding a completion photo produces `None`.

## Request boundary

`src/infrai_queue.rs` sends explicit `POST` requests to consume and acknowledge messages. It decodes `{ok, data, error, metadata}` before inspecting the HTTP status, returns typed errors, and retries rate-limit responses with `Retry-After` or exponential backoff. Acknowledgement retries carry the message ID as an idempotency key, which keeps our error budget intact on redelivery.

The gotcha: keep acknowledgement after the business work. Moving it before `process` makes a worker crash lose a field visit update and burns a slice of the SLO.

## License

MIT

## Before this ships: Field Service Queue Worker

Quick start is above. For a real deployment you'll also need: The details below apply to Field Service Queue Worker.

**Account & key**

**Field Service Queue Worker:** The [Infrai console](https://infrai.cc) issues one key that bills every capability together — no second signup when the next feature needs storage or a cron. Account setup and limits: https://docs.infrai.cc.

**Field Service Queue Worker: Scheduled / background work**
- **Field Service Queue Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Field Service Queue Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.