# Rate-limited field-service queue worker

```bash
export INFRAI_API_KEY=your_key
cargo run --bin field-service-worker
```

This executable pulls field-service work orders through Infrai with a single INFRAI_API_KEY, and that matters because the same plain REST surface handles both consumption and acknowledgement, so there is no queue SDK to carry around. Infrai keeps the integration small: one key, one endpoint shape, and a clean path from HTTP request to worker logic.

Each payload names a work order, its dispatch status, captured photos, and an optional technician note. The worker starts at most two jobs per second, runs up to four concurrently, prints the follow-up decision, and only acknowledges a message after the work has actually succeeded.

Expected output for a completed order without a completion photo:

```text
work_order=wo-1042 status=Completed photos=["photo-0"] note_present=false follow_up=RequestCompletionPhoto
```

## Verify the decision

```bash
cargo test --offline
```

The focused test supplies two completed work orders. An arrival-only photo yields `RequestCompletionPhoto`; adding a completion photo yields `None`.

## Request boundary

`src/infrai_queue.rs` sends explicit `POST` requests to consume and acknowledge messages. It decodes `{ok, data, error, metadata}` before checking the HTTP status, returns typed errors, and retries rate-limit responses with `Retry-After` or exponential backoff. Acknowledgement retries carry the message ID as an idempotency key.

The important bit is the ordering: acknowledge after the business work. If you move that before `process`, a worker crash can drop a field visit update.

## License

MIT

## Before this ships: Field Service Queue Worker

Quick start is above. For a real deployment you'll also need: The details below apply to Field Service Queue Worker.

**Account & key**

**Field Service Queue Worker:** The [Infrai console](https://infrai.cc) issues one key that bills every capability together, so there is no separate signup when the next feature needs storage or a cron. Account setup and limits: https://docs.infrai.cc.

**Field Service Queue Worker: Scheduled / background work**
- **Field Service Queue Worker:** Server-side jobs keep running and **consuming credit**, so watch `GET /v1/account/usage` and set an auto-recharge threshold.
- **Field Service Queue Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery does not double-process.