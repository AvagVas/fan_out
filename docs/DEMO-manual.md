# Fan-out pipeline — demo guide (manual enricher)

A self-contained walkthrough of the fan-out pipeline using the **manual enricher** (the default
`KTable ⋈ KTable` join implementation). Each step calls out what it proves. Commands are PowerShell.

> For the **Streamiz** (Kafka Streams DSL) enricher variant, see [DEMO.md](DEMO.md).

## What you're demonstrating

```
POST /api/requests ─► Proxy ──(proxy.requests)──┐
                       │                         ├─► Enricher ──(service-b.ready)──► Service B
                       └─► Service A ─(service-a.completed)─┘            │
                                                                    (service-b.dlq on failure)
```

The Enricher is a `KTable ⋈ KTable` inner join on `correlationId`: it emits a `ServiceBCommand` to
`service-b.ready` **only when both** the request and Service A's completion exist for the same key —
in **completion order**.

## Prerequisites

- Docker Desktop running
- .NET 10 SDK (only for step 11, the automated tests)
- Ports free: `5080–5083`, `8080`, `8081`, `9092`

**Service ports:** Proxy `5080`, Service A `5081`, Enricher `5082`, Service B `5083`,
Kafka UI `8080`, Schema Registry `8081`, Kafka `9092`.

---

## 1. Start the stack (manual enricher)

```powershell
docker compose --profile manual up -d --build
docker compose ps
```

> Both enrichers sit behind compose profiles, so you **must** pass `--profile manual` (or
> `--profile streamiz`) — without it, no enricher starts. Run only **one** enricher at a time.

Expect `kafka` + `schema-registry` **healthy**, and `proxy-api`, `service-a-api`, `enricher`,
`service-b-worker`, `kafka-ui` **up**. The `kafka-init` container creates the four topics and exits
(that's normal).

Confirm the manual enricher subscribed to both inputs:

```powershell
docker compose logs enricher | Select-String "subscribed to"
```

## 2. Health checks

```powershell
"http://localhost:5080","http://localhost:5081","http://localhost:5082","http://localhost:5083" |
  ForEach-Object { "$_/health -> " + (Invoke-RestMethod "$_/health") }
```

All four should return `Healthy`.

## 3. Happy path — one request end-to-end

```powershell
$body = @{ customerExternalId = "cust-1"; amount = 42.50; description = "demo order" } | ConvertTo-Json
$r = Invoke-RestMethod http://localhost:5080/api/requests -Method Post -ContentType application/json -Body $body
$r | ConvertTo-Json
$cid = $r.correlationId
"correlationId = $cid"
```

The call takes **~10 s** — Service A simulates work via `ProcessingDelayMs: 10000`, and the Proxy
waits for it synchronously. You get HTTP 200 with `correlationId` + `serviceAIds`.

Confirm the Enricher joined and Service B received the enriched command:

```powershell
docker compose logs --since 120s enricher         | Select-String $cid
docker compose logs --since 120s service-b-worker | Select-String $r.serviceAIds.operationId
```

You should see the enricher's *"Joined request + completion … emitted ServiceBCommand"* and
Service B's *"processed command for operationId op-… , amount 42.50"*. **Proves:** the full async
fan-out works and Service B gets the merged result.

## 4. Prove the fan-out + `key = correlationId` on every topic

Open **Kafka UI** → http://localhost:8080 → cluster `local` → **Topics**. For each of
`proxy.requests`, `service-a.completed`, `service-b.ready`:

1. **Messages** tab → **Seek** = `Oldest`, **Value Serde** = `SchemaRegistry` → **Refresh**.
2. Paste your `correlationId` into **Search**.

The record **Key** equals the `correlationId` on all three, and you can see the value chain grow:
`proxy.requests` (payload) → `service-a.completed` (serviceAIds) → `service-b.ready` (both merged).

CLI alternative (keys on the output topic):

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 `
  --topic service-b.ready --from-beginning --timeout-ms 4000 --property print.key=true
```

## 5. Prove Service B consumes ONLY `service-b.ready`

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group service-b
```

The `TOPIC` column shows **only** `service-b.ready` — never the raw `proxy.requests`.
**Proves:** Service B never sees un-enriched requests.

## 6. Prove the join needs BOTH events (no half-joins)

Stop Service A so a request is captured but no completion is ever produced:

```powershell
docker compose stop service-a-api

$body2 = @{ customerExternalId = "cust-noA"; amount = 10; description = "no completion" } | ConvertTo-Json
try { Invoke-RestMethod http://localhost:5080/api/requests -Method Post -ContentType application/json -Body $body2 -TimeoutSec 60 }
catch { "Proxy call failed as expected (Service A down): $($_.Exception.Message)" }
```

The request lands on `proxy.requests` (the Proxy publishes *before* calling Service A), but there's no
completion, so the enricher's `requests` KTable holds it and emits **nothing**:

```powershell
docker compose logs --since 120s enricher | Select-String "Unmatched"          # waiting for counterpart
docker compose logs --since 120s service-b-worker | Select-String "cust-noA"   # (no output expected)
```

Restore Service A — the held request joins automatically when the completion arrives:

```powershell
docker compose start service-a-api
```

**Proves:** emit only when both sides exist; order-independent durable state.

## 7. Duplicate dedup (manual enricher = exactly-once output)

The manual enricher's `emitted` ledger guarantees one output per `correlationId`, so a re-consumed
input never double-emits:

```powershell
docker compose logs --since 300s enricher | Select-String "Duplicate input"
```

**Proves:** the manual join dedupes (a raw Streamiz KTable-KTable join would re-emit on every same-key
update — the trade-off called out in [DEMO.md](DEMO.md) step 9).

## 8. Dead-letter path (`service-b.dlq`)

A description containing **"fail"** makes the simulated Service B handler throw, exhaust its 3 retries,
and route to the DLQ:

```powershell
$bad = @{ customerExternalId = "cust-bad"; amount = 5; description = "please fail" } | ConvertTo-Json
$rb = Invoke-RestMethod http://localhost:5080/api/requests -Method Post -ContentType application/json -Body $bad -TimeoutSec 60

docker compose logs --since 120s service-b-worker | Select-String "attempt|retries|DLQ"
```

Then in Kafka UI open **`service-b.dlq`** (Oldest + SchemaRegistry serde): the failed `ServiceBCommand`
is there with headers `x-error-reason` and `x-origin-topic`. **Proves:** bounded retry → DLQ; one
poison record never blocks the partition.

## 9. Metrics (manual enricher's custom join gauges)

```powershell
(Invoke-WebRequest http://localhost:5082/metrics).Content | Select-String "enricher_"
```

Shows `enricher_joined_records`, `enricher_duplicate_records`, `enricher_unmatched_records`,
`enricher_pending_requests`, `enricher_pending_completions`. **Proves:** observability into the join's
health/backlog.

## 10. Restart safety

```powershell
docker compose restart enricher
docker compose logs --since 60s enricher | Select-String "subscribed to"
```

Re-submit a request — state (the SQLite KTable + `emitted` ledger) survives because it's persisted to
the `enricher-state` volume. **Proves:** no lost pending records across a crash/restart.

## 11. Objective proof — automated acceptance suite

```powershell
dotnet test
```

Runs the 9 Testcontainers cases against a real Kafka + Schema Registry (both arrival orders, dedup,
missing-completion-emits-nothing, Service-B-reads-only-ready, DLQ, restart safety, correlationId
integrity). All green = every criterion proven without manual clicking.

## 12. Teardown

```powershell
docker compose --profile manual down -v   # -v also drops the enricher-state volume
```

---

## What each step proves (one screen)

| Step | Proven |
|------|--------|
| 3 | Full async fan-out; Service B gets the enriched command |
| 4 | `key = correlationId` on every related topic |
| 5 | Service B consumes only `service-b.ready` |
| 6 | Join emits only when both events exist; order-independent |
| 7 | Manual enricher = exactly-once output per key |
| 8 | Bounded retry → DLQ on failure |
| 9 | Join metrics exposed |
| 10 | Restart-safe durable state |
| 11 | All behaviors proven automatically |

---

### Notes

- To compare the **Streamiz** enricher: `docker compose stop enricher` then
  `docker compose --profile streamiz up -d enricher-streamiz` — see [DEMO.md](DEMO.md) for the
  Streamiz-specific steps (internal changelog topics, etc.).
- This demos **completion-order** fan-out. Strict **Request1-before-Request2** ordering is not part of
  this build.
