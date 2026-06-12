# Demo runbook — Streamiz enricher

A step-by-step script to demonstrate the full pipeline using the **Streamiz** (Kafka Streams DSL)
enricher and prove each acceptance criterion live. Commands are PowerShell.

> The same demo works with the manual enricher — just use `--profile manual` and the container name
> `enricher`. Only the manual enricher exposes the custom join metrics (step 7).

---

## 0. Start the stack with the Streamiz enricher

```powershell
docker compose --profile streamiz up -d --build
docker compose ps
```

Expect `kafka` + `schema-registry` **healthy** and `proxy-api`, `service-a-api`,
`enricher-streamiz`, `service-b-worker`, `kafka-ui` **up**. (Run only one enricher at a time.)

Confirm the Streamiz topology actually started (state stores restored, tasks RUNNING):

```powershell
docker compose logs enricher-streamiz | Select-String "state transition from RESTORING to RUNNING|Restoration took"
```

## 1. Health checks (all services)

```powershell
"http://localhost:5080","http://localhost:5081","http://localhost:5082","http://localhost:5083" |
  ForEach-Object { "$_/health -> " + (Invoke-RestMethod "$_/health") }
```

All should return `Healthy`.

## 2. Prove Streamiz really materializes KTables (state stores + changelog topics)

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

You'll see the four pipeline topics **plus** Streamiz-managed internal topics, e.g.
`enricher-streamiz-proxy.requests-STATE-STORE-…-changelog` and the `service-a.completed` equivalent.
Those changelog topics ARE the KTable state stores — proof this is a real Kafka Streams join, not a
naive consumer.

## 3. Happy path — one request flows end-to-end

```powershell
$body = @{ customerExternalId = "cust-1"; amount = 42.50; description = "demo order" } | ConvertTo-Json
$r = Invoke-RestMethod http://localhost:5080/api/requests -Method Post -ContentType application/json -Body $body
$r | ConvertTo-Json
$cid = $r.correlationId
"correlationId = $cid"
```

Expect HTTP 200 (after ~Service A's processing delay) with `correlationId` + `serviceAIds`.

Now confirm Service B received the **enriched** command with the matching `operationId`:

```powershell
docker compose logs --since 120s service-b-worker | Select-String $r.serviceAIds.operationId
```

You should see: `Service B processed command for operationId op-… , amount 42.50`.

## 4. Prove the same `correlationId` (key) is on every topic

Open **Kafka UI** → http://localhost:8080 → cluster `local` → **Topics**. For each of
`proxy.requests`, `service-a.completed`, `service-b.ready`:
1. Messages tab → set **Seek** to `Oldest`, **Value Serde** to `SchemaRegistry`, click **Refresh**.
2. Paste your `correlationId` into **Search**.

The record's **Key** equals the `correlationId` on all three, and the value chain is visible:
`proxy.requests` (payload) → `service-a.completed` (serviceAIds) → `service-b.ready` (both merged).

CLI alternative (keys only):

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 `
  --topic service-b.ready --from-beginning --timeout-ms 4000 --property print.key=true
```

## 5. Prove Service B consumes ONLY `service-b.ready` (never raw requests)

Service B subscribes to a single topic. Verify it:

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group service-b
```

The `TOPIC` column shows **only** `service-b.ready` — never `proxy.requests`.

## 6. Prove the join needs BOTH events (no half-joins)

Stop Service A so a request is captured but no completion is ever produced:

```powershell
docker compose stop service-a-api

$body2 = @{ customerExternalId = "cust-noA"; amount = 10; description = "no completion" } | ConvertTo-Json
try { Invoke-RestMethod http://localhost:5080/api/requests -Method Post -ContentType application/json -Body $body2 -TimeoutSec 120 }
catch { "Proxy call failed as expected (Service A down): $($_.Exception.Message)" }
```

The request IS on `proxy.requests` (the Proxy publishes before calling Service A), but there is no
`service-a.completed`. The Streamiz `requests` KTable holds it and emits **nothing**:

```powershell
# No new 'processed command' for cust-noA should appear:
docker compose logs --since 120s service-b-worker | Select-String "processed command"
```

Restore Service A:

```powershell
docker compose start service-a-api
```

## 7. Metrics

```powershell
# Streamiz process/runtime + HTTP metrics:
(Invoke-WebRequest http://localhost:5082/metrics).Content | Select-String "process_runtime|http_server"
```

> The custom join gauges (`enricher_pending_requests`, `enricher_duplicate_records`, …) are exposed by
> the **manual** enricher. To show those, switch profiles (step 9) and hit the same `/metrics`:
> `(Invoke-WebRequest http://localhost:5082/metrics).Content | Select-String "enricher_"`.

## 8. Dead-letter path (Service B failure → `service-b.dlq`)

A description containing "fail" makes the simulated Service B handler throw, exhaust retries, and DLQ:

```powershell
$bad = @{ customerExternalId = "cust-bad"; amount = 5; description = "please fail" } | ConvertTo-Json
$rb = Invoke-RestMethod http://localhost:5080/api/requests -Method Post -ContentType application/json -Body $bad -TimeoutSec 120

docker compose logs --since 120s service-b-worker | Select-String "retries|DLQ|attempt"
```

Then in Kafka UI open **`service-b.dlq`** (Oldest + SchemaRegistry serde): the failed `ServiceBCommand`
is there, with headers `x-error-reason` and `x-origin-topic`.

## 9. Switch back to the manual enricher (and compare)

```powershell
docker compose stop enricher-streamiz
docker compose --profile manual up -d enricher
docker compose logs -f enricher        # watch "Joined request + completion … emitted ServiceBCommand"
```

Repeat step 3 — identical result. Difference to call out: the manual enricher dedupes duplicate
inputs (exactly one output per `correlationId`); a raw Streamiz KTable-KTable join re-emits on every
same-key update.

## 10. Objective proof — the automated acceptance suite

```powershell
dotnet test
```

Runs the 9 Testcontainers cases (real Kafka + Schema Registry): both arrival orders, duplicate
dedup, missing-completion-emits-nothing, Service B reads only `service-b.ready`, DLQ on failure,
enricher restart safety, and correlationId integrity. All green = every acceptance criterion proven.

---

### One-screen summary of what each step proves

| Step | Acceptance criterion proven |
|------|------------------------------|
| 2 | Enricher materializes topics as KTables/state stores (not a naive consumer) |
| 3 | Full async flow; Service B receives the enriched message |
| 4 | `key = correlationId` on every related topic |
| 5 | Service B never consumes raw requests |
| 6 | Emit only when both request + completion exist; order-independent durable state |
| 8 | Bounded retry → DLQ on failure |
| 9 | Both enrichers interchangeable; manual guarantees exactly-once output |
| 10 | All 9 KTable-KTable behaviors proven automatically |
