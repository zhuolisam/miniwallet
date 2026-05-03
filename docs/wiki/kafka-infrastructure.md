---
title: Kafka Infrastructure — How It All Connects
tags: [phase-2, kafka, docker, infrastructure, event-driven-architecture, system-design]
phase: 1
week: 6
updated: 2026-04-30
---

# Kafka Infrastructure — How It All Connects

This page explains how Kafka is set up and wired into MiniBank at the infrastructure level: what Docker containers run, how they find each other, and what application code makes producers and consumers work. For the logical topology (topics, events, consumer groups), see [[kafka-topics-and-consumers]].

---

## Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────────┐
│  Docker Network                                                             │
│                                                                             │
│  ┌───────────┐       ┌─────────────────────────────────────┐               │
│  │ Zookeeper │◄──────│ Kafka Broker                         │               │
│  │ :2181     │       │                                     │               │
│  └───────────┘       │  Listener 1 (PLAINTEXT):            │               │
│                      │    kafka:9092  ← containers use this │               │
│                      │                                     │               │
│                      │  Listener 2 (PLAINTEXT_HOST):        │               │
│                      │    localhost:29092  ← host uses this  │               │
│                      │                                     │               │
│                      │  Topics:                             │               │
│                      │    transfer.events (1 partition)     │               │
│                      │    account.events  (1 partition)     │               │
│                      └──────────┬──────────────────────────┘               │
│                                 │                                           │
│       ┌─────────────────────────┼────────────────────────┐                 │
│       │                         │                        │                 │
│  ┌────▼────┐         ┌─────────▼─────────┐     ┌────────▼──────────┐      │
│  │   api   │         │  audit-consumer    │     │  kafka-init       │      │
│  │ :8000   │         │  (python -m        │     │  (one-shot:       │      │
│  │         │         │   workers.audit_   │     │   create-topics)  │      │
│  │ Producer│         │   consumer)        │     └───────────────────┘      │
│  └─────────┘         └───────────────────┘                                 │
│                                                                             │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Docker Containers — What Each One Does

| Container | Image | Role | Startup dependency |
|-----------|-------|------|-------------------|
| `zookeeper` | cp-zookeeper:7.6.0 | Cluster coordination: leader election, topic metadata storage | None |
| `kafka` | cp-kafka:7.6.0 | The message broker — stores events in topics, delivers them to consumers | zookeeper (healthy) |
| `kafka-init` | cp-kafka:7.6.0 | One-shot: runs `kafka/create-topics.sh` then exits 0 | kafka (healthy) |
| `api` | Dockerfile (app) | FastAPI + Kafka producer — publishes events inline after transfers | postgres, redis, kafka-init |
| `audit-consumer` | Dockerfile (app) | Kafka consumer — reads events, writes to `audit_events` | postgres, kafka-init |

**Why `kafka-init` is separate:** `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` on the broker. This prevents accidental topic creation from typos (e.g. publishing to `transfer.event` instead of `transfer.events`). Topics are created explicitly by the init script, and all other services wait for it to finish before starting.

**Why Zookeeper:** Kafka < 4.0 uses Zookeeper for cluster metadata. KRaft mode (Kafka 4.0+) removes this dependency, but cp-kafka:7.6.0 still requires it. Think of it as Kafka's internal database for "which brokers exist, which topics exist, who is the leader for each partition."

---

## The Dual Listener Problem

When a Kafka client connects to a broker, the broker responds with an **advertised listener** — the address it tells the client to use for all subsequent communication. This is a redirect, not just a connection.

**Problem with a single listener:**

```
Host machine → connects to localhost:9092 (port-mapped)
  → broker responds: "use kafka:9092 from now on"
    → host DNS cannot resolve "kafka"
      → connection fails
```

**Solution — two listeners:**

| Listener | Binds to | Advertises | Used by |
|----------|----------|-----------|---------|
| PLAINTEXT | 0.0.0.0:9092 | `kafka:9092` | Docker containers (api, audit-consumer, relay) |
| PLAINTEXT_HOST | 0.0.0.0:29092 | `localhost:29092` | Host machine (dev tools, tests run on host) |

Containers set `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` (from `.env`). Host dev overrides to `localhost:29092`.

---

## Configuration Flow

```
.env file
  KAFKA_BOOTSTRAP_SERVERS=kafka:9092
       │
       ▼  (pydantic-settings reads env vars)
app/config.py
  settings.kafka_bootstrap_servers
       │
       ├──► api container:         AIOKafkaProducer(bootstrap_servers=...)
       │
       └──► audit-consumer:        AIOKafkaConsumer(bootstrap_servers=...)
```

A single config field wires both sides. For tests, the `kafka_bootstrap` fixture (from testcontainers) provides a separate address pointing at a throwaway Kafka broker — tests never connect to the Docker Compose broker.

---

## Topic Creation (`kafka/create-topics.sh`)

```bash
kafka-topics --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic transfer.events --partitions 1 --replication-factor 1
```

The script runs inside the `kafka-init` container, which shares the Docker network — so `kafka:9092` resolves. It:
1. Loops until Kafka is accepting connections (retry with sleep)
2. Creates `transfer.events` and `account.events` (idempotent with `--if-not-exists`)
3. Exits 0 — docker-compose marks it as `service_completed_successfully`

**Why 1 partition:** Guarantees total ordering within each topic. Multi-partition would allow parallel consumption but loses ordering guarantees across partitions. Sufficient for learning; production trade-off documented in SYSTEM-DESIGN.md §15.

---

## The Producer (app side)

Lives in `app/services/transfer_service.py` as a **module-level singleton**:

```python
kafka_producer: AIOKafkaProducer | None = None
```

### Lifecycle

| When | What happens | Code |
|------|-------------|------|
| App startup | `start_producer()` called from FastAPI lifespan | Creates producer, calls `.start()` (TCP connect, metadata fetch) |
| Each transfer | `kafka_producer.send_and_wait(topic, value=event)` | Serializes dict → JSON bytes, sends to broker, waits for ack |
| App shutdown | `stop_producer()` called from lifespan | Flushes in-flight sends, closes TCP connection |

### Why a singleton

Creating a producer per request means a new TCP handshake + cluster metadata fetch (~50ms) per request. A shared producer maintains a persistent connection and batches sends internally. This is the standard pattern — `aiokafka` even warns against creating short-lived producers.

### Why `send_and_wait()` not `send()`

`send()` is fire-and-forget — it returns before the broker acknowledges. If the process crashes between `send()` and the broker ack, the event is silently lost. `send_and_wait()` blocks until the broker confirms the write — true at-least-once from producer to broker.

---

## The Consumer (worker side)

Lives in `workers/audit_consumer.py` as a standalone process:

```python
consumer = AIOKafkaConsumer(
    "transfer.events",
    group_id="minibank.audit-consumer",
    bootstrap_servers=settings.kafka_bootstrap_servers,
    enable_auto_commit=False,
)
```

### Key settings

| Setting | Value | Why |
|---------|-------|-----|
| `group_id` | `"minibank.audit-consumer"` | Kafka tracks this group's offset — which messages have been processed |
| `enable_auto_commit` | `False` | We advance the offset only after successful DB write (at-least-once delivery) |
| `auto_offset_reset` | `"earliest"` | On first run (no committed offset), start from the beginning of the topic |

### Message processing loop

```
Kafka broker
    │  async for msg in consumer:  (long-poll — blocks until message available)
    ▼
json.loads(msg.value) → event dict
    │
    ▼
INSERT INTO audit_events  (via db_factory session)
    │
    ▼
consumer.commit()  → tells Kafka "offset N is done, don't resend"
```

### `db_factory` — how the consumer gets DB access

Workers run outside FastAPI, so they can't use `Depends(get_db)`. Instead they import `db_factory` from `app/database.py` — an alias for `AsyncSessionLocal`. Usage:

```python
async with db_factory() as session:
    async with session.begin():
        session.add(AuditEvent(...))
```

Fresh session per message, committed immediately, then discarded. No shared state across messages.

---

## The Full Request Flow (Week 6)

```
1. Client → POST /v1/transfers → api container

2. transfer_service.transfer():
   ① BEGIN
   ② INSERT ledger_entry + transfer
   ③ COMMIT                                    ← money moved (DB is source of truth)
   ④ kafka_producer.send_and_wait(event)       ← outside TX boundary!
   ⑤ Return 201

3. Kafka broker stores the event in transfer.events

4. audit-consumer (separate process):
   ⑥ async for msg in consumer → receives event
   ⑦ INSERT INTO audit_events → COMMIT
   ⑧ consumer.commit() → offset advances
```

**The gap (Week 6's lesson):** Steps ③ and ④ are not atomic. If Kafka is unreachable at step ④, the transfer exists in the DB but the event never reaches the audit consumer. The audit log has a hole. This is the [[dual-write-problem]] that the [[outbox-pattern]] (Week 7) solves.

---

## Testing Infrastructure

Tests use `testcontainers.KafkaContainer` — a separate, disposable Kafka broker started per test session:

```python
@pytest.fixture(scope="session")
def kafka_container():
    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as kafka:
        yield kafka

@pytest.fixture(scope="session")
def kafka_bootstrap(kafka_container) -> str:
    return kafka_container.get_bootstrap_server()  # e.g. "localhost:32789"
```

- **Separate from Docker Compose** — tests don't depend on `docker compose up`
- **Auto-create topics enabled** — testcontainers defaults `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, so no init script needed
- **Random port** — no collision with a running Docker Compose Kafka

Consumer logic (`process()`) is tested **without Kafka** by calling it directly with a dict. Only integration tests (relay, end-to-end) need the Kafka container.

---

## Related

- [[kafka-topics-and-consumers]] — Logical topology: what events flow where, what each consumer does
- [[dead-letter-queue]] — What happens when a consumer fails after 3 retries
- [[eda-saga-and-monolith]] — When event-driven architecture is the right pattern
- [[p2p-transfer-deep-dive]] — How the transfer is committed to DB before events are published
