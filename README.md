# Async Kafka Policy Lookup System

## Overview

An asynchronous Python system that accepts customer/policy requests via Kafka,
looks up `PolicyName` and `Postcode`, publishes enriched responses back to Kafka,
and writes each response as a JSON file to the `responses/` folder.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     PRODUCER                            │
│  Input JSON → build_request_message() → Kafka           │
│  Topic: policy.request                                  │
└────────────────────────┬────────────────────────────────┘
                         │  (async)
                         ▼
┌─────────────────────────────────────────────────────────┐
│                     CONSUMER                            │
│  policy.request → lookup_policy() → build_response()   │
│      ├── Publishes to: policy.response (Kafka)         │
│      └── Writes file:  responses/<custID>_<msgID>.json │
└─────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
kafka-policy-system/
├── config/
│   └── kafka_config.py          # Broker URL, topic names, output dir
├── producer/
│   └── policy_producer.py       # Async producer — publishes requests
├── consumer/
│   └── policy_consumer.py       # Async consumer — enriches & responds
├── utils/
│   ├── message_schema.py        # Message builders + JSON serialization
│   └── policy_db.py             # Mock policy DB (replace with real DB)
├── responses/                   # ← Response JSON files land here
├── simulate.py                  # In-process simulation (no Kafka needed)
└── README.md
```

---

## Input Message (policy.request topic)

```json
{
  "message_id":    "uuid-v4",
  "timestamp":     "2026-06-04T14:00:00+00:00",
  "customer_id":   "CUST-001",
  "name":          "Alice Johnson",
  "policy_number": "POL-1001"
}
```

## Output Message (policy.response topic + file)

```json
{
  "message_id":    "uuid-v4",
  "request_id":    "original-message-id",
  "timestamp":     "2026-06-04T14:00:01+00:00",
  "customer_id":   "CUST-001",
  "name":          "Alice Johnson",
  "policy_number": "POL-1001",
  "policy_name":   "Comprehensive Health Cover",
  "postcode":      "EC1A 1BB",
  "status":        "SUCCESS",
  "error":         null
}
```

Status values: `SUCCESS` | `NOT_FOUND` | `ERROR`

---

## Quick Start

### Option A — Simulation (no Kafka broker needed)
```bash
python simulate.py
```

### Option B — Live Kafka

1. Start Kafka (Docker):
```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ENABLE_KRAFT=yes \
  -e ALLOW_PLAINTEXT_LISTENER=yes \
  bitnami/kafka:latest
```

2. Start the consumer (terminal 1):
```bash
python consumer/policy_consumer.py
```

3. Run the producer (terminal 2):
```bash
python producer/policy_producer.py
```

4. Check `responses/` for output files.

---

## Configuration

Edit `config/kafka_config.py`:

| Setting                 | Default            | Description                  |
|-------------------------|--------------------|------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address         |
| `TOPIC_POLICY_REQUEST`  | `policy.request`   | Inbound topic                |
| `TOPIC_POLICY_RESPONSE` | `policy.response`  | Outbound topic               |
| `CONSUMER_GROUP_ID`     | `policy-lookup-group` | Consumer group            |
| `RESPONSE_OUTPUT_DIR`   | `responses`        | Folder for output JSON files |

---

## Extending

- **Real database**: Replace `utils/policy_db.py → lookup_policy()` with
  a SQLAlchemy / asyncpg / REST API call.
- **Schema validation**: Add Pydantic models to `utils/message_schema.py`.
- **Dead-letter queue**: Route `NOT_FOUND` / `ERROR` responses to a
  `policy.dlq` topic in `policy_consumer.py`.
- **Multiple consumers**: Increase Kafka partitions and run multiple
  consumer instances with the same `group_id` for horizontal scale.
