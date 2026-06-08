"""
Async Policy Request Producer
------------------------------
Reads a list of customer/policy requests and publishes each one as a
JSON message to the `policy.request` Kafka topic.

Usage (standalone):
    python producer/policy_producer.py

Or import and call:
    from producer.policy_producer import publish_requests
    await publish_requests(requests)
"""

import asyncio
import json
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.kafka_config import TOPIC_POLICY_REQUEST, PRODUCER_CONFIG
from utils.message_schema import build_request_message, serialize

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [PRODUCER]  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Core async publish function ───────────────────────────────────────────────

async def publish_requests(requests: list[dict]) -> None:
    """
    Publish a list of policy-request dicts to Kafka asynchronously.

    Each dict must have keys: customer_id, name, policy_number
    """
    loop = asyncio.get_event_loop()

    # KafkaProducer is synchronous; we run it in a thread-pool executor so the
    # event loop stays non-blocking.
    await loop.run_in_executor(None, _sync_publish, requests)


def _sync_publish(requests: list[dict]) -> None:
    """Blocking publish – called from the executor."""
    cfg = {**PRODUCER_CONFIG, "value_serializer": serialize}
    producer = KafkaProducer(**cfg)

    try:
        for req in requests:
            message = build_request_message(
                customer_id   = req["customer_id"],
                name          = req["name"],
                policy_number = req["policy_number"],
            )
            future = producer.send(
                TOPIC_POLICY_REQUEST,
                value = message,
                key   = req["customer_id"].encode("utf-8"),
            )
            # Block only long enough to confirm the send (non-blocking overall
            # because we're already inside an executor thread).
            record_meta = future.get(timeout=10)
            log.info(
                "Published → topic=%s  partition=%s  offset=%s  customer_id=%s  policy=%s",
                record_meta.topic,
                record_meta.partition,
                record_meta.offset,
                req["customer_id"],
                req["policy_number"],
            )
    except KafkaError as exc:
        log.error("Kafka publish error: %s", exc)
        raise
    finally:
        producer.flush()
        producer.close()
        log.info("Producer closed.")


# ── Example payload ───────────────────────────────────────────────────────────

SAMPLE_REQUESTS = [
    {"customer_id": "CUST-001", "name": "Alice Johnson",  "policy_number": "POL-1001"},
    {"customer_id": "CUST-002", "name": "Bob Smith",      "policy_number": "POL-1004"},
    {"customer_id": "CUST-003", "name": "Carol Williams", "policy_number": "POL-1007"},
    {"customer_id": "CUST-004", "name": "David Brown",    "policy_number": "POL-9999"},  # not found
    {"customer_id": "CUST-005", "name": "Eve Davis",      "policy_number": "POL-1010"},
]


# ── Entry-point ───────────────────────────────────────────────────────────────

async def main():
    log.info("Starting Policy Request Producer …")
    log.info("Publishing %d request(s) to topic '%s'", len(SAMPLE_REQUESTS), TOPIC_POLICY_REQUEST)
    await publish_requests(SAMPLE_REQUESTS)
    log.info("All requests published successfully.")


if __name__ == "__main__":
    asyncio.run(main())
