"""
Async Policy Response Consumer
--------------------------------
• Consumes messages from `policy.request`
• Looks up PolicyName + Postcode from the mock DB
• Publishes the enriched response to `policy.response`
• Writes each response as a JSON file to  responses/<customer_id>_<message_id>.json

Usage:
    python consumer/policy_consumer.py
"""

import asyncio
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from config.kafka_config import (
    TOPIC_POLICY_REQUEST,
    TOPIC_POLICY_RESPONSE,
    CONSUMER_CONFIG,
    PRODUCER_CONFIG,
    RESPONSE_OUTPUT_DIR,
)
from utils.message_schema import build_response_message, serialize, deserialize
from utils.policy_db import lookup_policy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [CONSUMER]  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Ensure output directory exists
os.makedirs(RESPONSE_OUTPUT_DIR, exist_ok=True)


# ── File writer ───────────────────────────────────────────────────────────────

async def write_response_file(response: dict) -> str:
    """Write the response dict as a pretty-printed JSON file asynchronously."""
    filename = f"{response['customer_id']}_{response['request_id']}.json"
    filepath = os.path.join(RESPONSE_OUTPUT_DIR, filename)

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _write_json, filepath, response)
    return filepath


def _write_json(filepath: str, data: dict) -> None:
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=4, ensure_ascii=False)


# ── Message processor ─────────────────────────────────────────────────────────

async def process_message(message_value: bytes, producer: KafkaProducer) -> None:
    """Deserialize, enrich, respond, and persist a single request message."""
    try:
        request = deserialize(message_value)
    except Exception as exc:
        log.error("Failed to deserialize message: %s", exc)
        return

    customer_id   = request.get("customer_id", "UNKNOWN")
    policy_number = request.get("policy_number", "")

    log.info("Processing  customer_id=%s  policy_number=%s", customer_id, policy_number)

    # ── Policy lookup ────────────────────────────────────────────────────────
    policy_data = lookup_policy(policy_number)

    if policy_data:
        response = build_response_message(
            request     = request,
            policy_name = policy_data["policy_name"],
            postcode    = policy_data["postcode"],
            status      = "SUCCESS",
        )
        log.info(
            "Found  policy_name='%s'  postcode=%s",
            policy_data["policy_name"],
            policy_data["postcode"],
        )
    else:
        response = build_response_message(
            request     = request,
            policy_name = None,
            postcode    = None,
            status      = "NOT_FOUND",
            error       = f"No policy found for policy_number={policy_number}",
        )
        log.warning("Policy NOT FOUND for policy_number=%s", policy_number)

    # ── Publish response to Kafka ────────────────────────────────────────────
    try:
        future = producer.send(
            TOPIC_POLICY_RESPONSE,
            value = serialize(response),
            key   = customer_id.encode("utf-8"),
        )
        meta = future.get(timeout=10)
        log.info(
            "Response published → topic=%s  partition=%s  offset=%s",
            meta.topic, meta.partition, meta.offset,
        )
    except KafkaError as exc:
        log.error("Failed to publish response: %s", exc)

    # ── Write response file ──────────────────────────────────────────────────
    filepath = await write_response_file(response)
    log.info("Response file written → %s", filepath)


# ── Main consumer loop ────────────────────────────────────────────────────────

async def start_consumer(max_messages: int | None = None) -> None:
    """
    Start the async consumer loop.

    Args:
        max_messages: Stop after processing this many messages (None = run forever).
    """
    loop = asyncio.get_event_loop()

    consumer_cfg = {
        **CONSUMER_CONFIG,
        "value_deserializer": None,   # raw bytes; we deserialize manually
    }
    producer_cfg = {
        **PRODUCER_CONFIG,
        "value_serializer": None,     # we serialize manually
    }

    consumer = KafkaConsumer(TOPIC_POLICY_REQUEST, **consumer_cfg)
    producer = KafkaProducer(**producer_cfg)

    log.info(
        "Consumer started. Listening on topic '%s' (group=%s) …",
        TOPIC_POLICY_REQUEST,
        CONSUMER_CONFIG["consumer_group_id"] if "consumer_group_id" in CONSUMER_CONFIG else CONSUMER_CONFIG.get("group_id"),
    )

    count = 0
    try:
        for kafka_message in consumer:
            await process_message(kafka_message.value, producer)
            count += 1
            if max_messages and count >= max_messages:
                log.info("Reached max_messages=%d, stopping consumer.", max_messages)
                break
    except KeyboardInterrupt:
        log.info("Consumer interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        log.info("Consumer shut down cleanly. Processed %d message(s).", count)


# ── Entry-point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    asyncio.run(start_consumer())
