"""
End-to-End Simulation (no live Kafka broker required)
------------------------------------------------------
This script simulates the full producer → Kafka → consumer pipeline
in-process using asyncio queues as stand-ins for Kafka topics.

Run with:
    python simulate.py

You will see:
  • Producer publishing each request
  • Consumer processing each request
  • JSON response files written to  responses/
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

from utils.message_schema import build_request_message, build_response_message, serialize, deserialize
from utils.policy_db import lookup_policy
from config.kafka_config import RESPONSE_OUTPUT_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

os.makedirs(RESPONSE_OUTPUT_DIR, exist_ok=True)

# ── In-process "Kafka" queues ─────────────────────────────────────────────────
request_queue:  asyncio.Queue = asyncio.Queue()
response_queue: asyncio.Queue = asyncio.Queue()

# ── Sample input data ─────────────────────────────────────────────────────────
SAMPLE_REQUESTS = [
    {"customer_id": "CUST-001", "name": "Alice Johnson",  "policy_number": "POL-1001"},
    {"customer_id": "CUST-002", "name": "Bob Smith",      "policy_number": "POL-1004"},
    {"customer_id": "CUST-003", "name": "Carol Williams", "policy_number": "POL-1007"},
    {"customer_id": "CUST-004", "name": "David Brown",    "policy_number": "POL-9999"},  # not found
    {"customer_id": "CUST-005", "name": "Eve Davis",      "policy_number": "POL-1010"},
]


# ── Producer coroutine ────────────────────────────────────────────────────────

async def producer():
    log.info("━━━  PRODUCER started  ━━━")
    for req in SAMPLE_REQUESTS:
        message = build_request_message(
            customer_id   = req["customer_id"],
            name          = req["name"],
            policy_number = req["policy_number"],
        )
        raw = serialize(message)
        await request_queue.put(raw)
        log.info(
            "[PRODUCER] Published  customer_id=%-10s  policy_number=%s",
            req["customer_id"], req["policy_number"],
        )
        await asyncio.sleep(0.1)   # simulate slight inter-message delay

    # Sentinel to signal end of stream
    await request_queue.put(None)
    log.info("[PRODUCER] All messages published.")


# ── Consumer coroutine ────────────────────────────────────────────────────────

async def consumer():
    log.info("━━━  CONSUMER started  ━━━")
    while True:
        raw = await request_queue.get()
        if raw is None:
            break

        request = deserialize(raw)
        customer_id   = request["customer_id"]
        policy_number = request["policy_number"]

        log.info(
            "[CONSUMER] Received   customer_id=%-10s  policy_number=%s",
            customer_id, policy_number,
        )

        # Simulate async I/O (DB / API call)
        await asyncio.sleep(0.05)
        policy_data = lookup_policy(policy_number)

        if policy_data:
            response = build_response_message(
                request     = request,
                policy_name = policy_data["policy_name"],
                postcode    = policy_data["postcode"],
                status      = "SUCCESS",
            )
            log.info(
                "[CONSUMER] Resolved   policy_name='%s'  postcode=%s",
                policy_data["policy_name"], policy_data["postcode"],
            )
        else:
            response = build_response_message(
                request     = request,
                policy_name = None,
                postcode    = None,
                status      = "NOT_FOUND",
                error       = f"No policy found for policy_number={policy_number}",
            )
            log.warning("[CONSUMER] NOT FOUND  policy_number=%s", policy_number)

        # Write to file
        filename = f"{customer_id}_{response['request_id']}.json"
        filepath = os.path.join(RESPONSE_OUTPUT_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as fh:
            json.dump(response, fh, indent=4, ensure_ascii=False)
        log.info("[CONSUMER] File written → %s", filepath)

        # Also push to response queue (simulates policy.response topic)
        await response_queue.put(serialize(response))

    log.info("[CONSUMER] Finished processing all messages.")
    await response_queue.put(None)   # signal response reader to stop


# ── Response reader (simulates a downstream consumer) ────────────────────────

async def response_reader():
    log.info("━━━  RESPONSE READER started  ━━━")
    results = []
    while True:
        raw = await response_queue.get()
        if raw is None:
            break
        resp = deserialize(raw)
        results.append(resp)

    log.info("\n%s", "─" * 60)
    log.info("SUMMARY — %d response(s) processed", len(results))
    log.info("─" * 60)
    for r in results:
        status_icon = "✓" if r["status"] == "SUCCESS" else "✗"
        log.info(
            "%s  %-10s  %-30s  policy_name=%-35s  postcode=%s",
            status_icon,
            r["customer_id"],
            r["name"],
            r["policy_name"] or "N/A",
            r["postcode"]    or "N/A",
        )
    log.info("─" * 60)
    log.info("Response files saved in:  ./%s/", RESPONSE_OUTPUT_DIR)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    log.info("=" * 60)
    log.info("  Kafka Policy Lookup System — Simulation")
    log.info("=" * 60)
    await asyncio.gather(
        producer(),
        consumer(),
        response_reader(),
    )


if __name__ == "__main__":
    asyncio.run(main())
