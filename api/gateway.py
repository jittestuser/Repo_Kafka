"""
FastAPI REST Gateway
---------------------
Exposes HTTP endpoints that accept policy lookup requests,
publish them to Kafka (policy.request), and return an immediate
acknowledgement. The async consumer picks them up and writes
the response files to  responses/

Endpoints:
  POST /policy/lookup          → single request
  POST /policy/lookup/batch    → multiple requests
  GET  /policy/status/{msg_id} → check if response file exists
  GET  /health                 → liveness probe

Run:
  uvicorn api.gateway:app --reload --port 8000
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from utils.message_schema import build_request_message, serialize
from config.kafka_config import (
    TOPIC_POLICY_REQUEST,
    PRODUCER_CONFIG,
    RESPONSE_OUTPUT_DIR,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [GATEWAY]  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

os.makedirs(RESPONSE_OUTPUT_DIR, exist_ok=True)

# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Policy Lookup API",
    description="REST gateway that publishes requests to Kafka and returns enriched policy data.",
    version="1.0.0",
)


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class PolicyRequest(BaseModel):
    customer_id:   str = Field(..., example="CUST-001")
    name:          str = Field(..., example="Alice Johnson")
    policy_number: str = Field(..., example="POL-1001")


class PolicyRequestBatch(BaseModel):
    requests: list[PolicyRequest]


class AckResponse(BaseModel):
    message_id:    str
    customer_id:   str
    policy_number: str
    status:        str = "ACCEPTED"
    message:       str
    timestamp:     str


class BatchAckResponse(BaseModel):
    accepted:  int
    messages:  list[AckResponse]


# ── Kafka publish helper ──────────────────────────────────────────────────────

def _kafka_publish(messages: list[dict]) -> None:
    """Synchronous publish — called in a thread-pool executor."""
    try:
        from kafka import KafkaProducer
        cfg = {**PRODUCER_CONFIG, "value_serializer": serialize}
        producer = KafkaProducer(**cfg)
        for msg in messages:
            producer.send(
                TOPIC_POLICY_REQUEST,
                value=msg,
                key=msg["customer_id"].encode("utf-8"),
            )
            log.info("Published  customer_id=%s  policy=%s", msg["customer_id"], msg["policy_number"])
        producer.flush()
        producer.close()
    except Exception as exc:
        log.error("Kafka publish failed: %s", exc)
        raise


async def publish_to_kafka(messages: list[dict]) -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _kafka_publish, messages)


# ── Simulate mode (no live Kafka) ─────────────────────────────────────────────

def _simulate_response(request_msg: dict) -> None:
    """
    Write a response file directly (used when Kafka is not available).
    Mirrors what the consumer does.
    """
    from utils.policy_db import lookup_policy
    from utils.message_schema import build_response_message

    policy_data = lookup_policy(request_msg["policy_number"])
    if policy_data:
        response = build_response_message(
            request=request_msg,
            policy_name=policy_data["policy_name"],
            postcode=policy_data["postcode"],
            status="SUCCESS",
        )
    else:
        response = build_response_message(
            request=request_msg,
            policy_name=None,
            postcode=None,
            status="NOT_FOUND",
            error=f"No policy found for {request_msg['policy_number']}",
        )

    filename = f"{response['customer_id']}_{response['request_id']}.json"
    filepath = os.path.join(RESPONSE_OUTPUT_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(response, fh, indent=4, ensure_ascii=False)
    log.info("Simulated response written → %s", filepath)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["Ops"])
async def health():
    """Liveness probe."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post(
    "/policy/lookup",
    response_model=AckResponse,
    status_code=202,
    tags=["Policy"],
    summary="Submit a single policy lookup request",
)
async def policy_lookup(req: PolicyRequest, background_tasks: BackgroundTasks):
    """
    Accepts a policy lookup request, publishes it to Kafka, and returns
    an immediate **202 Accepted** acknowledgement.

    The enriched response (PolicyName + Postcode) is written asynchronously
    to `responses/<customer_id>_<message_id>.json`.
    """
    message = build_request_message(
        customer_id=req.customer_id,
        name=req.name,
        policy_number=req.policy_number,
    )

    # Try Kafka; fall back to direct simulation if broker unavailable
    try:
        await publish_to_kafka([message])
    except Exception:
        log.warning("Kafka unavailable — running in simulation mode.")
        background_tasks.add_task(_simulate_response, message)

    return AckResponse(
        message_id=message["message_id"],
        customer_id=req.customer_id,
        policy_number=req.policy_number,
        status="ACCEPTED",
        message="Request accepted. Response will be written to responses/ folder.",
        timestamp=message["timestamp"],
    )


@app.post(
    "/policy/lookup/batch",
    response_model=BatchAckResponse,
    status_code=202,
    tags=["Policy"],
    summary="Submit multiple policy lookup requests",
)
async def policy_lookup_batch(batch: PolicyRequestBatch, background_tasks: BackgroundTasks):
    """
    Accepts a batch of policy lookup requests and publishes all to Kafka
    in one shot.
    """
    if not batch.requests:
        raise HTTPException(status_code=400, detail="requests list cannot be empty.")

    messages = [
        build_request_message(r.customer_id, r.name, r.policy_number)
        for r in batch.requests
    ]

    try:
        await publish_to_kafka(messages)
    except Exception:
        log.warning("Kafka unavailable — running in simulation mode.")
        for msg in messages:
            background_tasks.add_task(_simulate_response, msg)

    acks = [
        AckResponse(
            message_id=msg["message_id"],
            customer_id=msg["customer_id"],
            policy_number=msg["policy_number"],
            status="ACCEPTED",
            message="Request accepted.",
            timestamp=msg["timestamp"],
        )
        for msg in messages
    ]

    return BatchAckResponse(accepted=len(acks), messages=acks)


@app.get(
    "/policy/status/{message_id}",
    tags=["Policy"],
    summary="Check if a response file has been written",
)
async def policy_status(message_id: str, customer_id: Optional[str] = None):
    """
    Polls the responses/ folder for a file matching `*_<message_id>.json`.

    Returns the full response payload if found, or a PENDING status if not yet written.
    """
    pattern = f"_{message_id}.json"
    matches = [f for f in os.listdir(RESPONSE_OUTPUT_DIR) if f.endswith(pattern)]

    if not matches:
        return JSONResponse(
            status_code=202,
            content={"message_id": message_id, "status": "PENDING", "detail": "Response not yet available."},
        )

    filepath = os.path.join(RESPONSE_OUTPUT_DIR, matches[0])
    with open(filepath, encoding="utf-8") as fh:
        data = json.load(fh)

    return JSONResponse(status_code=200, content=data)


@app.get(
    "/policy/responses",
    tags=["Policy"],
    summary="List all response files in the responses/ folder",
)
async def list_responses():
    """Returns a list of all response files written so far."""
    files = sorted(
        f for f in os.listdir(RESPONSE_OUTPUT_DIR) if f.endswith(".json")
    )
    return {"total": len(files), "files": files}
