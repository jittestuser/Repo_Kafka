"""
Message Schema & Serialization Utilities
-----------------------------------------
Defines the request / response envelope and JSON helpers.
"""

import json
import uuid
from datetime import datetime, timezone


# ── Schema helpers ──────────────────────────────────────────────────────────

def build_request_message(customer_id: str, name: str, policy_number: str) -> dict:
    """Create a standardised policy-request message."""
    return {
        "message_id":     str(uuid.uuid4()),
        "timestamp":      _now_iso(),
        "customer_id":    customer_id,
        "name":           name,
        "policy_number":  policy_number,
    }


def build_response_message(
    request: dict,
    policy_name: str | None,
    postcode:    str | None,
    status:      str = "SUCCESS",
    error:       str | None = None,
) -> dict:
    """Wrap a lookup result in a standardised response envelope."""
    return {
        "message_id":    str(uuid.uuid4()),
        "request_id":    request.get("message_id"),
        "timestamp":     _now_iso(),
        "customer_id":   request.get("customer_id"),
        "name":          request.get("name"),
        "policy_number": request.get("policy_number"),
        "policy_name":   policy_name,
        "postcode":      postcode,
        "status":        status,          # SUCCESS | NOT_FOUND | ERROR
        "error":         error,
    }


# ── Serialization ────────────────────────────────────────────────────────────

def serialize(payload: dict) -> bytes:
    """Serialize a dict to UTF-8 JSON bytes for Kafka."""
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def deserialize(raw: bytes) -> dict:
    """Deserialize UTF-8 JSON bytes from Kafka to a dict."""
    return json.loads(raw.decode("utf-8"))


# ── Internal ─────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
