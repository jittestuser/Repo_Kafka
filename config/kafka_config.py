# ─────────────────────────────────────────────
#  Kafka Configuration
# ─────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]

# Topics
TOPIC_POLICY_REQUEST  = "policy.request"
TOPIC_POLICY_RESPONSE = "policy.response"

# Consumer group
CONSUMER_GROUP_ID = "policy-lookup-group"

# Producer / Consumer settings
PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "value_serializer": None,          # handled manually (JSON bytes)
    "key_serializer":   None,
    "acks":             "all",
    "retries":          3,
}

CONSUMER_CONFIG = {
    "bootstrap_servers":   KAFKA_BOOTSTRAP_SERVERS,
    "group_id":            CONSUMER_GROUP_ID,
    "value_deserializer":  None,       # handled manually
    "auto_offset_reset":   "earliest",
    "enable_auto_commit":  True,
}

# Output folder where response JSON files are written
RESPONSE_OUTPUT_DIR = "responses"
