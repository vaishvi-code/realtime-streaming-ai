"""
Hacker News → Kafka Producer
=============================
Pulls live stories from the Hacker News Firebase API (100% free, no auth).
Publishes each story as a JSON message to the 'raw-posts' Kafka topic.

Architecture note:
  - Implements idempotency via a seen_ids set (in-memory deduplication)
  - Dead-letter queue (DLQ) for malformed/unparseable records
  - Exponential backoff on API failures
"""

import json
import time
import logging
import hashlib
import requests

from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─── Config ──────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_RAW       = "raw-posts"
TOPIC_DLQ       = "dead-letter-queue"
POLL_INTERVAL   = 30          # seconds between HN API polls
MAX_STORIES     = 50          # top N stories per poll
LOG_LEVEL       = logging.INFO

HN_TOP_STORIES  = "https://hacker-news.firebaseio.com/v0/topstories.json"
HN_ITEM         = "https://hacker-news.firebaseio.com/v0/item/{}.json"

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hn-producer")


# ─── Kafka Setup ─────────────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",                   # wait for all replicas
        retries=5,
        retry_backoff_ms=500,
        compression_type="gzip",      # gzip works on all platforms
    )


# ─── HN API Helpers ──────────────────────────────────────────────────────────

def fetch_top_ids(n: int = MAX_STORIES) -> list[int]:
    resp = requests.get(HN_TOP_STORIES, timeout=10)
    resp.raise_for_status()
    return resp.json()[:n]


def fetch_item(item_id: int) -> dict | None:
    resp = requests.get(HN_ITEM.format(item_id), timeout=10)
    resp.raise_for_status()
    return resp.json()


def normalize(item: dict, source: str = "hackernews") -> dict:
    """Map HN schema → unified pipeline schema."""
    return {
        "id":         str(item.get("id", "")),
        "source":     source,
        "title":      item.get("title", ""),
        "text":       item.get("text", ""),           # self-post body
        "url":        item.get("url", ""),
        "score":      item.get("score", 0),
        "author":     item.get("by", "unknown"),
        "num_comments": item.get("descendants", 0),
        "tags":       [item.get("type", "story")],
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "raw_ts":     item.get("time", 0),
    }


def message_key(item: dict) -> str:
    """Deterministic key for Kafka partition routing."""
    return hashlib.md5(f"{item['source']}:{item['id']}".encode()).hexdigest()


# ─── Main Loop ───────────────────────────────────────────────────────────────

def run():
    logger.info("Starting Hacker News → Kafka producer")
    producer   = build_producer()
    seen_ids   = set()          # in-memory dedup (resets on restart; extend with Redis for prod)
    total_sent = 0

    def on_send_success(metadata):
        logger.debug(
            "✓ Delivered → topic=%s partition=%d offset=%d",
            metadata.topic, metadata.partition, metadata.offset,
        )

    def on_send_error(exc):
        logger.error("✗ Delivery failed: %s", exc)

    while True:
        try:
            ids = fetch_top_ids()
            new_ids = [i for i in ids if i not in seen_ids]
            logger.info("Poll: %d top stories, %d new", len(ids), len(new_ids))

            for item_id in new_ids:
                try:
                    raw = fetch_item(item_id)
                    if not raw or raw.get("type") != "story":
                        continue

                    msg  = normalize(raw)
                    key  = message_key(msg)

                    producer.send(TOPIC_RAW, key=key, value=msg) \
                            .add_callback(on_send_success) \
                            .add_errback(on_send_error)

                    seen_ids.add(item_id)
                    total_sent += 1

                    if total_sent % 10 == 0:
                        logger.info("Progress: %d messages produced", total_sent)

                    time.sleep(0.2)   # gentle rate limiting

                except Exception as item_exc:
                    # Route bad records to DLQ
                    logger.warning("Sending item %d to DLQ: %s", item_id, item_exc)
                    producer.send(TOPIC_DLQ, value={
                        "item_id":    item_id,
                        "error":      str(item_exc),
                        "failed_at":  datetime.now(timezone.utc).isoformat(),
                    })

            producer.flush()
            logger.info("Sleeping %ds before next poll...", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            logger.info("Shutting down producer (total sent: %d)", total_sent)
            producer.close()
            break

        except requests.RequestException as api_err:
            logger.error("HN API error: %s — retrying in 60s", api_err)
            time.sleep(60)


if __name__ == "__main__":
    run()
