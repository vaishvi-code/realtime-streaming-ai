"""
Reddit Public JSON → Kafka Producer
=====================================
Uses Reddit's public .json endpoint (no API key, no OAuth).
Fetches hot posts from configurable subreddits and publishes to Kafka.

Usage: python reddit_producer.py
"""

import json
import time
import logging
import hashlib
import requests

from datetime import datetime, timezone
from kafka import KafkaProducer

# ─── Config ──────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_RAW       = "raw-posts"
TOPIC_DLQ       = "dead-letter-queue"
POLL_INTERVAL   = 60          # Reddit rate limit: be gentle
SUBREDDITS      = [
    "technology", "datascience", "MachineLearning",
    "programming", "artificial", "dataengineering",
]
POSTS_PER_SUB   = 25

HEADERS = {"User-Agent": "StreamingPipeline/1.0 (portfolio project)"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("reddit-producer")


# ─── Producer ────────────────────────────────────────────────────────────────

def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
        retries=5,
    )


def fetch_subreddit(sub: str, limit: int = POSTS_PER_SUB) -> list[dict]:
    url  = f"https://www.reddit.com/r/{sub}/hot.json?limit={limit}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return [child["data"] for child in data["data"]["children"]]


def normalize(post: dict, subreddit: str) -> dict:
    return {
        "id":           post.get("id", ""),
        "source":       "reddit",
        "title":        post.get("title", ""),
        "text":         post.get("selftext", ""),
        "url":          post.get("url", ""),
        "score":        post.get("score", 0),
        "author":       post.get("author", "unknown"),
        "num_comments": post.get("num_comments", 0),
        "tags":         [subreddit, post.get("link_flair_text", "")],
        "ingested_at":  datetime.now(timezone.utc).isoformat(),
        "raw_ts":       post.get("created_utc", 0),
    }


def run():
    logger.info("Starting Reddit → Kafka producer (public API, no auth)")
    producer = build_producer()
    seen_ids = set()
    total_sent = 0

    while True:
        for sub in SUBREDDITS:
            try:
                posts = fetch_subreddit(sub)
                new_posts = [p for p in posts if p["id"] not in seen_ids]
                logger.info("r/%s: %d posts fetched, %d new", sub, len(posts), len(new_posts))

                for post in new_posts:
                    try:
                        msg = normalize(post, sub)
                        key = hashlib.md5(f"reddit:{msg['id']}".encode()).hexdigest()
                        producer.send(TOPIC_RAW, key=key, value=msg)
                        seen_ids.add(post["id"])
                        total_sent += 1
                    except Exception as e:
                        producer.send(TOPIC_DLQ, value={
                            "post_id":   post.get("id"),
                            "subreddit": sub,
                            "error":     str(e),
                            "failed_at": datetime.now(timezone.utc).isoformat(),
                        })

                time.sleep(2)   # between subreddits

            except requests.HTTPError as e:
                if e.response.status_code == 429:
                    logger.warning("Rate limited on r/%s — sleeping 120s", sub)
                    time.sleep(120)
                else:
                    logger.error("HTTP error on r/%s: %s", sub, e)

        producer.flush()
        logger.info("Cycle done. Total sent: %d. Sleeping %ds...", total_sent, POLL_INTERVAL)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
