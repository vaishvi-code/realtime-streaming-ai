"""
Kafka Consumer - Pure Python
Flushes every record immediately so the dashboard updates in real time.
"""

import os
import json
import logging
import time
import pandas as pd

from datetime import datetime, timezone
from pathlib import Path
from kafka import KafkaConsumer

from sentiment_enricher import SentimentEnricher

KAFKA_BROKER  = "localhost:9092"
TOPIC_RAW     = "raw-posts"
GROUP_ID      = "sentiment-pipeline-v2"   # new group = reads from beginning

DATA_DIR    = Path(__file__).parent.parent / "data"
BRONZE_PATH = DATA_DIR / "bronze" / "posts"
SILVER_PATH = DATA_DIR / "silver" / "posts"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("py-consumer")
enricher = SentimentEnricher()


def write_parquet(records, path):
    df = pd.DataFrame(records)
    for col in ["score", "num_comments"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    for col in ["sentiment_compound", "vader_pos", "vader_neg", "vader_neu",
                "textblob_polarity", "textblob_subjectivity", "sentiment_confidence"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path / f"batch_{int(time.time()*1000)}.parquet", index=False)


def process(record):
    """Enrich one record and write immediately."""
    try:
        e = enricher.analyse_record(dict(record))
        e["processed_at"]   = datetime.now(timezone.utc).isoformat()
        e["date_partition"] = datetime.now().strftime("%Y-%m-%d")
        source    = e.get("source", "unknown")
        date_str  = e["date_partition"]
        silver_path = SILVER_PATH / f"source={source}" / f"date_partition={date_str}"
        bronze_path = BRONZE_PATH / f"date={date_str}"
        write_parquet([record], bronze_path)
        write_parquet([e], silver_path)
        return True
    except Exception as ex:
        logger.warning("Failed to process record %s: %s", record.get("id"), ex)
        return False


def run():
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)
    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    logger.info("Starting consumer (group: %s) - reads ALL messages from start", GROUP_ID)

    consumer = KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",   # start from beginning with new group id
        enable_auto_commit=True,
        max_poll_records=50,
    )

    logger.info("Connected. Listening on: %s", TOPIC_RAW)
    total = 0

    try:
        while True:
            batch = consumer.poll(timeout_ms=5000)
            count = 0
            for tp, messages in batch.items():
                for msg in messages:
                    if msg.value and msg.value.get("id"):
                        if process(msg.value):
                            count += 1
                            total += 1
            if count:
                logger.info("Wrote %d records to Silver (total: %d)", count, total)
            else:
                logger.info("Polling... total so far: %d", total)
    except KeyboardInterrupt:
        consumer.close()
        logger.info("Stopped. Total: %d", total)


if __name__ == "__main__":
    run()
