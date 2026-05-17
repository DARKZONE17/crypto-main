import os
import json
import time
import logging
import boto3
import pandas as pd

from confluent_kafka import Consumer
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_TOPIC    = "test_crypto-topic"
BUCKET_NAME    = "amzn-crypto-s3-storage-bucket"
BATCH_SIZE     = 1_000
FLUSH_INTERVAL = 30          # seconds
OUTPUT_DIR     = "./crypto_parquet"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── AWS client ─────────────────────────────────────────────────────────────────
s3_client = boto3.client(
    "s3",
    region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
)

# ── Kafka consumer ─────────────────────────────────────────────────────────────
consumer = Consumer({
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "group.id":          "crypto-consumer-group",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([KAFKA_TOPIC])


def write_parquet(records: list) -> None:
    """Serialize records to Parquet, upload to S3, then remove the local file."""
    if not records:
        return

    # Single timestamp snapshot — avoids midnight skew across multiple calls
    now       = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    local_file = os.path.join(OUTPUT_DIR, f"crypto_{timestamp}.parquet")

    df = pd.DataFrame(records)
    df.to_parquet(local_file, engine="pyarrow", index=False)
    logger.info("Local parquet created: %s (%d rows)", local_file, len(df))

    s3_key = (
        f"raw/crypto/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"crypto_{timestamp}.parquet"
    )

    try:
        s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
        logger.info("Uploaded to S3: s3://%s/%s", BUCKET_NAME, s3_key)
    except ClientError as exc:
        logger.error("S3 upload failed — local file kept for retry: %s", exc)
        return          # Keep the local file so data isn't lost
    finally:
        # Only remove after a confirmed upload
        if os.path.exists(local_file):
            try:
                os.remove(local_file)
            except OSError as exc:
                logger.warning("Could not remove local file %s: %s", local_file, exc)


# ── Main loop ──────────────────────────────────────────────────────────────────
def main() -> None:
    buffer: list          = []
    last_flush: float     = time.time()

    logger.info("Consumer started — topic: %s", KAFKA_TOPIC)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                # Still check time-based flush even when the queue is quiet
                pass
            elif msg.error():
                logger.error("Kafka error: %s", msg.error())
            else:
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    buffer.append(data)
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    logger.error("Malformed message skipped: %s", exc)

            now = time.time()
            if buffer and (
                len(buffer) >= BATCH_SIZE or
                now - last_flush >= FLUSH_INTERVAL
            ):
                write_parquet(buffer)
                buffer.clear()
                last_flush = now

    except KeyboardInterrupt:
        logger.info("Interrupt received — shutting down...")
    finally:
        if buffer:
            logger.info("Flushing %d remaining records...", len(buffer))
            write_parquet(buffer)
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()