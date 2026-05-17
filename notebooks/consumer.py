from confluent_kafka import Consumer
import pandas as pd
import json
import time
import os
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "test_crypto-topic"

# Local parquet landing folder
OUTPUT_DIR = "./crypto_parquet"

os.makedirs(OUTPUT_DIR, exist_ok=True)

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'crypto-consumer-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([KAFKA_TOPIC])

buffer = []

BATCH_SIZE = 1000
FLUSH_INTERVAL = 30   # seconds

last_flush_time = time.time()

def write_parquet(records):
    if not records:
        return

    df = pd.DataFrame(records)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    file_path = f"{OUTPUT_DIR}/crypto_{timestamp}.parquet"

    df.to_parquet(
        file_path,
        engine='pyarrow',
        index=False
    )

    logger.info(f"Parquet written: {file_path}")
    logger.info(f"Rows written: {len(df)}")


try:
    logger.info("Consumer Started...")

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            logger.error(msg.error())
            continue

        try:
            value = msg.value().decode("utf-8")

            data = json.loads(value)

            buffer.append(data)

            current_time = time.time()

            # Flush by size OR time
            if (
                len(buffer) >= BATCH_SIZE or
                current_time - last_flush_time >= FLUSH_INTERVAL
            ):

                write_parquet(buffer)

                buffer.clear()

                last_flush_time = current_time

        except Exception as e:
            logger.error(f"Error processing message: {e}")

except KeyboardInterrupt:
    logger.info("Stopping consumer...")

finally:

    # Final flush before shutdown
    if buffer:
        write_parquet(buffer)

    consumer.close()