import json

from loguru import logger
from consumer.clickhouse import insert_events_batch
from kafka import KafkaConsumer
from consumer.settings import settings

BATCH_SIZE = 1000


def run_consumer():
    servers = settings.KAFKA_BOOTSTRAP_SERVERS.split(",")
    consumer = KafkaConsumer(
        "messages",
        bootstrap_servers=servers,
        auto_offset_reset="earliest",
        group_id="echo-messages-to-stdout",
        enable_auto_commit=False,
    )

    buffer = []

    try:
        logger.info("Kafka consumer started, waiting for messages...")
        for message in consumer:
            event = json.loads(message.value)
            buffer.append(event)

            should_flush = len(buffer) >= BATCH_SIZE

            if should_flush:
                try:
                    insert_events_batch(buffer)
                    consumer.commit()
                    buffer.clear()
                except Exception as e:
                    logger.error(f"Failed to insert batch or commit offsets: {e}")

    except Exception as e:
        logger.error(e)

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")

    finally:
        insert_events_batch(buffer)
        consumer.close()


if __name__ == "__main__":
    run_consumer()
