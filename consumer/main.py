import json

from loguru import logger
from consumer.clickhouse import insert_event
from kafka import KafkaConsumer
from consumer.settings import settings


def run_consumer():
    servers = settings.KAFKA_BOOTSTRAP_SERVERS.split(',')
    consumer = KafkaConsumer(
        'messages',
        bootstrap_servers=servers,
        auto_offset_reset='earliest',
        group_id='echo-messages-to-stdout',
    )

    try:
        logger.info("Kafka consumer started, waiting for messages...")
        for message in consumer:
            logger.info(message.value)

            event = json.loads(message.value)
            insert_event(event)
            logger.info("занес данные")

    except Exception as e:
        logger.error(e)

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
