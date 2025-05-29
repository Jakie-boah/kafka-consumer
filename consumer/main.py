import json
from confluent_kafka import Consumer, KafkaException
from settings import settings
from loguru import logger
from consumer.clickhouse import insert_event


def run_consumer():
    consumer_conf = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": settings.KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
    }

    if settings.KAFKA_SECURITY_PROTOCOL:
        consumer_conf["security.protocol"] = settings.KAFKA_SECURITY_PROTOCOL
    if settings.KAFKA_SASL_MECHANISMS:
        consumer_conf["sasl.mechanisms"] = settings.KAFKA_SASL_MECHANISMS
    if settings.KAFKA_SASL_USERNAME and settings.KAFKA_SASL_PASSWORD:
        consumer_conf["sasl.username"] = settings.KAFKA_SASL_USERNAME
        consumer_conf["sasl.password"] = settings.KAFKA_SASL_PASSWORD

    consumer = Consumer(consumer_conf)
    consumer.subscribe(["metrics"])

    try:
        logger.info("Kafka consumer started, waiting for messages...")
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            event = json.loads(msg.value().decode("utf-8"))
            logger.info(f"Received event: {event}")
            insert_event(event)

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
