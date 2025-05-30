import json

from loguru import logger
from consumer.clickhouse import insert_event
from kafka import KafkaConsumer


def run_consumer():
    consumer = KafkaConsumer(
        'messages',
        bootstrap_servers=['158.160.23.254:9094'],
        auto_offset_reset='earliest',
        group_id='echo-messages-to-stdout',
    )

    try:
        logger.info("Kafka consumer started, waiting for messages...")
        for message in consumer:
            # logger.info(message.value)
            # logger.info(json.loads(message.value))
            event = {
                "user_id": "12",
                "type": "sdfasdf",
                "page": "12",
                "element_id": "asdf",
                "metadata": "asdf",
                "duration_seconds": 12,
            }
            insert_event(event)
        # while True:
        #     msg = consumer.poll(1.0)
        #     if msg is None:
        #         continue
        #     if msg.error():
        #         raise KafkaException(msg.error())
        #
        #     event = json.loads(msg.value().decode("utf-8"))
        #     logger.info(f"Received event: {event}")
        #     insert_event(event)

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
