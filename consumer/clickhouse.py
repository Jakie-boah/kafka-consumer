from clickhouse_driver import Client
from settings import settings
import json
from loguru import logger
import time
from datetime import datetime

MAX_RETRIES = 10


def connect_clickhouse():
    for i in range(MAX_RETRIES):
        try:
            client_ = Client(
                host=settings.CLICKHOUSE_HOST,
                user="user",
                password="password",
                database=settings.CLICKHOUSE_DB,
            )
            client_.execute("SELECT 1")
            return client_
        except Exception as e:
            logger.info(
                f"[{i + 1}/{MAX_RETRIES}] ClickHouse not ready, retrying... ({e})"
            )
            time.sleep(3)
    raise RuntimeError("ClickHouse not available after retries")


client = connect_clickhouse()


def insert_events_batch(events: list[dict]):
    if not events:
        return

    data = [
        (
            e.get("user_id", ""),
            e.get("type", ""),
            e.get("page", ""),
            e.get("element_id", ""),
            json.dumps(e.get("metadata", {})),
            int(e.get("duration", 0)),
            e.get("timestamp") or datetime.now(),
        )
        for e in events
    ]

    try:
        client.execute(
            f"""
            INSERT INTO {settings.CLICKHOUSE_DB}.{settings.CLICKHOUSE_TABLE}
            (user_id, event_type, page, element_id, metadata, duration_seconds, timestamp)
            VALUES
            """,
            data,
        )
        logger.info(f"Inserted {len(events)} events into ClickHouse.")
    except Exception as e:
        logger.error(f"Failed to insert batch: {e}")
