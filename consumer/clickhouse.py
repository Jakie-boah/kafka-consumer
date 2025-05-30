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
                user='user',
                password='password',
                database=settings.CLICKHOUSE_DB,
            )
            client_.execute('SELECT 1')
            return client_
        except Exception as e:
            logger.info(f"[{i + 1}/{MAX_RETRIES}] ClickHouse not ready, retrying... ({e})")
            time.sleep(3)
    raise RuntimeError("ClickHouse not available after retries")


client = connect_clickhouse()


def insert_event(event: dict):
    client.execute(
        f"""
        INSERT INTO {settings.CLICKHOUSE_DB}.{settings.CLICKHOUSE_TABLE}
        (user_id, event_type, page, element_id, metadata, duration_seconds, timestamp)
        VALUES
        """,
        [
            (
                event.get("user_id", ""),
                event.get("type", ""),
                event.get("page", ""),
                event.get("element_id", ""),
                json.dumps(event.get("metadata", {})),
                int(event.get("duration", 0)),
                event.get("timestamp") or datetime.now(),
            )
        ]
    )
