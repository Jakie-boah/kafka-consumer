from clickhouse_driver import Client
from settings import settings
import json

client = Client(host=settings.CLICKHOUSE_HOST)


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
                event.get("timestamp"),
            )
        ]
    )
