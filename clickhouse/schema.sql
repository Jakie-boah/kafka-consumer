CREATE DATABASE IF NOT EXISTS metrics;

CREATE TABLE IF NOT EXISTS metrics.events (
    user_id String,
    event_type String,
    page String,
    element_id String,
    metadata String,
    duration_seconds Int32,
    timestamp DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (event_type, timestamp);
