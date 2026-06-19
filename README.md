Real-Time Stock Market Data Engineering Pipeline

A fault-tolerant, real-time streaming pipeline that ingests live NSE stock market data via Angel One SmartAPI, processes it through Apache Kafka, and serves four independent downstream consumers — dashboard, analytics, alerts, and persistent storage.

Problem statement-
Stock markets generate continuous, high-frequency tick data that must be processed without loss or duplication, distributed to multiple independent systems (dashboards, alerting, analytics, storage), and remain resilient to network failures, broker crashes, and bad/corrupt data — all while respecting real-world constraints like market trading hours.

This project implements that pipeline end-to-end using Apache Kafka as the central nervous system, decoupling data ingestion from data consumption so that a failure in one consumer (e.g. storage) never affects another (e.g. live dashboard).


Architecture

Angel One SmartAPI (WebSocket)  /  CSV (offline testing)
                |
                v
        producer_angelone.py  (acks=all, idempotent)
                |
                v
        Kafka Broker — topic: stock-ohlcv (3 partitions)
                |
   -------------------------------------------
   |          |             |               |
   v          v             v               v
dashboard  analytics     alerts          storage
consumer   consumer      consumer        consumer
(live      (5-period     (price          (MySQL,
 prices)    moving avg)   threshold)      market-hours
                                           gated, data
                                           quality checked)

Each consumer belongs to its own Kafka consumer group, so all four read every message independently — one consumer crashing or lagging never blocks or affects the others.

Tech stack

Streaming platform: Apache Kafka 2.13-3.6.0, ZooKeeper
Data source: Angel One SmartAPI (SmartWebSocketV2)
Language: Python 3
Kafka client: kafka-python
Database: MySQL
Config management: python-dotenv (.env + config.py)
Logging: Python logging module, file-based



Key engineering decisions

Why Kafka over direct API-to-DB writes?
Decouples ingestion from processing. Four consumers can read the same stream independently without coordinating with each other or with the producer, and a slow/crashed consumer never blocks data flow to the others.


Why WebSocket over REST polling for Angel One?
REST polling means actively asking "any new data?" on an interval — this hits rate limits and creates gaps between polls. WebSocket is a persistent push connection: Angel One sends ticks the instant they happen, with no polling delay and no rate-limit risk.

Why acks='all' on the producer?
Guarantees a message is acknowledged only after all broker replicas have stored it, preventing silent data loss if a single broker fails right after a write.

Why manual offset commits instead of auto-commit?
Auto-commit marks a message as "processed" the moment it's received — even if the downstream write (e.g. to MySQL) fails afterward. Manual commit only advances the offset after the database write succeeds, so a crash mid-processing results in safe re-delivery instead of silent data loss.


Why separate consumer groups per service?
Kafka delivers every message to every distinct consumer group. Using one shared group across dashboard/analytics/alerts/storage would split messages between them (each missing data); separate group IDs ensure each service gets the complete stream.



Reliability and data quality features


Producer: acks='all' for guaranteed broker-side durability; automatic reconnect-with-retry loop on WebSocket disconnects
Consumers: manual offset commits — Kafka offset only advances after successful downstream processing
Data validation: incoming events are checked for missing fields, non-positive prices, negative volume, and logically impossible OHLC values (High < Low) before being persisted
Market-hours gating: the storage consumer checks NSE trading hours (Mon–Fri, 9:15 AM–3:30 PM IST) before writing to MySQL; outside trading hours it reports the day's last traded price instead of writing duplicate post-market ticks
Structured logging: all consumers log to pipeline.log with timestamps and severity levels (INFO/WARNING/ERROR) for post-hoc debugging


