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


