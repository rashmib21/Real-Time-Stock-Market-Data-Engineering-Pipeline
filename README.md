Real-Time Stock Market Data Engineering Pipeline

A fault-tolerant, real-time streaming pipeline that ingests live NSE stock market data via Angel One SmartAPI, processes it through Apache Kafka, and serves four independent downstream consumers — dashboard, analytics, alerts, and persistent storage.

Problem statement-
Stock markets generate continuous, high-frequency tick data that must be processed without loss or duplication, distributed to multiple independent systems (dashboards, alerting, analytics, storage), and remain resilient to network failures, broker crashes, and bad/corrupt data — all while respecting real-world constraints like market trading hours.

This project implements that pipeline end-to-end using Apache Kafka as the central nervous system, decoupling data ingestion from data consumption so that a failure in one consumer (e.g. storage) never affects another (e.g. live dashboard).

