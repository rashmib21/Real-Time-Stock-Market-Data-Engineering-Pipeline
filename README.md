📈 Real-Time Stock Market Data Engineering Pipeline

A fault-tolerant streaming pipeline that ingests live NSE tick data via Angel One's WebSocket API, processes it through Apache Kafka, and serves four independent downstream consumers — dashboard, analytics, alerts, and MySQL storage.

📌 Project Overview

This pipeline streams live equity tick data (Open, High, Low, Close, Volume) from Angel One's SmartAPI WebSocket, publishes it to Apache Kafka, and fans it out to four independently operating consumers.

The core engineering goal was decoupling: every downstream service reads the same stream without coordinating with the producer or with each other, so a failure in one — a slow database write, a dropped connection — never blocks or degrades the rest.

🚀 Key Facts
