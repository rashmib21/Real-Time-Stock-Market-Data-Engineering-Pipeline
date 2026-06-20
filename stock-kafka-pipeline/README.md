# 📈 Real-Time Stock Market Data Engineering Pipeline

> A fault-tolerant streaming pipeline that ingests live NSE tick data via Angel One's WebSocket API, processes it through Apache Kafka, and serves four independent downstream consumers — dashboard, analytics, alerts, and MySQL storage.

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-2.13--3.6.0-black.svg)](https://kafka.apache.org/)
[![MySQL](https://img.shields.io/badge/MySQL-Database-orange.svg)](https://www.mysql.com/)

---

## 📌 Project Overview

This pipeline streams live equity tick data (Open, High, Low, Close, Volume) from **Angel One's SmartAPI WebSocket**, publishes it to **Apache Kafka**, and fans it out to four independently operating consumers.

The core engineering goal was **decoupling**: every downstream service reads the same stream without coordinating with the producer or with each other, so a failure in one — a slow database write, a dropped connection — never blocks or degrades the rest.

---

## 🚀 Key Facts

- ⚡ **Live WebSocket ingestion** — not polling — ticks arrive the instant they happen, no rate-limit risk
- 🧩 **3 Kafka partitions**, 4 independent consumer groups, zero coordination between them
- 🛡️ **`acks='all'`** on the producer — a write only counts once every broker replica confirms it
- ✅ **Manual offset commits** — a message is only marked "done" after its database write actually succeeds
- 🕒 **Market-hours aware** — storage consumer knows NSE trading hours and avoids duplicate post-close writes
- 📝 **Structured logging** to `pipeline.log` for every consumer (INFO / WARNING / ERROR)

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Streaming platform** | Apache Kafka 2.13-3.6.0, ZooKeeper |
| **Data source** | Angel One SmartAPI (SmartWebSocketV2) |
| **Language** | Python 3 |
| **Kafka client** | kafka-python |
| **Database** | MySQL |
| **Config management** | python-dotenv (`.env` + `config.py`) |
| **Logging** | Python `logging` module, file-based |

---

## 🏗️ Architecture

<p align="center">
  <img src="assets/architecture-overview.png" alt="Pipeline architecture overview" width="650">
</p>

Each consumer runs under its own Kafka consumer group ID. Kafka delivers the **full** stream to every group independently — that's what lets dashboard, analytics, alerts, and storage operate without any awareness of one another.

<details>
<summary><b>🔎 See the detailed data flow diagram</b></summary>
<br>
<p align="center">
  <img src="assets/architecture-detailed.png" alt="Detailed pipeline data flow" width="650">
</p>

> The diagrams illustrate the conceptual flow. In the actual implementation, the Kafka topic is `stock-ohlcv`, the alert threshold is configurable via `.env` (`ALERT_PRICE`), and the moving-average window is configurable (default 5-period SMA via `collections.deque`).
</details>

---

## 📁 Components

| Stage | File | Responsibility |
|---|---|---|
| Ingestion | `producer_angelone.py` | Maintains the Angel One WebSocket connection, serializes ticks to JSON, publishes to Kafka with `acks='all'` |
| Broker | Kafka — `stock-ohlcv` topic | 3 partitions; decouples the producer from every consumer |
| Dashboard | `consumer_dashboard.py` | Live terminal price feed, color-coded by tick direction |
| Analytics | `consumer_analytics.py` | Rolling N-period simple moving average |
| Alerts | `consumer_alerts.py` | Edge-triggered threshold alerts — fires once on crossing, not on every tick |
| Storage | `consumer_storage.py` | Persists to MySQL; market-hours aware, skips redundant writes after close |

---

## 🧠 Engineering Decisions

| Decision | Why |
|---|---|
| **Kafka over direct API → DB writes** | Four services read the same stream independently. A slow or crashed consumer never blocks the others. |
| **WebSocket over REST polling** | Polling means repeatedly asking "anything new?" — hits rate limits and leaves gaps. WebSocket pushes ticks the instant they happen. |
| **`acks='all'` on the producer** | A message only counts as sent once every broker replica has it — prevents silent data loss if a broker dies right after a write. |
| **Manual offset commits** | Auto-commit marks a message "done" the moment it's received — even if the database write after it fails. Manual commit only advances the offset once the write actually succeeds. |
| **Separate consumer groups** | One shared group across all four services would split the stream between them, each missing data. Independent group IDs guarantee every service sees everything. |

---

## 🔍 Data Schema

Full Kafka message schema, MySQL table definition, and consumer group reference: [`SCHEMA.md`](./SCHEMA.md)

<details>
<summary><b>Quick preview — Kafka message format</b></summary>

```json
{
  "symbol": "HINDCOPPER",
  "ltp": 513.95,
  "open": 513.00,
  "high": 522.50,
  "low": 507.75,
  "close": 511.75,
  "volume": 3581828,
  "timestamp": "2026-06-19 14:32:10"
}
```
</details>

---

## 📂 Project Structure

```
stock-kafka-pipeline/
├── .env                      credentials, not committed
├── .gitignore
├── config.py                 loads .env, exposes settings
├── market_hours.py           NSE trading-hours check
├── logger_config.py          shared file logger
├── producer_angelone.py      Angel One WebSocket -> Kafka
├── consumer_dashboard.py     live terminal price feed
├── consumer_analytics.py     rolling moving average
├── consumer_alerts.py        price threshold alerts
├── consumer_storage.py       MySQL persistence, market-hours gated
├── data/
│   └── hindustan_copper.csv
├── sql/
│   └── create_tables.sql
├── assets/                   diagrams referenced in this README
├── SCHEMA.md
└── README.md
```

---

## ▶️ How to Run

<details>
<summary><b>Prerequisites</b></summary>

- Java 11+ (required by Kafka)
- Apache Kafka 2.13-3.6.0
- Python 3.10+
- MySQL Server
</details>

**1. Install dependencies**
```bash
pip install kafka-python mysql-connector-python pandas smartapi-python pyotp python-dotenv
```

**2. Configure credentials**
Create `.env` in the project root with your Angel One API credentials, Kafka broker address, and MySQL connection details.

**3. Start Kafka and ZooKeeper**
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties      # terminal 1
bin/kafka-server-start.sh config/server.properties              # terminal 2

# one-time topic creation
bin/kafka-topics.sh --create --topic stock-ohlcv \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

**4. Initialize the database**
```bash
mysql -u root -p < sql/create_tables.sql
```

**5. Run the consumers** (each in its own terminal)
```bash
python consumer_dashboard.py
python consumer_analytics.py
python consumer_alerts.py
python consumer_storage.py
```

**6. Run the producer**
```bash
# requires live NSE market hours: 9:15 AM-3:30 PM IST, Mon-Fri
python producer_angelone.py
```

---

## ⚠️ Limitations

Listed here rather than left for someone else to find.

- True exactly-once semantics would require Kafka's Transactions API (`init_transactions`, `send_offsets_to_transaction`). The current implementation — idempotent production plus manual-commit consumption — prevents duplicates and silent data loss, but isn't a formal transactional guarantee.
- No data validation layer yet; malformed events (negative prices, logically inconsistent OHLC values) aren't filtered before reaching MySQL.
- No dead-letter queue for messages that fail processing repeatedly.
- Single-broker, single-instance Kafka setup; no multi-broker replication tested.
- Alerts are console-based only; no email or SMS delivery integrated.
- No offline replay mode; the pipeline currently runs only against live data during market hours.

---

## 👩‍💻 About the Author

**Rashmi Barethiya**
MCA Graduate — DAVV Indore

Built as a hands-on Data Engineering project to work directly with Kafka's reliability and consistency guarantees rather than studying them in the abstract.

- 🔗 [LinkedIn](https://www.linkedin.com/in/rashmi-barethiya-2823b1227)
- 💻 [GitHub](https://github.com/rashmib21)

---

⭐ If you found this project useful, consider giving it a star!
