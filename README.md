# Fraud Detection Pipeline — Kafka · Spark · XGBoost

End-to-end **real-time fraud detection system** processing financial transactions across 30 countries. A Kafka producer simulates a continuous transaction stream, Apache Spark handles structured streaming and enrichment, and XGBoost classifies each transaction as legitimate or fraudulent. Experiments are tracked with MLflow and results are visualized in a Power BI dashboard.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         TRANSACTION PIPELINE                        │
│                                                                     │
│  Transaction Simulator ──► Kafka ──► Spark Streaming ──► PostgreSQL│
│   (30 countries · €10–25k)   │         (enrichment)        │       │
│                              │                              ▼       │
│                              └──────────► XGBoost Classifier        │
│                                          (fraud / legit)            │
└─────────────────────────────────────────────────────────────────────┘
                                     │
                          ┌──────────┴──────────┐
                          ▼                     ▼
                     MLflow UI            Power BI Dashboard
                  (experiment tracking)   (bigdata.pbix)
```

---

## Pipeline Steps

| Step | File | Description |
|---|---|---|
| 0 | `0_init_db.py` | Initialize PostgreSQL schema |
| 1 | `1_producer.py` | Kafka producer — simulates transactions |
| 2 | `2_processor.py` | Spark Structured Streaming — consume & enrich |
| 3 | `3_fraude_detector.py` | XGBoost classifier — real-time fraud scoring |
| — | `train_model.py` | Offline model training with MLflow tracking |

---

## Features

- **1 Million Transactions** — Dataset covering 30 countries and varied transaction amounts
- **Kafka Streaming** — Continuous transaction simulation at 2 transactions/second
- **Spark Structured Streaming** — Schema-validated real-time processing
- **XGBoost Classifier** — High-performance gradient boosting fraud detection
- **MLflow Tracking** — Experiment logging (accuracy, F1 score, model artifacts)
- **Power BI Report** — Pre-built dashboard (`bigdata.pbix`) for stakeholder reporting
- **Dockerized Stack** — One-command startup with `docker-compose`

---

## Project Structure

```
ProjetBigData/
├── 0_init_db.py             # PostgreSQL table creation
├── 1_producer.py            # Kafka producer (30-country transaction simulator)
├── 2_processor.py           # Spark Structured Streaming consumer
├── 3_fraude_detector.py     # XGBoost real-time fraud classifier
├── train_model.py           # Offline XGBoost training + MLflow logging
├── docker-compose.yml       # Kafka + Zookeeper + PostgreSQL stack
├── donnees_fraude_1M.csv    # 1M transaction training dataset
├── fraude_model.json        # Trained XGBoost model
├── mlflow.db                # MLflow SQLite backend
└── bigdata.pbix             # Power BI dashboard
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka |
| Stream Processing | Apache Spark (Structured Streaming) |
| Machine Learning | XGBoost |
| Experiment Tracking | MLflow |
| Database | PostgreSQL |
| Data Processing | Pandas, PySpark |
| Visualization | Power BI |
| Infrastructure | Docker, Docker Compose |
| Language | Python 3.10+ |

---

## Dataset

`donnees_fraude_1M.csv` — 1,000,000 financial transactions with the following fields:

| Column | Description |
|---|---|
| `id` | Transaction ID |
| `montant` | Transaction amount (€10 – €25,000) |
| `pays` | Country of origin (30 countries) |
| `timestamp` | Transaction datetime |
| `is_fraud` | Binary label: 1 = fraud, 0 = legitimate |

Countries covered: France, Cameroun, USA, Maroc, Allemagne, Espagne, and 24 more.

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Apache Spark (local or cluster)

### 1. Start infrastructure

```bash
docker-compose up -d
```

Services started: Kafka (port 9092), Zookeeper (port 2181), PostgreSQL (port 5432).

### 2. Initialize the database

```bash
python 0_init_db.py
```

### 3. Train the fraud detection model

```bash
python train_model.py
```

Model saved to `fraude_model.json`. MLflow experiments tracked in `mlflow.db`.

### 4. Start the real-time pipeline

```bash
# Terminal 1 — Start Kafka producer
python 1_producer.py

# Terminal 2 — Start Spark processor
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.x.x 2_processor.py

# Terminal 3 — Start fraud detector
python 3_fraude_detector.py
```

### 5. View MLflow experiments

```bash
mlflow ui --backend-store-uri sqlite:///mlflow.db
```

Open [http://localhost:5000](http://localhost:5000) to compare experiment runs.

---

## Model Performance

The XGBoost model is trained with the following configuration:

```
n_estimators  : 100
max_depth     : 6
learning_rate : 0.1
tree_method   : hist
```

Metrics logged to MLflow: **Accuracy** and **F1 Score**.

---

## Database Schema

```sql
CREATE TABLE transactions_result (
    id        SERIAL PRIMARY KEY,
    tr_id     INTEGER,
    montant   FLOAT,
    pays      VARCHAR(50),
    is_fraud  INTEGER,          -- 0 or 1
    timestamp TIMESTAMP
);
```

---

## Kafka Topic

| Parameter | Value |
|---|---|
| Topic | `transactions_brutes` |
| Throughput | ~2 messages/second |
| Serialization | JSON |
| Bootstrap server | `localhost:9092` |
