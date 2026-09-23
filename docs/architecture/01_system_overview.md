# 01 — System Overview

> **Mục tiêu:** Cung cấp bức tranh toàn cảnh của hệ thống — từ nguồn dữ liệu cho đến lớp trình bày.

---

## Tech Stack

| Layer | Technology | Vai trò |
|---|---|---|
| **Source** | ClickHouse | OLAP database của hệ thống LapLapTech |
| **Ingestion** | Python (`clickhouse-connect`, `pandas`, `SQLAlchemy`) | Extract & Load |
| **Warehouse** | PostgreSQL (Supabase / Local) | Data Warehouse |
| **Transform** | dbt Core 1.12 | ELT transformation, testing, docs |
| **Orchestration** | GitHub Actions (cron) | Scheduling daily pipeline |
| **Visualization** | Streamlit + Plotly + AgGrid | Analytics Dashboard |

---

## End-to-End Data Flow

```mermaid
flowchart TD
    subgraph SOURCE["☁️ Source System"]
        CH[("ClickHouse\nlaplaptech DB")]
    end

    subgraph INGESTION["🐍 Python Ingestion Script"]
        direction TB
        FR["Full Refresh\n5 dimension tables"]
        INC["Incremental Load\n1 event table\n(Watermark-based)"]
    end

    subgraph WAREHOUSE["🗄️ PostgreSQL — Data Warehouse"]
        direction TB
        RAW["Schema: raw\n(6 tables — source copy)"]

        subgraph DBT["⚙️ dbt Core — Transform Layer"]
            direction TB
            BRZ["Bronze Layer\n6 models — SELECT *\n(Staging / Abstraction)"]
            SLV["Silver Layer\n11 models — Cleaned & Parsed\n(Type cast, JSON parse, Join dim)"]
            GLD["Gold Layer\n12 mart models\n(Aggregated, Business-ready)"]
        end
    end

    subgraph PRESENTATION["📊 Presentation Layer"]
        ST["Streamlit Dashboard\n4 tabs — Live analytics"]
    end

    subgraph ORCHESTRATION["⏱️ Orchestration"]
        GA["GitHub Actions\nCron: 08:15 & 16:45 daily"]
    end

    CH -->|"Extract via HTTP"| FR
    CH -->|"Extract delta rows"| INC
    FR -->|"DROP + INSERT"| RAW
    INC -->|"APPEND new rows"| RAW

    RAW --> BRZ
    BRZ --> SLV
    SLV --> GLD

    GLD -->|"SQL queries"| ST
    GA -->|"Trigger"| INGESTION
    GA -->|"dbt build"| DBT
```

---

## Deployment Architecture

```mermaid
flowchart LR
    subgraph LOCAL["💻 Local Dev"]
        DEV["VS Code\n+ dbt Power User\n+ Streamlit localhost"]
    end

    subgraph CLOUD["☁️ Cloud"]
        GH["GitHub\nlaplaptech_pipeline repo\n(develop / main branches)"]
        GA["GitHub Actions\nCI/CD + Cron jobs"]
        SB["Supabase\nPostgreSQL Cloud\nData Warehouse"]
        ST["Streamlit Community Cloud\n(Optional deploy)"]
    end

    DEV -->|"git push"| GH
    GH -->|"trigger workflow"| GA
    GA -->|"python ingestion + dbt build"| SB
    SB -->|"query"| ST
```

---

## Data Volume & Scale (hiện tại)

| Table | Type | Rows (approx) | Load Strategy |
|---|---|---|---|
| `brand` | Dimension | ~50 | Full Refresh |
| `cpu_model` | Dimension | ~200 | Full Refresh |
| `gpu_model` | Dimension | ~150 | Full Refresh |
| `laptop_model` | Dimension | ~500 | Full Refresh |
| `laptop_benchmark_result` | Dimension | ~300 | Full Refresh |
| `user_event_tracking` | **Fact** | **~760,000+** | **Incremental** |

---

## Pipeline Execution Timeline

```mermaid
sequenceDiagram
    participant GA as GitHub Actions
    participant PY as Python Script
    participant CH as ClickHouse
    participant PG as PostgreSQL (raw)
    participant DBT as dbt Core
    participant ST as Streamlit

    GA->>PY: Trigger (08:15 / 16:45)
    PY->>CH: Full refresh queries (5 tables)
    CH-->>PY: Return DataFrames
    PY->>PG: DROP + INSERT dimension tables

    PY->>CH: Incremental query (WHERE ts > max_ts)
    CH-->>PY: New event rows only
    PY->>PG: APPEND to user_event_tracking

    GA->>DBT: dbt build
    DBT->>PG: Transform raw → bronze → silver → gold
    DBT-->>GA: 47 models PASS, N tests PASS

    Note over ST: User opens dashboard
    ST->>PG: Query public_gold.mart_*
    PG-->>ST: Return aggregated data
```
