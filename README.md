# 🇳🇵 Nepal Power Grid & Cross-Border Energy Trade — Data Warehouse

> An end-to-end data pipeline that digitizes NEA's daily NDOR (Nepal Daily Operational Report) PDFs into a queryable star-schema data warehouse with automated dashboards.

[![Python 3.9+](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL 15](https://img.shields.io/badge/PostgreSQL-15-336791.svg)](https://www.postgresql.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7+-FF694B.svg)](https://www.getdbt.com/)
[![Airflow 2.8](https://img.shields.io/badge/Airflow-2.8-017CEE.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## 📐 Architecture

```
requests + pdfplumber    Clean CSVs     SQL COPY (upsert)   dbt run        SQL queries
┌──────────────┐    ┌──────────────┐  ┌────────────────┐  ┌───────────┐  ┌──────────────┐
│  NEA Website │───>│   Python     │─>│    Bronze       │─>│ PostgreSQL│─>│   Metabase   │
│  (NDOR PDFs) │    │  Extractor   │  │   (CSVs)       │  │  (Docker) │  │  Dashboards  │
└──────────────┘    └──────────────┘  └────────────────┘  └───────────┘  └──────────────┘
                    extract.py         data/bronze/         raw → analytics  localhost:3000
                                                           ↑
                                                     Apache Airflow
                                                     (Docker, daily)
                                                     localhost:8080
```

### Data Flow

| Layer | What Happens | Tools |
|-------|-------------|-------|
| **Ingestion** | Download PDFs from NEA, parse tables with pdfplumber | `requests`, `pdfplumber` |
| **Bronze** | Raw extracted CSVs with BS↔AD dates, 33 columns | `pandas`, `nepali-datetime` |
| **Raw (PostgreSQL)** | Landing tables with JSONB for nested data, upsert for idempotency | `psycopg2`, Docker PostgreSQL |
| **Analytics (dbt)** | Star schema: staging → dimensions → facts | `dbt-core`, `dbt-postgres` |
| **Orchestration** | Daily scheduling, retries, dependency management | Apache Airflow (Docker) |
| **Presentation** | Interactive dashboards and SQL queries | Metabase |

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.9+** — `python --version`
- **Docker Desktop** — `docker --version`
- **Git** — `git --version`

### Setup (5 minutes)

```bash
# 1. Clone the repo
git clone <your-repo-url>
cd nea-data-warehouse

# 2. Configure environment
cp .env.example .env
# Edit .env and set POSTGRES_PASSWORD to a secure value

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Start Docker services (PostgreSQL + Metabase + Airflow)
docker-compose up -d

# 5. Extract sample PDFs to CSV
python -m extractor.extract

# 6. Load into PostgreSQL
python db/load.py

# 7. Run dbt transformations
cd dbt_nea && dbt run --profiles-dir . && dbt test --profiles-dir .

# 8. Open dashboards
# Metabase: http://localhost:3000
# Airflow:  http://localhost:8080 (admin/admin)
```

### One-Command Pipeline

```bash
# Run the full pipeline
make pipeline
```

---

## 📊 What Gets Extracted

Each NDOR PDF contains Nepal's daily power grid snapshot:

| Metric | Unit | Example |
|--------|------|---------|
| NEA Generation | MWh | 7,376 |
| IPP Generation | MWh | 15,231 |
| Total Import (India) | MWh | 7,438 |
| Total Export | MWh | 0 |
| Net Energy Met | MWh | 35,340 |
| Peak Demand | MW | 2,036 |
| Peak Time | HH:MM | 18:00 |
| System Loss | % | Computed |

### Derived Metrics (computed by dbt)

- **Season classification**: Monsoon 🌧️ / Dry ☀️ / Pre-monsoon 🌤️
- **Nepal Fiscal Year**: e.g., 2081/82 (starts mid-July)
- **Import dependency ratio**: % of energy from imports
- **NEA vs IPP generation share**: Government vs private
- **Surplus/deficit**: Generation vs demand gap

---

## 🏗️ Project Structure

```
nea-data-warehouse/
├── README.md                         # You are here
├── Makefile                          # make up, make extract, make pipeline
├── docker-compose.yml                # PostgreSQL + Metabase + Airflow
├── requirements.txt                  # Python dependencies
├── .env.example                      # Environment variables template
├── .github/workflows/ci.yml          # CI: pytest + dbt build
│
├── extractor/                        # 📥 PDF Extraction Engine
│   ├── extract.py                    # Core pdfplumber extraction logic
│   ├── download.py                   # Download PDFs from NEA website
│   ├── bs_calendar.py                # Bikram Sambat ↔ Gregorian converter
│   └── utils.py                      # Data cleaning helpers
│
├── data/                             # 💾 Local Data Lake
│   ├── *.pdf                         # Sample NDOR PDFs (committed)
│   ├── raw_pdfs/                     # Downloaded PDFs (gitignored)
│   └── bronze/                       # Extracted CSVs (gitignored)
│
├── db/                               # 🗄️ Database
│   ├── init.sql                      # Schema + table creation
│   └── load.py                       # CSV → PostgreSQL loader (upsert)
│
├── dbt_nea/                          # 📐 dbt Transformations
│   ├── models/
│   │   ├── staging/stg_nea_daily.sql # Cleaned + enriched staging
│   │   ├── dimensions/
│   │   │   └── dim_date.sql          # BS/AD calendar dimension
│   │   └── facts/
│   │       └── fact_daily_generation.sql  # Core fact table
│   ├── tests/
│   │   └── energy_balance_check.sql  # Data quality test
│   └── profiles.yml                  # DB connection (uses env vars)
│
├── airflow/                          # ⏰ Orchestration
│   └── dags/nea_pipeline_dag.py      # Daily pipeline DAG
│
└── tests/                            # ✅ Test Suite
    └── test_extract.py               # pytest extraction tests
```

---

## 🔬 Data Model (Star Schema)

```
                    ┌─────────────────┐
                    │   dim_date      │
                    │─────────────────│
                    │ date_key (PK)   │
                    │ bs_year, month  │
                    │ bs_month_name   │
                    │ season          │
                    │ fiscal_year     │
                    │ is_weekend      │
                    └────────┬────────┘
                             │
                      ┌──────┴──────────────────┐
                      │  fact_daily_generation   │
                      │─────────────────────────│
                      │ report_date_ad (PK/FK)  │
                      │ total_generation_mwh    │
                      │ nea_generation_mwh      │
                      │ ipp_generation_mwh      │
                      │ total_import_mwh        │
                      │ total_export_mwh        │
                      │ peak_demand_mw          │
                      │ net_energy_met_mwh      │
                      │ import_dependency_pct   │
                      │ surplus_deficit_mwh     │
                      │ season, fiscal_year     │
                      └─────────────────────────┘
```

---

## 🧪 Testing

```bash
# Run pytest extraction tests
python -m pytest tests/test_extract.py -v

# Run dbt tests (after loading data)
cd dbt_nea && dbt test --profiles-dir .
```

### What's tested:
- ✅ Numeric cleaning (commas, parentheses, dashes, None)
- ✅ BS↔AD date conversion roundtrip
- ✅ Season/fiscal year classification
- ✅ PDF extraction against known values (5 sample PDFs)
- ✅ Batch extraction CSV output
- ✅ Energy balance validation (dbt)
- ✅ Data quality (not_null, unique, accepted_values)
- ✅ Error-path handling (corrupt PDFs, missing tables)

---

## 🛠️ Technology Stack

| Tool | Purpose | Why This One |
|------|---------|-------------|
| `pdfplumber` | PDF table extraction | Best for tables, pure Python (no Java/Ghostscript) |
| `nepali-datetime` | BS↔AD conversion | Only reliable BS converter for Python |
| PostgreSQL 15 | Data warehouse | Industry standard, dbt-compatible, free |
| `dbt-core` | Data transformation | Industry standard, testing + lineage built-in |
| Apache Airflow | Orchestration | Industry standard DAG scheduler, Docker-native |
| Metabase | BI Dashboards | Free, Docker-ready, SQL-based |
| Docker Compose | Infrastructure | One-command setup, full stack in containers |

---

## 📈 Sample Dashboard Queries

```sql
-- Daily generation trend
SELECT report_date_ad, total_generation_mwh, total_import_mwh
FROM analytics.fact_daily_generation
ORDER BY report_date_ad;

-- Season comparison
SELECT season, 
       AVG(total_generation_mwh) as avg_gen,
       AVG(total_import_mwh) as avg_import,
       AVG(import_dependency_pct) as avg_import_dep
FROM analytics.fact_daily_generation
GROUP BY season;

-- NEA vs IPP market share
SELECT report_date_ad,
       nea_generation_pct,
       ipp_generation_pct
FROM analytics.fact_daily_generation
ORDER BY report_date_ad;
```

---

## 🔒 Security

- **Credentials**: All secrets managed via `.env` file (gitignored)
- **dbt**: Uses `env_var()` — no plaintext passwords in committed files
- **Docker**: Environment variables injected from `.env`
- **CI/CD**: Secrets managed via GitHub Secrets

---

## 👤 Author

**Aayush Paudel** 
B.Sc. CSIT | Data Engineering Portfolio Project

---

## 📝 License

This project is for educational/portfolio purposes. NEA data is publicly available.
