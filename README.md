# 🍎 Feed to Succeed: A School Nutrition & Student Outcomes Pipeline

## Overview

**Core Business Question:** Do stronger school nutrition programs correlate with better academic performance and healthier kids — and where does Tennessee stand?

Feed to Succeed is an end-to-end data engineering pipeline that ingests, transforms, and visualizes data from four federal data sources to explore the relationship between school meal program participation, student health outcomes, and academic performance across all 50 U.S. states.

---

## Pipeline Architecture

```
USDA FNS (S3) + CDC Chronic Disease Indicators + NCES NAEP + Census ACS
                              ↓
                  Polars Ingestion Scripts
                              ↓
                     Snowflake RAW Schema
                              ↓
                    Airflow DAGs (Scheduled)
                              ↓
              dbt staging → intermediate → marts
                              ↓
                     Metabase Dashboard
```

---

## Data Sources

| Source | Description | Update Frequency |
|--------|-------------|-----------------|
| USDA FNS | National School Lunch Program (NSLP) and School Breakfast Program (SBP) participation by state | Annually |
| CDC Chronic Disease Indicators | Youth obesity, physical activity, and dietary habits by state (YRBSS-sourced) | Every 2 years |
| NCES NAEP | 8th grade math and reading scores by state | Annually |
| Census ACS | State-level poverty rates and demographic breakdowns | Annually |

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python | Core programming language |
| Polars | Fast DataFrame library for data ingestion |
| UV | Python environment and dependency management |
| Snowflake | Cloud data warehouse |
| AWS S3 | Raw file storage for USDA Excel files |
| dbt | Data transformation and modeling |
| Airflow + Cosmos | Pipeline orchestration and scheduling |
| Docker | Containerized Airflow and Metabase environments |
| Metabase | Business intelligence dashboard |
| Loguru | Python logging |
| Ruff | Python linting |

---

## Project Structure

```
feed-to-succeed/
├── ingestion/
│   ├── census.py         # Census ACS API ingestion
│   ├── usda.py           # USDA NSLP/SBP S3 ingestion
│   ├── cdc.py            # CDC Chronic Disease Indicators API ingestion
│   └── nces.py           # NCES NAEP API ingestion
├── nutrition_pipeline/   # dbt project
│   ├── models/
│   │   ├── staging/      # Clean and rename raw tables
│   │   ├── intermediate/ # Join and calculate derived metrics
│   │   └── marts/        # Business-ready tables for dashboard
│   └── seeds/
│       └── state_crosswalk.csv  # State FIPS, abbreviations, and regions
├── dags/                 # Airflow DAGs
├── init-db/              # Postgres initialization scripts
├── Dockerfile            # Airflow container definition
├── docker-compose.yml    # Multi-service Docker configuration
└── pyproject.toml        # UV dependency management
```

---

## Snowflake Schema Design

### RAW Layer (Bronze)
| Table | Description |
|-------|-------------|
| `raw.census_acs` | Raw Census ACS poverty and demographic data |
| `raw.nslp_participation` | Raw NSLP participation by state |
| `raw.sbp_participation` | Raw SBP participation by state |
| `raw.cdc_data` | Raw CDC chronic disease indicators |
| `raw.nces_math` | Raw NCES 8th grade math scores |
| `raw.nces_reading` | Raw NCES 8th grade reading scores |

### Staging Layer (Silver)
| Model | Description |
|-------|-------------|
| `stg_census_acs` | Cleaned Census data with renamed columns |
| `stg_nslp_participation` | Cleaned NSLP participation data |
| `stg_sbp_participation` | Cleaned SBP participation data |
| `stg_cdc_data` | Filtered CDC health metrics |
| `stg_nces_math` | Cleaned math scores for all students |
| `stg_nces_reading` | Cleaned reading scores for all students |

### Intermediate Layer (Silver)
| Model | Description |
|-------|-------------|
| `int_state_demographics` | Poverty rates and demographic percentages by state |
| `int_state_nutrition` | Combined NSLP and SBP participation by state |
| `int_state_health` | Filtered obesity and physical activity metrics |
| `int_state_academic` | Combined math and reading scores by state and year |

### Marts Layer (Gold)
| Model | Description |
|-------|-------------|
| `mart_state_nutrition_overview` | State meal participation rates with demographic context |
| `mart_nutrition_vs_health` | Participation rates vs student obesity and activity |
| `mart_nutrition_vs_academics` | Participation rates vs academic performance |
| `mart_tennessee_spotlight` | Tennessee vs neighboring states comparison |

---

## Business Questions Answered

1. Which states have the highest school meal participation rates?
2. Do high-participation states show lower obesity rates?
3. Is there a correlation between free/reduced lunch access and academic performance?
4. How does Tennessee rank nationally across nutrition, health, and academic metrics?
5. How does Tennessee compare to neighboring states (KY, GA, AL, VA, NC, MS, AR)?

---

## Running Locally

### Prerequisites
- Python 3.12
- UV
- Docker Desktop
- AWS CLI (configured with SSO)
- Snowflake account

### Setup

**1. Clone the repository**
```bash
git clone https://github.com/your-username/feed-to-succeed.git
cd feed-to-succeed
```

**2. Install dependencies**
```bash
uv sync
```

**3. Configure environment variables**

Create a `.env` file in the project root:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=feed_to_succeed
SNOWFLAKE_SCHEMA=raw
SNOWFLAKE_WAREHOUSE=feed_to_succeed_wh
SNOWFLAKE_ROLE=ACCOUNTADMIN

AWS_PROFILE=your_aws_profile
AWS_REGION=your_region
S3_BUCKET=your_bucket
S3_NSLP_KEY=path/to/NSLP.xlsx
S3_SBP_KEY=path/to/SBP.xlsx

CENSUS_API_KEY=your_census_api_key
```

**4. Run ingestion scripts**
```bash
uv run ingestion/census.py
uv run ingestion/usda.py
uv run ingestion/cdc.py
uv run ingestion/nces.py
```

**5. Run dbt transformations**
```bash
cd nutrition_pipeline
uv run dbt seed
uv run dbt run
uv run dbt test
```

**6. Start Airflow and Metabase**
```bash
docker compose up --build
```

- Airflow UI: `http://localhost:8080`
- Metabase: `http://localhost:3000`

---

## Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `census_dag` | Monthly | Ingests Census ACS data |
| `usda_dag` | Monthly | Ingests USDA NSLP/SBP data from S3 |
| `cdc_dag` | Monthly | Ingests CDC chronic disease data |
| `nces_dag` | Monthly | Ingests NCES NAEP score data |
| `dbt_staging_dag` | Monthly | Runs dbt staging models |
| `dbt_intermediate_dag` | Monthly | Runs dbt intermediate models |
| `dbt_marts_dag` | Monthly | Runs dbt mart models |

---

## Notes

- USDA FNS data is downloaded manually and stored in AWS S3 due to CDN restrictions on programmatic downloads
- CDC data is sourced from the CDC Chronic Disease Indicators API, which uses YRBSS as its underlying data source
- All credentials are stored in `.env` and excluded from version control via `.gitignore`
- The state crosswalk seed file maps state FIPS codes, abbreviations, names, and Census regions

---

## Author

Kayla McKenzie
Data Engineering Bootcamp Capstone — 2026# 🍎 Feed to Succeed: A School Nutrition & Student Outcomes Pipeline

## Overview

**Core Business Question:** Do stronger school nutrition programs correlate with better academic performance and healthier kids — and where does Tennessee stand?

Feed to Succeed is an end-to-end data engineering pipeline that ingests, transforms, and visualizes data from four federal data sources to explore the relationship between school meal program participation, student health outcomes, and academic performance across all 50 U.S. states.

---

## Pipeline Architecture

```
USDA FNS (S3) + CDC Chronic Disease Indicators + NCES NAEP + Census ACS
                              ↓
                  Polars Ingestion Scripts
                              ↓
                     Snowflake RAW Schema
                              ↓
                    Airflow DAGs (Scheduled)
                              ↓
              dbt staging → intermediate → marts
                              ↓
                     Metabase Dashboard
```

---

## Data Sources

| Source | Description | Update Frequency |
|--------|-------------|-----------------|
| USDA FNS | National School Lunch Program (NSLP) and School Breakfast Program (SBP) participation by state | Annually |
| CDC Chronic Disease Indicators | Youth obesity, physical activity, and dietary habits by state (YRBSS-sourced) | Every 2 years |
| NCES NAEP | 8th grade math and reading scores by state | Annually |
| Census ACS | State-level poverty rates and demographic breakdowns | Annually |

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python | Core programming language |
| Polars | Fast DataFrame library for data ingestion |
| UV | Python environment and dependency management |
| Snowflake | Cloud data warehouse |
| AWS S3 | Raw file storage for USDA Excel files |
| dbt | Data transformation and modeling |
| Airflow + Cosmos | Pipeline orchestration and scheduling |
| Docker | Containerized Airflow and Metabase environments |
| Metabase | Business intelligence dashboard |
| Loguru | Python logging |
| Ruff | Python linting |

---

## Project Structure

```
feed-to-succeed/
├── ingestion/
│   ├── census.py         # Census ACS API ingestion
│   ├── usda.py           # USDA NSLP/SBP S3 ingestion
│   ├── cdc.py            # CDC Chronic Disease Indicators API ingestion
│   └── nces.py           # NCES NAEP API ingestion
├── nutrition_pipeline/   # dbt project
│   ├── models/
│   │   ├── staging/      # Clean and rename raw tables
│   │   ├── intermediate/ # Join and calculate derived metrics
│   │   └── marts/        # Business-ready tables for dashboard
│   └── seeds/
│       └── state_crosswalk.csv  # State FIPS, abbreviations, and regions
├── dags/                 # Airflow DAGs
├── init-db/              # Postgres initialization scripts
├── Dockerfile            # Airflow container definition
├── docker-compose.yml    # Multi-service Docker configuration
└── pyproject.toml        # UV dependency management
```

---

## Snowflake Schema Design

### RAW Layer (Bronze)
| Table | Description |
|-------|-------------|
| `raw.census_acs` | Raw Census ACS poverty and demographic data |
| `raw.nslp_participation` | Raw NSLP participation by state |
| `raw.sbp_participation` | Raw SBP participation by state |
| `raw.cdc_data` | Raw CDC chronic disease indicators |
| `raw.nces_math` | Raw NCES 8th grade math scores |
| `raw.nces_reading` | Raw NCES 8th grade reading scores |

### Staging Layer (Silver)
| Model | Description |
|-------|-------------|
| `stg_census_acs` | Cleaned Census data with renamed columns |
| `stg_nslp_participation` | Cleaned NSLP participation data |
| `stg_sbp_participation` | Cleaned SBP participation data |
| `stg_cdc_data` | Filtered CDC health metrics |
| `stg_nces_math` | Cleaned math scores for all students |
| `stg_nces_reading` | Cleaned reading scores for all students |

### Intermediate Layer (Silver)
| Model | Description |
|-------|-------------|
| `int_state_demographics` | Poverty rates and demographic percentages by state |
| `int_state_nutrition` | Combined NSLP and SBP participation by state |
| `int_state_health` | Filtered obesity and physical activity metrics |
| `int_state_academic` | Combined math and reading scores by state and year |

### Marts Layer (Gold)
| Model | Description |
|-------|-------------|
| `mart_state_nutrition_overview` | State meal participation rates with demographic context |
| `mart_nutrition_vs_health` | Participation rates vs student obesity and activity |
| `mart_nutrition_vs_academics` | Participation rates vs academic performance |
| `mart_tennessee_spotlight` | Tennessee vs neighboring states comparison |

---

## Business Questions Answered

1. Which states have the highest school meal participation rates?
2. Do high-participation states show lower obesity rates?
3. Is there a correlation between free/reduced lunch access and academic performance?
4. How does Tennessee rank nationally across nutrition, health, and academic metrics?
5. How does Tennessee compare to neighboring states (KY, GA, AL, VA, NC, MS, AR)?

---

## Running Locally

### Prerequisites
- Python 3.12
- UV
- Docker Desktop
- AWS CLI (configured with SSO)
- Snowflake account

### Setup

**1. Clone the repository**
```bash
git clone https://github.com/your-username/feed-to-succeed.git
cd feed-to-succeed
```

**2. Install dependencies**
```bash
uv sync
```

**3. Configure environment variables**

Create a `.env` file in the project root:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=feed_to_succeed
SNOWFLAKE_SCHEMA=raw
SNOWFLAKE_WAREHOUSE=feed_to_succeed_wh
SNOWFLAKE_ROLE=ACCOUNTADMIN

AWS_PROFILE=your_aws_profile
AWS_REGION=your_region
S3_BUCKET=your_bucket
S3_NSLP_KEY=path/to/NSLP.xlsx
S3_SBP_KEY=path/to/SBP.xlsx

CENSUS_API_KEY=your_census_api_key
```

**4. Run ingestion scripts**
```bash
uv run ingestion/census.py
uv run ingestion/usda.py
uv run ingestion/cdc.py
uv run ingestion/nces.py
```

**5. Run dbt transformations**
```bash
cd nutrition_pipeline
uv run dbt seed
uv run dbt run
uv run dbt test
```

**6. Start Airflow and Metabase**
```bash
docker compose up --build
```

- Airflow UI: `http://localhost:8080`
- Metabase: `http://localhost:3000`

---

## Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `census_dag` | Monthly | Ingests Census ACS data |
| `usda_dag` | Monthly | Ingests USDA NSLP/SBP data from S3 |
| `cdc_dag` | Monthly | Ingests CDC chronic disease data |
| `nces_dag` | Monthly | Ingests NCES NAEP score data |
| `dbt_staging_dag` | Monthly | Runs dbt staging models |
| `dbt_intermediate_dag` | Monthly | Runs dbt intermediate models |
| `dbt_marts_dag` | Monthly | Runs dbt mart models |

---

## Notes

- USDA FNS data is downloaded manually and stored in AWS S3 due to CDN restrictions on programmatic downloads
- CDC data is sourced from the CDC Chronic Disease Indicators API, which uses YRBSS as its underlying data source
- All credentials are stored in `.env` and excluded from version control via `.gitignore`
- The state crosswalk seed file maps state FIPS codes, abbreviations, names, and Census regions

---

## Author

Kayla McKenzie
Data Engineering Bootcamp Capstone — 2026