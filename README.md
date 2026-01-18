# Kara Solutions: Medical Telegram Data Platform

## Project Overview
This project aims to build a **data platform for Kara Solutions** to generate actionable insights about Ethiopian medical businesses by scraping Telegram channels. The platform is designed using an **ELT framework** with PostgreSQL as the data warehouse and dbt for transformations. Insights from the data include top products, posting trends, visual content analysis, and channel-level engagement metrics.

---

## **Task 1: Data Scraping and Collection**

### Objective
Collect raw Telegram data for Ethiopian medical and cosmetic channels to populate the data lake.

### Implementation
- **Library Used:** [Telethon](https://docs.telethon.dev/) for Telegram scraping.
- **Data Collected:** Messages, media information, channel details, timestamps, views, and forwards.
- **Channels Scraped:**  
  - lobelia4cosmetics  
  - tikvahpharma
- **Date Range:** Multiple days (e.g., 2026-01-16 to 2026-01-18)

### Data Storage
- Raw JSON files stored in `/data/raw` folder.
- SQLite database (`telegram_warehouse.db`) used for development; PostgreSQL intended for production.

### Example Data
| message_id | channel_name       | message_date        | message_text         | has_media | views | forwards |
|------------|------------------|------------------|--------------------|-----------|-------|----------|
| 101        | lobelia4cosmetics | 2026-01-16 10:00 | New skincare set!  | TRUE      | 120   | 5        |
| 102        | tikvahpharma      | 2026-01-16 11:30 | Discount on meds!  | FALSE     | 95    | 2        |

---

## **Task 2: Data Modeling and Transformation**

### Objective
Transform raw Telegram data into a **star schema** for analytics.

### DBT Project Structure
- **Models:**
  - `staging/stg_telegram_messages.sql`
  - `staging/stg_telegram_channels.sql`
  - `marts/dim_channels.sql`
  - `marts/dim_dates.sql`
  - `marts/fct_messages.sql`
- **Sources:** `models/sources.yml` defines `raw.telegram_messages` source.
- **Macros:** Standardized functions for data transformations.

### Star Schema
- **Fact Table:** `fct_messages` – contains message-level metrics (views, forwards, has_media)
- **Dimension Tables:**
  - `dim_channels` – stores channel-level info (total posts, average views)
  - `dim_dates` – date dimension for time-based analysis

#### Schema Diagram
[dim_channels] <-- [fct_messages] --> [dim_dates]

### Staging Transformations
- Cast message dates to proper timestamps.
- Normalize booleans for media presence.
- Filter out messages with missing or future dates.
- Extract image paths from message text.

### Data Quality Checks (dbt Tests)
- `not_null`: message_id, channel_key, date_key, etc.
- `unique`: message_id, channel_key, date_key
- `relationships`: foreign keys from fact to dimension tables

### Example Queries
- Top channels by message count
- Average views per channel
- Messages with media vs. without media

---

## **Next Steps**
- Task 3: **Data Enrichment** using YOLOv8 for image analysis.
- Task 4: **Analytical FastAPI** for querying insights.
- Task 5: **Pipeline Orchestration** with Dagster.

---

## **Setup Instructions**

1. **Clone repository**
```bash
git clone <repo_url>
cd medical-telegram-warehouse

2. **Create virtual environment**

python -m venv .venv
.\.venv\Scripts\activate   # Windows
pip install -r requirements.txt


Load raw data

python src/load_to_db.py
# Choose SQLite for development or PostgreSQL for production


Run dbt models

cd medical_warehouse
dbt clean
dbt deps
dbt run
dbt test


Explore SQLite database

sqlite3 data/telegram_warehouse.db