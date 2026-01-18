# Medical Telegram Warehouse - Data Model Documentation

## Overview
This data warehouse stores and analyzes Telegram data from Ethiopian medical businesses.

## Architecture
1. **Raw Layer**: Direct copy of scraped data
2. **Staging Layer**: Cleaned and standardized data
3. **Marts Layer**: Dimensional model for analytics

## Star Schema Design

### Fact Tables
- **fct_messages**: One row per Telegram message with metrics

### Dimension Tables
- **dim_channels**: Information about Telegram channels
- **dim_dates**: Date dimension for time-based analysis

## Key Business Questions Answered

1. **What are the top products mentioned?**
   - Use `product_category` in `fct_messages`

2. **How does engagement vary by channel?**
   - Join `fct_messages` with `dim_channels` on `channel_key`

3. **What are the posting trends over time?**
   - Join `fct_messages` with `dim_dates` on `date_key`

4. **Which channels have the most visual content?**
   - Filter `fct_messages` on `has_media = TRUE`

## Data Quality
- All primary keys have uniqueness and not-null tests
- Foreign key relationships are enforced
- Custom tests validate business rules
- Data is cleaned and standardized in staging