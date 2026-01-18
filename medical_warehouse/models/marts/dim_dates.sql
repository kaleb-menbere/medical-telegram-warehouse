-- models/marts/dim_dates.sql
{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH date_range AS (
    SELECT 
        generate_series(
            '2024-01-01'::date,
            '2026-12-31'::date,
            '1 day'::interval
        ) AS full_date
),

date_attributes AS (
    SELECT 
        full_date,
        TO_CHAR(full_date, 'YYYYMMDD')::integer AS date_key,
        EXTRACT(DAY FROM full_date) AS day_of_month,
        EXTRACT(DOW FROM full_date) AS day_of_week,
        TO_CHAR(full_date, 'Day') AS day_name,
        EXTRACT(WEEK FROM full_date) AS week_of_year,
        EXTRACT(MONTH FROM full_date) AS month,
        TO_CHAR(full_date, 'Month') AS month_name,
        EXTRACT(QUARTER FROM full_date) AS quarter,
        EXTRACT(YEAR FROM full_date) AS year,
        CASE 
            WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        CASE 
            WHEN EXTRACT(MONTH FROM full_date) = 1 AND EXTRACT(DAY FROM full_date) = 1 THEN 'New Year'
            -- Add other holidays as needed
            ELSE 'Regular Day'
        END AS holiday_flag
    FROM date_range
)

SELECT *
FROM date_attributes