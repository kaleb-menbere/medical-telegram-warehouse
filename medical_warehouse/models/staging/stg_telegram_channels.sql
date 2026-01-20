-- medical_warehouse/models/staging/stg_telegram_channels.sql
{{ config(materialized='view') }}

WITH channel_stats AS (
    SELECT
        channel_name,
        channel_username,
        COUNT(*) as total_posts,
        SUM(CASE WHEN has_media THEN 1 ELSE 0 END) as posts_with_media,
        AVG(views) as avg_views,
        AVG(forwards) as avg_forwards,
        MIN(message_date) as first_post_date,
        MAX(message_date) as last_post_date
    FROM {{ source('raw', 'telegram_messages') }}
    GROUP BY channel_name, channel_username
)

SELECT
    ROW_NUMBER() OVER (ORDER BY channel_name) as channel_key,
    channel_name,
    channel_username,
    total_posts,
    posts_with_media,
    ROUND(avg_views::numeric, 2) as avg_views,
    ROUND(avg_forwards::numeric, 2) as avg_forwards,
    first_post_date,
    last_post_date,
    CASE 
        WHEN channel_name ILIKE '%pharma%' OR channel_name ILIKE '%drug%' OR channel_name ILIKE '%med%' THEN 'Pharmaceutical'
        WHEN channel_name ILIKE '%cosmetic%' OR channel_name ILIKE '%beauty%' THEN 'Cosmetics'
        ELSE 'Medical'
    END as channel_type
FROM channel_stats