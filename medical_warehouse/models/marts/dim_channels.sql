-- medical_warehouse/models/marts/dim_channels.sql
{{ config(materialized='table') }}

SELECT
    channel_key,
    channel_name,
    channel_username,
    channel_type,
    total_posts,
    posts_with_media,
    avg_views,
    avg_forwards,
    first_post_date,
    last_post_date
FROM {{ ref('stg_telegram_channels') }}