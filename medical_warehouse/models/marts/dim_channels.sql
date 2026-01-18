-- models/marts/dim_channels.sql
{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH channel_stats AS (
    SELECT 
        channel_name,
        channel_type_derived,
        COUNT(*) AS total_posts,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date,
        AVG(views)::numeric(10,2) AS avg_views,
        AVG(forwards)::numeric(10,2) AS avg_forwards,
        SUM(CASE WHEN has_image THEN 1 ELSE 0 END) AS total_images
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY 1, 2
),

ranked_channels AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY channel_name) AS channel_key
    FROM channel_stats
)

SELECT 
    channel_key,
    channel_name,
    channel_type_derived AS channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views,
    avg_forwards,
    total_images,
    total_images::float / NULLIF(total_posts, 0) * 100 AS image_percentage
FROM ranked_channels