{{ 
    config(
        materialized='view',
        schema='staging'
    ) 
}}

WITH messages AS (
    SELECT
        channel_name,
        COUNT(*) AS total_posts,
        AVG(views) AS avg_views,
        MIN(message_date) AS first_post_date,
        MAX(message_date) AS last_post_date
    FROM {{ ref('stg_telegram_messages') }}
    GROUP BY channel_name
)

SELECT
    ROW_NUMBER() OVER (ORDER BY channel_name) AS channel_key,
    channel_name,
    'Medical' AS channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views
FROM messages
