-- models/marts/fct_messages.sql
{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH messages_with_keys AS (
    SELECT 
        stg.message_id,
        dc.channel_key,
        dd.date_key,
        stg.message_text,
        stg.message_length,
        stg.views AS view_count,
        stg.forwards AS forward_count,
        stg.has_image,
        stg.image_path,
        stg.message_date,
        ROW_NUMBER() OVER (
            PARTITION BY stg.message_id 
            ORDER BY stg.message_date DESC
        ) AS rn  -- Handle potential duplicates
    FROM {{ ref('stg_telegram_messages') }} stg
    LEFT JOIN {{ ref('dim_channels') }} dc 
        ON stg.channel_name = dc.channel_name
    LEFT JOIN {{ ref('dim_dates') }} dd 
        ON DATE(stg.message_date) = dd.full_date
    WHERE stg.message_date IS NOT NULL
)

SELECT 
    message_id,
    channel_key,
    date_key,
    message_text,
    message_length,
    view_count,
    forward_count,
    has_image,
    image_path,
    message_date
FROM messages_with_keys
WHERE rn = 1  -- Keep only the most recent version of each message