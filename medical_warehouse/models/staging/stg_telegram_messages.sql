-- medical_warehouse/models/staging/stg_telegram_messages.sql
{{ config(materialized='view') }}

WITH source AS (
    SELECT
        message_id,
        channel_name,
        channel_username,
        message_date,
        message_text,
        has_media,
        media_type,
        image_path,
        views,
        forwards,
        replies,
        edited,
        edit_date,
        pinned,
        scraped_at,
        scraping_session_id
    FROM {{ source('raw', 'telegram_messages') }}
),

cleaned AS (
    SELECT
        message_id,
        channel_name,
        channel_username,
        
        -- Handle date conversion safely
        CASE 
            WHEN message_date IS NOT NULL THEN message_date::timestamp
            ELSE NULL
        END as post_timestamp,
        
        CASE 
            WHEN message_date IS NOT NULL THEN DATE(message_date)
            ELSE NULL
        END as post_date,
        
        message_text,
        COALESCE(LENGTH(message_text), 0) as message_length,
        has_media,
        media_type,
        image_path,
        COALESCE(views, 0) as view_count,
        COALESCE(forwards, 0) as forward_count,
        COALESCE(replies, 0) as reply_count,
        edited,
        edit_date,
        pinned,
        scraped_at,
        scraping_session_id
    FROM source
)

SELECT *
FROM cleaned