{{ 
    config(
        materialized='view',
        schema='staging'
    ) 
}}

WITH raw_messages AS (
    SELECT 
        message_id,
        channel_name,

        CASE 
            WHEN message_date ~ '^\d{4}-\d{2}-\d{2}' 
                THEN message_date::timestamp
            WHEN message_date ~ '^\d{10}$' 
                THEN to_timestamp(message_date::bigint)
            ELSE NULL
        END AS message_date,

        COALESCE(message_text, '') AS message_text,
        COALESCE(has_media, false)::boolean AS has_media,
        COALESCE(views, 0)::integer AS views,
        COALESCE(forwards, 0)::integer AS forwards

    FROM {{ source('raw', 'telegram_messages') }}
    WHERE message_id IS NOT NULL
      AND channel_name IS NOT NULL
      AND message_text IS NOT NULL
      AND message_text <> ''
)

SELECT
    message_id,
    channel_name,
    message_date,
    message_text,
    LENGTH(message_text) AS message_length,
    has_media AS has_image,
    views,
    forwards,
    TO_CHAR(message_date, 'YYYYMMDD')::integer AS date_key
FROM raw_messages
WHERE message_date IS NOT NULL
  AND message_date <= CURRENT_TIMESTAMP
