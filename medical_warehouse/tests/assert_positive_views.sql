-- Custom test: Ensure view counts are non-negative
SELECT 
    message_id,
    channel_name,
    view_count
FROM {{ ref('stg_telegram_messages') }}
WHERE view_count < 0