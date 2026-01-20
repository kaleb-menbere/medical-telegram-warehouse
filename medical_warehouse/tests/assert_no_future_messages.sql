-- Test to ensure no messages have future dates
SELECT 
    message_id,
    post_timestamp
FROM {{ ref('stg_telegram_messages') }}
WHERE post_timestamp > CURRENT_TIMESTAMP