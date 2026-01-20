-- Test to ensure message dates are valid (not null and within reasonable range)
SELECT 
    message_id,
    post_timestamp
FROM {{ ref('stg_telegram_messages') }}
WHERE post_timestamp IS NULL 
   OR post_timestamp < '2020-01-01'  -- Assuming no messages before 2020
   OR post_timestamp > CURRENT_TIMESTAMP + INTERVAL '1 day'