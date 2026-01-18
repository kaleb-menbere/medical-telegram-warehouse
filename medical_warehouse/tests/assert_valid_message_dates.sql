-- tests/assert_valid_message_dates.sql
SELECT 
    message_id,
    message_date,
    channel_name
FROM {{ ref('stg_telegram_messages') }}
WHERE message_date < '2020-01-01'  -- Assuming no messages before 2020
   OR message_date IS NULL