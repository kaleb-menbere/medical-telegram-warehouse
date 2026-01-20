{{ config(materialized='table') }}

WITH messages AS (
    SELECT
        m.message_id,
        c.channel_key,
        DATE(m.post_timestamp) as message_date,
        TO_CHAR(DATE(m.post_timestamp), 'YYYYMMDD')::integer as date_key,
        m.post_timestamp,
        m.message_text,
        m.message_length,
        m.view_count,
        m.forward_count,
        m.reply_count,
        m.has_media,
        m.media_type,
        m.image_path,
        m.edited,
        m.edit_date,
        m.pinned
    FROM {{ ref('stg_telegram_messages') }} m
    LEFT JOIN {{ ref('stg_telegram_channels') }} c
        ON m.channel_name = c.channel_name
    WHERE m.post_timestamp IS NOT NULL
)

SELECT
    message_id,
    channel_key,
    date_key,
    message_text,
    message_length,
    view_count,
    forward_count,
    reply_count,
    has_media,
    media_type,
    image_path,
    edited,
    edit_date,
    pinned
FROM messages