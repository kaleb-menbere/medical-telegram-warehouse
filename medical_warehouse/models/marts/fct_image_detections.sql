{{ config(materialized='table') }}

WITH yolo_detections AS (
    SELECT
        y.message_id,
        y.channel_name,
        y.image_path,
        y.detected_objects,
        y.num_detections,
        y.image_category,
        y.confidence_score,
        y.loaded_at
    FROM {{ source('raw', 'yolo_detections') }} y
),

joined_data AS (
    SELECT
        y.message_id,
        c.channel_key,
        f.date_key,
        y.image_path,
        y.detected_objects,
        y.num_detections,
        y.image_category,
        y.confidence_score,
        y.loaded_at,
        f.view_count,
        f.forward_count,
        f.has_media
    FROM yolo_detections y
    LEFT JOIN {{ ref('fct_messages') }} f
        ON y.message_id = f.message_id
    LEFT JOIN {{ ref('dim_channels') }} c
        ON y.channel_name = c.channel_name
    WHERE f.message_id IS NOT NULL
)

SELECT
    message_id,
    channel_key,
    date_key,
    image_path,
    detected_objects,
    num_detections,
    image_category,
    confidence_score,
    loaded_at,
    view_count,
    forward_count,
    has_media
FROM joined_data
