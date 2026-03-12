{{
    config(
        materialized='table'
    )
}}

-- Small dimension table (~1,200+ rows) of all distinct segments across sources.
-- Use this for string matching / segment discovery, then join the resulting
-- SEGMENT_IDs back to locality_segments for the household list.

WITH datonics AS (
    SELECT
        CAST(segment AS STRING) AS SEGMENT_ID,
        CONCAT_WS(', ',
            L1,
            CASE WHEN L2 IS NOT NULL THEN L2 END,
            CASE WHEN L3 IS NOT NULL THEN L3 END,
            CASE WHEN L4 IS NOT NULL THEN L4 END,
            CASE WHEN L5 IS NOT NULL THEN L5 END
        ) AS SEGMENT_NAME,
        L1 AS SEGMENT_L1,
        L2 AS SEGMENT_L2,
        L3 AS SEGMENT_L3,
        L4 AS SEGMENT_L4,
        L5 AS SEGMENT_L5,
        segment_description AS SEGMENT_DESCRIPTION,
        'datonics' AS SEGMENT_SOURCE
    FROM {{ ref('int_datonics_segments_metadata') }}
    WHERE record_type = 'segment'
),

inscape AS (
    SELECT DISTINCT
        i_seg.segment_id AS SEGMENT_ID,
        {{ normalize_segment_name('i_seg.segment_name') }} AS SEGMENT_NAME,
        CAST(NULL AS STRING) AS SEGMENT_L1,
        CAST(NULL AS STRING) AS SEGMENT_L2,
        CAST(NULL AS STRING) AS SEGMENT_L3,
        CAST(NULL AS STRING) AS SEGMENT_L4,
        CAST(NULL AS STRING) AS SEGMENT_L5,
        CAST(NULL AS STRING) AS SEGMENT_DESCRIPTION,
        'inscape' AS SEGMENT_SOURCE
    FROM {{ source('locality_poc_share_silver', 'inscape_segments') }} i_seg
),

onspot AS (
    SELECT DISTINCT
        o.audience_name AS SEGMENT_ID,
        {{ normalize_segment_name('o.audience_name') }} AS SEGMENT_NAME,
        CAST(NULL AS STRING) AS SEGMENT_L1,
        CAST(NULL AS STRING) AS SEGMENT_L2,
        CAST(NULL AS STRING) AS SEGMENT_L3,
        CAST(NULL AS STRING) AS SEGMENT_L4,
        CAST(NULL AS STRING) AS SEGMENT_L5,
        CAST(NULL AS STRING) AS SEGMENT_DESCRIPTION,
        'onspot' AS SEGMENT_SOURCE
    FROM {{ ref('onspot_audience_data') }} o
)

SELECT * FROM datonics
UNION ALL
SELECT * FROM inscape
UNION ALL
SELECT * FROM onspot
