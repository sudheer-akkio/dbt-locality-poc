{{
    config(
        materialized='table',
        unique_key=['AKKIO_ID', 'SEGMENT_ID', 'SEGMENT_SOURCE']
    )
}}

-- Union of Inscape and Datonics segments
-- Datonics uses hierarchical L1-L5 structure, Inscape uses flat SEGMENT_NAME

WITH inscape_segments AS (
    SELECT DISTINCT
        ita.AKKIO_ID,
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
    INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
        ON i_seg.ip_address = ita.IDENTITY
        AND ita.ID_TYPE = 'ip'
),

datonics_segments AS (
    SELECT
        AKKIO_ID,
        SEGMENT_ID,
        SEGMENT_NAME,
        SEGMENT_L1,
        SEGMENT_L2,
        SEGMENT_L3,
        SEGMENT_L4,
        SEGMENT_L5,
        SEGMENT_DESCRIPTION,
        SEGMENT_SOURCE
    FROM {{ ref('datonics_all_segments') }}
)

SELECT * FROM inscape_segments
UNION ALL
SELECT * FROM datonics_segments
