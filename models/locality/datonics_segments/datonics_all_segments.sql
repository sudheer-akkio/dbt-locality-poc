{{
    config(
        materialized='table',
        unique_key=['AKKIO_ID', 'SEGMENT_ID', 'SEGMENT_SOURCE']
    )
}}

-- Consolidated Datonics segments processing
-- Processes all id_types in one pass with DISTINCT to handle:
--   1. Same AKKIO_ID reached via multiple id_types (ip, aaid, idfa, ctv)
--   2. Deduplication at join time (never materializes 118B intermediate rows)
--
-- Leaf-filtering disabled due to scale (NOT EXISTS infeasible at 600B+ rows)
-- All segments included; downstream can filter by hierarchy depth if needed

WITH all_category_segments AS (
    SELECT
        segment,
        segment_name,
        segment_description,
        category,
        L1, L2, L3, L4, L5
    FROM {{ ref('int_datonics_segments_metadata') }}
    WHERE record_type = 'segment'
)

SELECT DISTINCT
    ita.AKKIO_ID,
    d.segment AS SEGMENT_ID,
    CONCAT_WS(', ',
        seg.L1,
        CASE WHEN seg.L2 IS NOT NULL THEN seg.L2 END,
        CASE WHEN seg.L3 IS NOT NULL THEN seg.L3 END,
        CASE WHEN seg.L4 IS NOT NULL THEN seg.L4 END,
        CASE WHEN seg.L5 IS NOT NULL THEN seg.L5 END
    ) AS SEGMENT_NAME,
    seg.L1 AS SEGMENT_L1,
    seg.L2 AS SEGMENT_L2,
    seg.L3 AS SEGMENT_L3,
    seg.L4 AS SEGMENT_L4,
    seg.L5 AS SEGMENT_L5,
    seg.segment_description AS SEGMENT_DESCRIPTION,
    'datonics' AS SEGMENT_SOURCE
FROM {{ source('locality_poc_share_silver', 'datonics_ids') }} d
INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
    ON d.id = ita.IDENTITY
    AND d.id_type = ita.ID_TYPE
INNER JOIN all_category_segments seg
    ON d.segment = seg.segment
