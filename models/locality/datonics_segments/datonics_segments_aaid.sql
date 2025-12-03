{{
    config(
        materialized='table'
    )
}}

-- Datonics segments for AAID id_type only

WITH all_category_segments AS (
    SELECT
        segment,
        segment_name,
        segment_description,
        category,
        L1, L2, L3, L4, L5
    FROM {{ ref('int_datonics_segments_metadata') }}
    WHERE record_type = 'segment'
),

parent_child AS (
    SELECT
        parent_segment,
        child_segment
    FROM {{ ref('int_datonics_segments_metadata') }}
    WHERE record_type = 'parent_child'
),

aaid_ids AS (
    SELECT d.id, d.id_type, d.segment
    FROM {{ source('locality_poc_share_silver', 'datonics_ids') }} d
    WHERE d.id_type = 'aaid'
        AND d.segment IN (SELECT segment FROM all_category_segments)
),

aaid_leaf AS (
    SELECT ids.*
    FROM aaid_ids ids
    WHERE NOT EXISTS (
        SELECT 1
        FROM aaid_ids ids2
        INNER JOIN parent_child pc
            ON ids2.segment = pc.child_segment
        WHERE ids2.id = ids.id
            AND ids2.id_type = ids.id_type
            AND pc.parent_segment = ids.segment
    )
)

SELECT DISTINCT
    ita.AKKIO_ID,
    leaf.segment AS SEGMENT_ID,
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
FROM aaid_leaf leaf
INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
    ON leaf.id = ita.IDENTITY
    AND leaf.id_type = ita.ID_TYPE
INNER JOIN all_category_segments seg
    ON leaf.segment = seg.segment
