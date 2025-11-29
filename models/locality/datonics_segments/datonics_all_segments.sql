{{
    config(
        materialized='table',
        unique_key=['AKKIO_ID', 'SEGMENT_ID', 'SEGMENT_SOURCE']
    )
}}

-- Single-pass processing of all Datonics categories
-- This query processes all 29 categories at once, filtering to leaf segments per ID
-- and joining to AKKIO_IDs, split by id_type for performance

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

-- Process IP id_type
ip_ids AS (
    SELECT d.id, d.id_type, d.segment
    FROM {{ source('locality_poc_share_silver', 'datonics_ids') }} d
    WHERE d.id_type = 'ip'
        AND d.segment IN (SELECT segment FROM all_category_segments)
),

ip_leaf AS (
    SELECT ids.*
    FROM ip_ids ids
    WHERE NOT EXISTS (
        SELECT 1
        FROM ip_ids ids2
        INNER JOIN parent_child pc
            ON ids2.segment = pc.child_segment
        WHERE ids2.id = ids.id
            AND ids2.id_type = ids.id_type
            AND pc.parent_segment = ids.segment
    )
),

ip_with_akkio AS (
    SELECT DISTINCT
        ita.AKKIO_ID,
        leaf.segment,
        seg.L1,
        seg.L2,
        seg.L3,
        seg.L4,
        seg.L5,
        seg.segment_description
    FROM ip_leaf leaf
    INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
        ON leaf.id = ita.IDENTITY
        AND leaf.id_type = ita.ID_TYPE
    INNER JOIN all_category_segments seg
        ON leaf.segment = seg.segment
),

-- Process AAID id_type
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
),

aaid_with_akkio AS (
    SELECT DISTINCT
        ita.AKKIO_ID,
        leaf.segment,
        seg.L1,
        seg.L2,
        seg.L3,
        seg.L4,
        seg.L5,
        seg.segment_description
    FROM aaid_leaf leaf
    INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
        ON leaf.id = ita.IDENTITY
        AND leaf.id_type = ita.ID_TYPE
    INNER JOIN all_category_segments seg
        ON leaf.segment = seg.segment
),

-- Process IDFA id_type
idfa_ids AS (
    SELECT d.id, d.id_type, d.segment
    FROM {{ source('locality_poc_share_silver', 'datonics_ids') }} d
    WHERE d.id_type = 'idfa'
        AND d.segment IN (SELECT segment FROM all_category_segments)
),

idfa_leaf AS (
    SELECT ids.*
    FROM idfa_ids ids
    WHERE NOT EXISTS (
        SELECT 1
        FROM idfa_ids ids2
        INNER JOIN parent_child pc
            ON ids2.segment = pc.child_segment
        WHERE ids2.id = ids.id
            AND ids2.id_type = ids.id_type
            AND pc.parent_segment = ids.segment
    )
),

idfa_with_akkio AS (
    SELECT DISTINCT
        ita.AKKIO_ID,
        leaf.segment,
        seg.L1,
        seg.L2,
        seg.L3,
        seg.L4,
        seg.L5,
        seg.segment_description
    FROM idfa_leaf leaf
    INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
        ON leaf.id = ita.IDENTITY
        AND leaf.id_type = ita.ID_TYPE
    INNER JOIN all_category_segments seg
        ON leaf.segment = seg.segment
),

-- Process CTV id_type
ctv_ids AS (
    SELECT d.id, d.id_type, d.segment
    FROM {{ source('locality_poc_share_silver', 'datonics_ids') }} d
    WHERE d.id_type = 'ctv'
        AND d.segment IN (SELECT segment FROM all_category_segments)
),

ctv_leaf AS (
    SELECT ids.*
    FROM ctv_ids ids
    WHERE NOT EXISTS (
        SELECT 1
        FROM ctv_ids ids2
        INNER JOIN parent_child pc
            ON ids2.segment = pc.child_segment
        WHERE ids2.id = ids.id
            AND ids2.id_type = ids.id_type
            AND pc.parent_segment = ids.segment
    )
),

ctv_with_akkio AS (
    SELECT DISTINCT
        ita.AKKIO_ID,
        leaf.segment,
        seg.L1,
        seg.L2,
        seg.L3,
        seg.L4,
        seg.L5,
        seg.segment_description
    FROM ctv_leaf leaf
    INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
        ON leaf.id = ita.IDENTITY
        AND leaf.id_type = ita.ID_TYPE
    INNER JOIN all_category_segments seg
        ON leaf.segment = seg.segment
),

-- Union all id_types
all_segments AS (
    SELECT * FROM ip_with_akkio
    UNION ALL
    SELECT * FROM aaid_with_akkio
    UNION ALL
    SELECT * FROM idfa_with_akkio
    UNION ALL
    SELECT * FROM ctv_with_akkio
)

-- Final output with readable SEGMENT_NAME
SELECT
    AKKIO_ID,
    segment AS SEGMENT_ID,
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
FROM all_segments
