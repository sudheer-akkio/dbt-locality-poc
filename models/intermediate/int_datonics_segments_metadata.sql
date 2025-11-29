{{
    config(
        materialized='table'
    )
}}

-- Pre-compute segment metadata and parent-child relationships for all datonics segments
-- This metadata table is small (~1191 segments + relationships) and can be reused across all category queries

WITH all_segments AS (
    SELECT
        segment,
        segment_name,
        segment_description,
        GET(SPLIT(segment_name, ' > '), 0) as category,
        SIZE(SPLIT(segment_name, ' > ')) as depth
    FROM {{ source('locality_poc_share_silver', 'datonics_segments') }}
    WHERE segment_name IS NOT NULL
),

segment_info AS (
    SELECT
        segment,
        segment_name,
        segment_description,
        category,
        depth,
        GET(SPLIT(segment_name, ' > '), 0) as L1,
        GET(SPLIT(segment_name, ' > '), 1) as L2,
        GET(SPLIT(segment_name, ' > '), 2) as L3,
        GET(SPLIT(segment_name, ' > '), 3) as L4,
        GET(SPLIT(segment_name, ' > '), 4) as L5
    FROM all_segments
),

parent_child AS (
    SELECT
        p.segment as parent_segment,
        c.segment as child_segment,
        p.category
    FROM all_segments p
    INNER JOIN all_segments c
        ON c.segment_name LIKE p.segment_name || ' > %'
        AND c.depth = p.depth + 1
        AND c.category = p.category
)

SELECT
    'segment' as record_type,
    segment,
    segment_name,
    segment_description,
    category,
    depth,
    L1, L2, L3, L4, L5,
    CAST(NULL AS STRING) as parent_segment,
    CAST(NULL AS STRING) as child_segment
FROM segment_info

UNION ALL

SELECT
    'parent_child' as record_type,
    CAST(NULL AS STRING) as segment,
    CAST(NULL AS STRING) as segment_name,
    CAST(NULL AS STRING) as segment_description,
    category,
    CAST(NULL AS INT) as depth,
    CAST(NULL AS STRING) as L1,
    CAST(NULL AS STRING) as L2,
    CAST(NULL AS STRING) as L3,
    CAST(NULL AS STRING) as L4,
    CAST(NULL AS STRING) as L5,
    parent_segment,
    child_segment
FROM parent_child
