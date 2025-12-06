{% macro process_datonics_category(category_name) %}

-- Process a single datonics category with leaf-filtering and AKKIO_ID join
-- This macro:
--   1. Filters to the specified category segments
--   2. Applies leaf-filtering (removes redundant parent segments per ID)
--   3. Joins to identity_to_akkio_deduped by id_type for efficiency
--   4. Pivots segment hierarchy into L1-L5 columns

WITH category_segments AS (
    SELECT
        segment,
        segment_name,
        segment_description,
        L1, L2, L3, L4, L5
    FROM {{ ref('int_datonics_segments_metadata') }}
    WHERE record_type = 'segment'
        AND category = '{{ category_name }}'
),

parent_child AS (
    SELECT
        parent_segment,
        child_segment
    FROM {{ ref('int_datonics_segments_metadata') }}
    WHERE record_type = 'parent_child'
        AND category = '{{ category_name }}'
),

{% for id_type in ['ip', 'aaid', 'idfa', 'ctv'] %}

{{ id_type }}_ids AS (
    SELECT d.id, d.id_type, d.segment
    FROM {{ source('locality_poc_share_silver', 'datonics_ids') }} d
    WHERE d.segment IN (SELECT segment FROM category_segments)
        AND d.id_type = '{{ id_type }}'
),

{{ id_type }}_leaf AS (
    SELECT ids.*
    FROM {{ id_type }}_ids ids
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ id_type }}_ids ids2
        INNER JOIN parent_child pc
            ON ids2.segment = pc.child_segment
        WHERE ids2.id = ids.id
            AND ids2.id_type = ids.id_type
            AND pc.parent_segment = ids.segment
    )
),

{{ id_type }}_with_akkio AS (
    SELECT DISTINCT
        ita.AKKIO_ID,
        leaf.segment,
        seg.segment_name,
        seg.segment_description,
        seg.L1,
        seg.L2,
        seg.L3,
        seg.L4,
        seg.L5
    FROM {{ id_type }}_leaf leaf
    INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
        ON leaf.id = ita.IDENTITY
        AND leaf.id_type = ita.ID_TYPE
    INNER JOIN category_segments seg
        ON leaf.segment = seg.segment
){{ "," if not loop.last else "" }}

{% endfor %}

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
FROM ip_with_akkio

UNION ALL

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
FROM aaid_with_akkio

UNION ALL

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
FROM idfa_with_akkio

UNION ALL

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
FROM ctv_with_akkio

{% endmacro %}
