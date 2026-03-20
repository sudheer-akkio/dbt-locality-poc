{{
    config(
        materialized='table'
    )
}}

SELECT DISTINCT
    ita.{{ locality_id_col() }},
    d.segment AS SEGMENT_ID,
    'datonics' AS SEGMENT_SOURCE
FROM {{ source('locality_poc_share_silver', 'datonics_consolidated_id_map') }} d
INNER JOIN {{ ref('identity_to_locality_deduped') }} ita
    ON d.identity = ita.IDENTITY
    AND d.id_type = ita.ID_TYPE
INNER JOIN {{ ref('int_datonics_segments_metadata') }} seg
    ON d.segment = seg.segment
    AND seg.record_type = 'segment'
WHERE d.id_type = 'aaid'
    AND d.active_status LIKE 'true%'
