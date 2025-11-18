{{
    config(
        materialized='table',
        unique_key=['AKKIO_ID', 'SEGMENT_ID', 'SEGMENT_SOURCE']
    )
}}

-- NOTE: Datonics processing is SKIPPED due to massive table size (609B rows)
-- Datonics will require specialized handling (batching, streaming, or alternative approach)
-- For now, only processing Inscape segments (692M rows -> ~410M matched)

SELECT DISTINCT
    ita.AKKIO_ID,
    i_seg.segment_id AS SEGMENT_ID,
    {{ normalize_segment_name('i_seg.segment_name') }} AS SEGMENT_NAME,
    CAST(NULL AS STRING) AS SEGMENT_DESCRIPTION,
    'inscape' AS SEGMENT_SOURCE
FROM {{ source('locality_poc_share_silver', 'inscape_segments') }} i_seg
INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
    ON i_seg.ip_address = ita.IDENTITY
    AND ita.ID_TYPE = 'ip'

{# TODO: Add datonics segments with proper batching strategy
datonics_segments AS (
    SELECT DISTINCT
        ita.AKKIO_ID,
        d_seg.segment AS SEGMENT_ID,
        {{ normalize_segment_name('d_seg.segment_name') }} AS SEGMENT_NAME,
        d_seg.segment_description AS SEGMENT_DESCRIPTION,
        'datonics' AS SEGMENT_SOURCE
    FROM {{ ref('int_datonics_ids_matched') }} d_ids
    INNER JOIN {{ source('locality_poc_share_silver', 'datonics_segments') }} d_seg
        ON d_ids.segment = d_seg.segment
    INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
        ON d_ids.id = ita.IDENTITY
        AND d_ids.id_type = ita.ID_TYPE
)
#}
