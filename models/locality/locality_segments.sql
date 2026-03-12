{{
    config(
        materialized='table',
        unique_key=[locality_id_col(), 'SEGMENT_ID', 'SEGMENT_SOURCE']
    )
}}

-- Fact table: household presence in segments.
-- Metadata (SEGMENT_NAME, L1-L5, DESCRIPTION) lives in segment_lookup.
-- Join: locality_segments JOIN segment_lookup USING (SEGMENT_ID, SEGMENT_SOURCE)

WITH inscape_segments AS (
    SELECT DISTINCT
        ita.{{ locality_id_col() }},
        i_seg.segment_id AS SEGMENT_ID,
        'inscape' AS SEGMENT_SOURCE
    FROM (SELECT * FROM {{ source('locality_poc_share_silver', 'inscape_segments') }}) i_seg
    INNER JOIN (SELECT * FROM {{ ref('identity_to_locality_deduped') }}) ita
        ON i_seg.ip_address = ita.IDENTITY
        AND ita.ID_TYPE = 'ip'
),

onspot_segments AS (
    SELECT DISTINCT
        ita.{{ locality_id_col() }},
        o.audience_name AS SEGMENT_ID,
        'onspot' AS SEGMENT_SOURCE
    FROM (SELECT * FROM {{ ref('onspot_audience_data') }}) o
    INNER JOIN (SELECT * FROM {{ ref('identity_to_locality_deduped') }}) ita
        ON o.device_id = ita.IDENTITY
)

SELECT * FROM inscape_segments
UNION ALL
SELECT {{ locality_id_col() }}, SEGMENT_ID, SEGMENT_SOURCE FROM {{ ref('datonics_segments_ip') }}
UNION ALL
SELECT {{ locality_id_col() }}, SEGMENT_ID, SEGMENT_SOURCE FROM {{ ref('datonics_segments_aaid') }}
UNION ALL
SELECT {{ locality_id_col() }}, SEGMENT_ID, SEGMENT_SOURCE FROM {{ ref('datonics_segments_idfa') }}
UNION ALL
SELECT {{ locality_id_col() }}, SEGMENT_ID, SEGMENT_SOURCE FROM {{ ref('datonics_segments_ctv') }}
UNION ALL
SELECT * FROM onspot_segments
