{{
    config(
        materialized='table',
        unique_key=['AKKIO_ID', 'SEGMENT_ID', 'SEGMENT_SOURCE']
    )
}}

-- Union of all Datonics segments across id_types
-- Each id_type is processed in a separate model for resilience

SELECT * FROM {{ ref('datonics_segments_ip') }}
UNION ALL
SELECT * FROM {{ ref('datonics_segments_aaid') }}
UNION ALL
SELECT * FROM {{ ref('datonics_segments_idfa') }}
UNION ALL
SELECT * FROM {{ ref('datonics_segments_ctv') }}
