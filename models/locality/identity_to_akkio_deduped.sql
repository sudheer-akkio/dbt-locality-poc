{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{this}} cluster by (IDENTITY, ID_TYPE)"
        ]
    )
}}

-- CRITICAL: This MUST be a TABLE not a VIEW
-- It's referenced 3+ times in downstream models and would re-scan 7.9B rows each time
-- Build time: ~5-10 min | Rows: 1.76B | Reused by: akkio_segments, locality_campaign_exposure

SELECT DISTINCT
    e_map.identity AS IDENTITY,
    e_map.id_type AS ID_TYPE,
    attr.AKKIO_ID
FROM {{ source('locality_poc_share_silver', 'experian_consolidated_id_map') }} e_map
INNER JOIN {{ ref('v_akkio_attributes_latest') }} attr
    ON e_map.hh_id = attr.AKKIO_ID
WHERE e_map.id_type IN ('ip', 'ctv', 'idfa', 'aaid')
