{{
    config(
        materialized='table'
    )
}}

-- CRITICAL: This MUST be a TABLE not a VIEW
-- It's referenced 3+ times in downstream models and would re-scan 7.9B rows each time
-- Build time: ~5-10 min | Rows: 1.76B | Reused by: akkio_segments, locality_campaign_exposure

select distinct
    e_map.identity,
    e_map.id_type,
    attr.akkio_id
from {{ source('locality_poc_share_silver', 'experian_consolidated_id_map') }} e_map
inner join {{ ref('v_akkio_attributes_latest') }} attr
    on e_map.hh_id = attr.akkio_id
where e_map.id_type in ('ip', 'ctv', 'idfa', 'aaid')
