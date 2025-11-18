{{
    config(
        materialized='view'
    )
}}

with one_luid_per_household as (
    select
        hh_id,
        min(luid) as luid
    from {{ source('locality_poc_share_silver', 'experian_consolidated_id_map') }}
    group by hh_id
)

select
    olh.hh_id as akkio_id,
    olh.luid,
    e_cv.*
from one_luid_per_household olh
left join {{ source('locality_poc_share_silver', 'experian_consumerview') }} e_cv
    on olh.luid = e_cv.recd_luid
