{{
    config(
        materialized='incremental',
        unique_key=['akkio_id', 'segment_id', 'segment_source']
    )
}}

-- NOTE: Datonics processing is SKIPPED due to massive table size (609B rows)
-- Datonics will require specialized handling (batching, streaming, or alternative approach)
-- For now, only processing Inscape segments (692M rows -> ~410M matched)

select distinct
    ita.akkio_id,
    i_seg.segment_id,
    {{ normalize_segment_name('i_seg.segment_name') }} as segment_name,
    cast(null as string) as segment_description,
    'inscape' as segment_source
from {{ source('locality_poc_share_silver', 'inscape_segments') }} i_seg
inner join {{ ref('identity_to_akkio_deduped') }} ita
    on i_seg.ip_address = ita.identity
    and ita.id_type = 'ip'

-- TODO: Add datonics segments with proper batching strategy
-- datonics_segments as (
--     select distinct
--         ita.akkio_id,
--         d_seg.segment as segment_id,
--         {{ normalize_segment_name('d_seg.segment_name') }} as segment_name,
--         d_seg.segment_description,
--         'datonics' as segment_source
--     from {{ ref('int_datonics_ids_matched') }} d_ids
--     inner join {{ source('locality_poc_share_silver', 'datonics_segments') }} d_seg
--         on d_ids.segment = d_seg.segment
--     inner join {{ ref('identity_to_akkio_deduped') }} ita
--         on d_ids.id = ita.identity
--         and d_ids.id_type = ita.id_type
-- )
