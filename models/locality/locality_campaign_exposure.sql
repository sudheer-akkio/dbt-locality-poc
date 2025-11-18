{{
    config(
        materialized='incremental',
        unique_key=['transaction_id', 'akkio_id'],
        partition_by={
            'field': 'exposure_date',
            'data_type': 'date',
            'granularity': 'month'
        }
    )
}}

with loopme_exposures as (
    select
        ita.akkio_id,
        l.locality_campaign_id,
        l.loopme_campaign_id,
        cast(null as string) as fw_campaign_id,
        l.date as exposure_date,
        'loopme' as source,
        cast(null as string) as transaction_id,
        l.ip as ip_address,
        cast(null as string) as device_id,
        cast(null as string) as locality_advertiser,
        cast(null as string) as locality_campaign,
        cast(null as int) as creative_id,
        cast(null as double) as revenue,
        l.maid
    from {{ source('locality_poc_share_silver', 'loopme') }} l
    inner join {{ ref('identity_to_akkio_deduped') }} ita
        on (l.maid = ita.identity) or (l.ip = ita.identity)
    {% if is_incremental() %}
        where l.date > (select max(exposure_date) from {{ this }})
    {% endif %}
),

freewheel_exposures as (
    select
        coalesce(ita_ip.akkio_id, ita_device.akkio_id) as akkio_id,
        fw.locality_campaign_id,
        l.loopme_campaign_id,
        fw.fw_campaign_id,
        fw.event_date as exposure_date,
        'freewheel' as source,
        fw.transaction_id,
        fw.ip_address,
        fw.device_id,
        fw.locality_advertiser,
        fw.locality_campaign,
        fw.creative_id,
        fw.revenue,
        cast(null as string) as maid
    from {{ source('locality_poc_share_gold', 'freewheel_logs_gold') }} fw
    left join (
        select distinct locality_campaign_id, loopme_campaign_id
        from {{ source('locality_poc_share_silver', 'loopme') }}
    ) l on fw.locality_campaign_id = l.locality_campaign_id
    left join {{ ref('identity_to_akkio_deduped') }} ita_ip
        on fw.ip_address = ita_ip.identity and ita_ip.id_type = 'ip'
    left join {{ ref('identity_to_akkio_deduped') }} ita_device
        on fw.device_id = ita_device.identity
    where coalesce(ita_ip.akkio_id, ita_device.akkio_id) is not null
    {% if is_incremental() %}
        and fw.event_date > (select max(exposure_date) from {{ this }})
    {% endif %}
)

select * from loopme_exposures

union all

select * from freewheel_exposures

