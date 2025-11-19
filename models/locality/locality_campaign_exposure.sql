{{
    config(
        materialized='incremental',
        unique_key=['transaction_id', 'AKKIO_ID'],
        incremental_strategy='merge',
        partition_by='event_date'
    )
}}

-- Campaign exposures from Freewheel, enriched with LoopMe conversion data
-- LoopMe provides conversion attribution - checking if exposed households converted

WITH freewheel_with_households AS (
    SELECT
        fw.*,
        COALESCE(ita_ip.AKKIO_ID, ita_device.AKKIO_ID) AS AKKIO_ID
    FROM {{ source('locality_poc_share_gold', 'freewheel_logs_gold') }} fw
    LEFT JOIN {{ ref('identity_to_akkio_deduped') }} ita_ip
        ON fw.ip_address = ita_ip.IDENTITY AND ita_ip.ID_TYPE = 'ip'
    LEFT JOIN {{ ref('identity_to_akkio_deduped') }} ita_device
        ON fw.device_id = ita_device.IDENTITY
    WHERE COALESCE(ita_ip.AKKIO_ID, ita_device.AKKIO_ID) IS NOT NULL
    {% if is_incremental() %}
        AND fw.event_date > (SELECT MAX(event_date) FROM {{ this }})
    {% endif %}
),

loopme_conversions AS (
    SELECT DISTINCT
        ita.AKKIO_ID,
        l.locality_campaign_id,
        l.loopme_campaign_id
    FROM {{ source('locality_poc_share_silver', 'loopme') }} l
    INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
        ON (l.maid = ita.IDENTITY) OR (l.ip = ita.IDENTITY)
)

SELECT
    fw.AKKIO_ID,
    -- LoopMe enrichment: campaign ID and conversion flag
    conv.loopme_campaign_id AS LOOPME_CAMPAIGN_ID,
    CASE WHEN conv.AKKIO_ID IS NOT NULL THEN TRUE ELSE FALSE END AS HAS_LOOPME_CONVERSION,
    -- Include ALL freewheel columns (70+ fields)
    fw.transaction_id,
    fw.ad_unit_id,
    fw.event_start_time,
    fw.event_end_time,
    fw.ip_address,
    fw.device_id_prefix,
    fw.device_id,
    fw.platform_group,
    fw.placement_id,
    fw.fw_campaign_id,
    fw.fw_placement_id,
    fw.locality_campaign_id,
    fw.locality_placement_id,
    fw.locality_advertiser,
    fw.locality_agency,
    fw.advertiser_category,
    fw.locality_campaign,
    fw.locality_placement_name,
    fw.locality_product,
    fw.locality_campaign_start_date,
    fw.locality_campaign_end_date,
    fw.locality_placement_end_date,
    fw.locality_placement_start_date,
    fw.creative_id,
    fw.creative_rendition_id,
    fw.creative_duration,
    fw.net_value,
    fw.revenue,
    fw.visitor_city,
    fw.visitor_state_province,
    fw.visitor_postal_code,
    fw.visitor_dma,
    fw.visitor_country,
    fw.global_brand_id,
    fw.global_advertiser_id,
    fw.platform_browser_id,
    fw.platform_os_id,
    fw.platform_device_id,
    fw.content_provider_partner_id,
    fw.external_reseller,
    fw.site_id,
    fw.site_section_id,
    fw.video_asset_id,
    fw.time_position_class,
    fw.slot_index,
    fw.position_in_slot,
    fw.time_position,
    fw.ad_unit_hash,
    fw.unique_identifier,
    fw.custom_visitor_id,
    fw.visitor_time_zone_offset,
    fw.parsed_user_agent,
    fw.max_duration,
    fw.raw_user_agent,
    fw.sales_channel_type,
    fw.matched_audience_item,
    fw.purchased_order_id,
    fw.content_brand_id,
    fw.genre_id,
    fw.language_id,
    fw.endpoint_owner_id,
    fw.endpoint_id,
    fw.stream_type,
    fw.standard_environment_id,
    fw.standard_os_id,
    fw.standard_device_type_id,
    fw.content_duration,
    fw.ip_enabled_audience,
    fw.programmer_id,
    fw.content_channel_id,
    fw.matched_inventory_package,
    fw.matched_targeting_items,
    fw.global_slot_index,
    fw.event_date
FROM freewheel_with_households fw
LEFT JOIN loopme_conversions conv
    ON fw.AKKIO_ID = conv.AKKIO_ID
    AND fw.locality_campaign_id = conv.locality_campaign_id
