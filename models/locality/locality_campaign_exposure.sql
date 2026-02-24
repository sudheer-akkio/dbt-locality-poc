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
--
-- Identity resolution strategy (fixes DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE):
--   1. IP match preferred over device (IP is more household-specific)
--   2. Device match (aaid/idfa only) as fallback when IP doesn't match
--   3. Geo tiebreaker: prefer AKKIO_ID whose state matches FreeWheel visitor_state_province
--   4. Deterministic fallback: lowest AKKIO_ID

WITH ip_matched AS (
    SELECT
        fw.*,
        ita_ip.AKKIO_ID,
        ROW_NUMBER() OVER (
            PARTITION BY fw.transaction_id
            ORDER BY
                CASE WHEN UPPER(fw.visitor_state_province) = attr.STATE THEN 0 ELSE 1 END,
                ita_ip.AKKIO_ID
        ) AS rn
    FROM {{ source('locality_poc_share_gold', 'freewheel_logs_gold') }} fw
    INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita_ip
        ON fw.ip_address = ita_ip.IDENTITY AND ita_ip.ID_TYPE = 'ip'
    LEFT JOIN {{ ref('akkio_attributes_latest') }} attr
        ON ita_ip.AKKIO_ID = attr.AKKIO_ID
    {% if is_incremental() %}
    WHERE fw.event_date > (SELECT MAX(event_date) FROM {{ this }})
    {% endif %}
),

device_matched AS (
    SELECT
        fw.*,
        ita_device.AKKIO_ID,
        ROW_NUMBER() OVER (
            PARTITION BY fw.transaction_id
            ORDER BY
                CASE WHEN UPPER(fw.visitor_state_province) = attr.STATE THEN 0 ELSE 1 END,
                ita_device.AKKIO_ID
        ) AS rn
    FROM {{ source('locality_poc_share_gold', 'freewheel_logs_gold') }} fw
    INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita_device
        ON fw.device_id = ita_device.IDENTITY
        AND ita_device.ID_TYPE IN ('aaid', 'idfa')
    LEFT JOIN {{ ref('akkio_attributes_latest') }} attr
        ON ita_device.AKKIO_ID = attr.AKKIO_ID
    LEFT JOIN ip_matched
        ON fw.transaction_id = ip_matched.transaction_id
    WHERE ip_matched.transaction_id IS NULL
    {% if is_incremental() %}
        AND fw.event_date > (SELECT MAX(event_date) FROM {{ this }})
    {% endif %}
),

freewheel_with_households AS (
    SELECT * FROM ip_matched WHERE rn = 1
    UNION ALL
    SELECT * FROM device_matched WHERE rn = 1
),

loopme_conversions AS (
    SELECT
        AKKIO_ID,
        locality_campaign_id,
        loopme_campaign_id
    FROM (
        -- MAID match (preferred — device-level identity)
        SELECT
            ita.AKKIO_ID,
            l.locality_campaign_id,
            l.loopme_campaign_id,
            ROW_NUMBER() OVER (
                PARTITION BY ita.AKKIO_ID, l.locality_campaign_id
                ORDER BY l.loopme_campaign_id
            ) AS rn
        FROM {{ source('locality_poc_share_silver', 'loopme') }} l
        INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
            ON l.maid = ita.IDENTITY AND ita.ID_TYPE IN ('aaid', 'idfa')

        UNION ALL

        -- IP fallback — only for loopme rows with no MAID match
        SELECT
            ita.AKKIO_ID,
            l.locality_campaign_id,
            l.loopme_campaign_id,
            ROW_NUMBER() OVER (
                PARTITION BY ita.AKKIO_ID, l.locality_campaign_id
                ORDER BY l.loopme_campaign_id
            ) AS rn
        FROM {{ source('locality_poc_share_silver', 'loopme') }} l
        INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
            ON l.ip = ita.IDENTITY AND ita.ID_TYPE = 'ip'
        WHERE NOT EXISTS (
            SELECT 1 FROM {{ ref('identity_to_akkio_deduped') }} ita_maid
            WHERE l.maid = ita_maid.IDENTITY AND ita_maid.ID_TYPE IN ('aaid', 'idfa')
        )
    )
    WHERE rn = 1
)

SELECT
    fw.AKKIO_ID,
    conv.loopme_campaign_id AS LOOPME_CAMPAIGN_ID,
    CASE WHEN conv.AKKIO_ID IS NOT NULL THEN TRUE ELSE FALSE END AS HAS_LOOPME_CONVERSION,
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
