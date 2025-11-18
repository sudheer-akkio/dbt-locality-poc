{{
    config(
        materialized='incremental',
        unique_key=['TRANSACTION_ID', 'AKKIO_ID'],
        incremental_strategy='merge',
        partition_by='EXPOSURE_DATE'
    )
}}

WITH loopme_exposures AS (
    SELECT
        ita.AKKIO_ID,
        l.locality_campaign_id AS LOCALITY_CAMPAIGN_ID,
        l.loopme_campaign_id AS LOOPME_CAMPAIGN_ID,
        CAST(NULL AS STRING) AS FW_CAMPAIGN_ID,
        l.date AS EXPOSURE_DATE,
        'LoopMe' AS SOURCE,
        CAST(NULL AS STRING) AS TRANSACTION_ID,
        l.ip AS IP_ADDRESS,
        CAST(NULL AS STRING) AS DEVICE_ID,
        CAST(NULL AS STRING) AS LOCALITY_ADVERTISER,
        CAST(NULL AS STRING) AS LOCALITY_CAMPAIGN,
        CAST(NULL AS INT) AS CREATIVE_ID,
        CAST(NULL AS DOUBLE) AS REVENUE,
        l.maid AS MAID
    FROM {{ source('locality_poc_share_silver', 'loopme') }} l
    INNER JOIN {{ ref('identity_to_akkio_deduped') }} ita
        ON (l.maid = ita.IDENTITY) OR (l.ip = ita.IDENTITY)
    {% if is_incremental() %}
        WHERE l.date > (SELECT MAX(EXPOSURE_DATE) FROM {{ this }})
    {% endif %}
),

freewheel_exposures AS (
    SELECT
        COALESCE(ita_ip.AKKIO_ID, ita_device.AKKIO_ID) AS AKKIO_ID,
        fw.locality_campaign_id AS LOCALITY_CAMPAIGN_ID,
        l.loopme_campaign_id AS LOOPME_CAMPAIGN_ID,
        fw.fw_campaign_id AS FW_CAMPAIGN_ID,
        fw.event_date AS EXPOSURE_DATE,
        'FreeWheel' AS SOURCE,
        fw.transaction_id AS TRANSACTION_ID,
        fw.ip_address AS IP_ADDRESS,
        fw.device_id AS DEVICE_ID,
        fw.locality_advertiser AS LOCALITY_ADVERTISER,
        fw.locality_campaign AS LOCALITY_CAMPAIGN,
        fw.creative_id AS CREATIVE_ID,
        fw.revenue AS REVENUE,
        CAST(NULL AS STRING) AS MAID
    FROM {{ source('locality_poc_share_gold', 'freewheel_logs_gold') }} fw
    LEFT JOIN (
        SELECT DISTINCT
            locality_campaign_id,
            loopme_campaign_id
        FROM {{ source('locality_poc_share_silver', 'loopme') }}
    ) l ON fw.locality_campaign_id = l.locality_campaign_id
    LEFT JOIN {{ ref('identity_to_akkio_deduped') }} ita_ip
        ON fw.ip_address = ita_ip.IDENTITY AND ita_ip.ID_TYPE = 'ip'
    LEFT JOIN {{ ref('identity_to_akkio_deduped') }} ita_device
        ON fw.device_id = ita_device.IDENTITY
    WHERE COALESCE(ita_ip.AKKIO_ID, ita_device.AKKIO_ID) IS NOT NULL
    {% if is_incremental() %}
        AND fw.event_date > (SELECT MAX(EXPOSURE_DATE) FROM {{ this }})
    {% endif %}
)

SELECT * FROM loopme_exposures

UNION ALL

SELECT * FROM freewheel_exposures
