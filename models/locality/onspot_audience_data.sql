{{
    config(
        materialized='table'
    )
}}

-- OnSpot audience data: full nightly rebuild from Delta Share.
-- Grain: (device_id, audience_name).
-- Must be a full rebuild (not incremental merge) so departed memberships are
-- dropped each night — incremental merge cannot delete rows that vanished
-- from source, which silently accumulates stale memberships.

SELECT
    device_id,
    audience_name
FROM {{ source('locality_poc_share_silver', 'onspot') }}
WHERE audience_name IS NOT NULL
