{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['device_id', 'audience_name']
    )
}}

/*
    OnSpot audience data: incremental copy from Delta Share into project schema.

    Source: locality_poc_share_silver.onspot (same Delta Share as inscape, datonics, etc.).
    Grain: (device_id, audience_name) — a device can belong to multiple audiences.
    Incremental: merge-only (no timestamp filter). Each run reads the full source
    and merges by (device_id, audience_name). If the share gains a _loaded_at or
    similar column, add {% if is_incremental() %} WHERE ... {% endif %} to limit scan.
*/

SELECT
    device_id,
    audience_name
FROM {{ source('locality_poc_share_silver', 'onspot') }}
