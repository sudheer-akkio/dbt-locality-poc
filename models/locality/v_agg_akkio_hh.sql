{{ config(
    materialized='table',
    alias='V_AGG_AKKIO_HH',
    post_hook=[
        "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_HH_ID)",
    ]
)}}

/*
    Locality Household Aggregation Table

    Purpose: Household-level aggregation of demographic attributes for analytics (Insights compatibility).
    Source: akkio_attributes_latest
    Grain: One row per AKKIO_HH_ID (household)

    Note: Since AKKIO_HH_ID = AKKIO_ID in the source, this is currently 1:1,
    but structured for future scenarios where multiple individuals may share a household.
*/

SELECT
    -- Primary Key
    attr.AKKIO_HH_ID,

    -- Weight (fixed at 11.0 per requirements - FLOAT type for insights)
    11.0 AS WEIGHT,

    -- Home Ownership (required for HOMEOWNERSHIP insight)
    attr.HOME_OWNERSHIP AS HOMEOWNER,

    -- Household Income (required for INCOME insight)
    attr.INCOME,
    attr.INCOME_RANGE AS INCOME_BUCKET,

    -- Temporal
    attr.PARTITION_DATE

FROM {{ ref('akkio_attributes_latest') }} attr
