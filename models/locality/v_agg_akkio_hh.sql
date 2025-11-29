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
    Source: akkio_attributes_latest (now with real ConsumerView2 data)
    Grain: One row per AKKIO_HH_ID (household)

    Note: Since AKKIO_HH_ID = AKKIO_ID in the source, this is currently 1:1,
    but structured for future scenarios where multiple individuals may share a household.
*/

SELECT
    -- Primary Key
    attr.AKKIO_HH_ID,

    -- Weight (1.0 for equal weighting - FLOAT type required for insights)
    1.0 AS WEIGHT,
    1.0 AS HH_WEIGHT,  -- Alias for backwards compatibility with Insights queries

    -- ============================================================
    -- HOME OWNERSHIP & INCOME
    -- ============================================================
    CASE
        WHEN attr.HOME_OWNERSHIP LIKE '%Owner%' THEN 1
        WHEN attr.HOME_OWNERSHIP LIKE '%Rent%' THEN 0
        ELSE NULL
    END AS HOMEOWNER,

    attr.INCOME,
    attr.INCOME_BUCKET,

    -- ============================================================
    -- CHILDREN DATA
    -- ============================================================
    attr.CHILD_AGE_GROUP,
    attr.NUMBER_OF_CHILDREN,
    attr.PRESENCE_OF_CHILDREN,

    -- ============================================================
    -- HOUSEHOLD SIZE & HOME VALUE
    -- ============================================================
    attr.NUM_PEOPLE_IN_HOUSEHOLD_GROUP,
    attr.HOME_VALUE_RANGE AS MEDIAN_HOME_VALUE_BY_STATE,

    -- Temporal
    attr.PARTITION_DATE

FROM {{ ref('akkio_attributes_latest') }} attr
