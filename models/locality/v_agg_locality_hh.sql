{{ config(
    materialized='table',
    alias='V_AGG_LOCALITY_HH',
    post_hook=[
        "alter table {{this}} cluster by (PARTITION_DATE, LOCALITY_HH_ID)",
    ]
)}}

/*
    Locality Household Aggregation Table

    Purpose: Household-level aggregation of demographic attributes for analytics (Insights compatibility).
    Source: locality_attributes_latest (now with real ConsumerView2 data)
    Grain: One row per LOCALITY_HH_ID (household)

    Note: Since LOCALITY_HH_ID = LOCALITY_ID in the source, this is currently 1:1,
    but structured for future scenarios where multiple individuals may share a household.
*/

SELECT
    -- Primary Key
    attr.LOCALITY_HH_ID,

    -- Weight (1.0 for equal weighting - FLOAT type required for insights)
    1.0 AS WEIGHT,
    1.0 AS HH_WEIGHT,  -- Alias for backwards compatibility with Insights queries

    -- ============================================================
    -- HOME OWNERSHIP & INCOME
    -- ============================================================
    -- HOME_OWNERSHIP is now 'Homeowner'/'Renter' from locality_attributes_latest
    CASE attr.HOME_OWNERSHIP
        WHEN 'Homeowner' THEN 1
        WHEN 'Renter' THEN 0
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

FROM {{ ref('locality_attributes_latest') }} attr
