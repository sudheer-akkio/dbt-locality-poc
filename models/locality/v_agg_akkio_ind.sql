{{ config(
    materialized='table',
    alias='V_AGG_AKKIO_IND',
    post_hook=[
        "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_ID)",
    ]
)}}

/*
    Locality Individual Aggregation Table

    Purpose: Individual-level aggregation of demographic attributes for analytics.
    Source: akkio_attributes_latest (now with real ConsumerView2 data)
    Grain: One row per AKKIO_ID (household)

    Note: Like vizio, this is household-level data formatted for insights compatibility.
*/

SELECT
    -- Primary Keys
    attr.AKKIO_ID,
    attr.AKKIO_HH_ID,

    -- Weight (1.0 for equal weighting - FLOAT type required for insights)
    1.0 AS WEIGHT,

    -- ============================================================
    -- INDIVIDUAL DEMOGRAPHICS (real data from ConsumerView2)
    -- ============================================================
    COALESCE(attr.GENDER, 'UNDETERMINED') AS GENDER,
    attr.AGE,
    attr.AGE_BUCKET,
    COALESCE(attr.ETHNICITY, 'Unknown') AS ETHNICITY_PREDICTION,
    COALESCE(attr.EDUCATION_LEVEL, 'Unknown') AS EDUCATION,
    COALESCE(attr.MARITAL_STATUS, 'Unknown') AS MARITAL_STATUS,
    COALESCE(NULLIF(attr.STATE, ''), 'Unknown') AS STATE,
    CAST(NULL AS STRING) AS ZIP_CODE,

    -- Occupation
    attr.OCCUPATION,

    -- ============================================================
    -- HOUSEHOLD ATTRIBUTES
    -- ============================================================
    CASE
        WHEN attr.HOME_OWNERSHIP LIKE '%Owner%' THEN 1
        WHEN attr.HOME_OWNERSHIP LIKE '%Rent%' THEN 0
        ELSE NULL
    END AS HOMEOWNER,
    attr.INCOME,
    attr.INCOME_BUCKET,
    attr.NET_WORTH_BUCKET,

    -- ============================================================
    -- INTERESTS (comma-separated strings)
    -- ============================================================
    attr.GENERAL_INTERESTS,
    attr.SPORTS_INTERESTS,
    attr.READING_INTERESTS,
    attr.TRAVEL_INTERESTS,

    -- ============================================================
    -- VEHICLE DATA (comma-separated strings)
    -- ============================================================
    attr.VEHICLE_MAKES AS MAKE,
    attr.VEHICLE_STYLES AS VEHICLE_STYLE,
    attr.VEHICLE_CLASS,
    attr.FUEL_CODE,

    -- ============================================================
    -- FINANCIAL DATA
    -- ============================================================
    attr.FINANCIAL_HEALTH_BUCKET,
    attr.CREDIT_CARD_INFO,
    attr.INVESTMENT_TYPE,

    -- Contact identifiers (NULL strings for insights compatibility)
    CAST(NULL AS STRING) AS MAIDS,
    CAST(NULL AS STRING) AS IPS,
    CAST(NULL AS STRING) AS EMAILS,
    CAST(NULL AS STRING) AS PHONES,

    -- Temporal
    attr.PARTITION_DATE

FROM {{ ref('akkio_attributes_latest') }} attr
