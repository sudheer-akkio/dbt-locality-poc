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
    Source: akkio_attributes_latest
    Grain: One row per AKKIO_ID (household in this dataset)

    Note: Like vizio, this is household-level data formatted for insights compatibility.
    Individual demographics (age, gender, etc.) are not available in Experian Consumerview.
*/

SELECT
    -- Primary Keys
    attr.AKKIO_ID,
    attr.AKKIO_HH_ID,

    -- Weight (fixed at 11.0 per requirements - FLOAT type for insights)
    11.0 AS WEIGHT,

    -- Individual Demographics (COALESCE NULLs to defaults for insights compatibility)
    COALESCE(attr.GENDER, 'UNDETERMINED') AS GENDER,
    attr.AGE,
    attr.AGE_BUCKET,
    COALESCE(attr.ETHNICITY, 'Unknown') AS ETHNICITY_PREDICTION,
    COALESCE(attr.EDUCATION_LEVEL, 'Unknown') AS EDUCATION,
    COALESCE(attr.MARITAL_STATUS, 'Unknown') AS MARITAL_STATUS,
    COALESCE(NULLIF(attr.STATE, ''), 'Unknown') AS STATE,

    -- Household-level attributes (from Experian)
    attr.HOME_OWNERSHIP AS HOMEOWNER,
    attr.INCOME AS INCOME,
    attr.INCOME_RANGE AS INCOME_BUCKET,
    RIGHT(attr.ZIP11, 5) AS ZIP_CODE,

    COALESCE(CAST(attr.NET_WORTH_RANGE AS STRING), 'Unknown') AS NET_WORTH_BUCKET,

    -- Contact identifiers (NULL strings for insights compatibility)
    CAST(NULL AS STRING) AS MAIDS,
    CAST(NULL AS STRING) AS IPS,
    CAST(NULL AS STRING) AS EMAILS,
    CAST(NULL AS STRING) AS PHONES,

    -- Temporal
    attr.PARTITION_DATE

FROM {{ ref('akkio_attributes_latest') }} attr
