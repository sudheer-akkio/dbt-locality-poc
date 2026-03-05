{{
    config(
        materialized='view',
        alias='V_LOCALITY_ATTRIBUTES_LATEST'
    )
}}

-- Shallow view for backwards compatibility with Insights system
-- Points to the actual table LOCALITY_ATTRIBUTES_LATEST

SELECT * FROM {{ ref('locality_attributes_latest') }}
