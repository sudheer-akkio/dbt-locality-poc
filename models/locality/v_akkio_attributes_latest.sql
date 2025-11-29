{{
    config(
        materialized='view',
        alias='V_AKKIO_ATTRIBUTES_LATEST'
    )
}}

-- Shallow view for backwards compatibility with Insights system
-- Points to the actual table AKKIO_ATTRIBUTES_LATEST

SELECT * FROM {{ ref('akkio_attributes_latest') }}
