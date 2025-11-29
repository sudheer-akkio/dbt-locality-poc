{{
    config(
        materialized='view'
    )
}}

-- Simple list of all distinct Datonics top-level categories
-- This makes it easy to see what categories exist and validate coverage

SELECT DISTINCT category
FROM {{ ref('int_datonics_segments_metadata') }}
WHERE record_type = 'segment'
  AND category IS NOT NULL
ORDER BY category
