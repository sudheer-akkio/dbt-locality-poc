{{
    config(
        materialized='table'
    )
}}

WITH one_luid_per_household AS (
    SELECT
        hh_id,
        MIN(luid) AS luid
    FROM {{ source('locality_poc_share_silver', 'experian_consolidated_id_map') }}
    GROUP BY hh_id
)

SELECT DISTINCT
    olh.hh_id AS {{ locality_id_col() }},
    e_cv.`MOSAIC_HH_v3` AS SEGMENT_ID,
    'experian_mosaic' AS SEGMENT_SOURCE
FROM one_luid_per_household olh
INNER JOIN {{ source('locality_poc_share_silver', 'experian_consumerview2') }} e_cv
    ON olh.luid = e_cv.recd_luid
INNER JOIN {{ ref('experian_mosaic_hh_decode') }} m
    ON e_cv.`MOSAIC_HH_v3` = m.mosaic_code
