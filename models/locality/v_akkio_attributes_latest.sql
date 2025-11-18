{{
    config(
        materialized='table',
        alias='V_AKKIO_ATTRIBUTES_LATEST',
        post_hook=[
            "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_ID)"
        ]
    )
}}

WITH one_luid_per_household AS (
    SELECT
        hh_id,
        MIN(luid) AS luid
    FROM {{ source('locality_poc_share_silver', 'experian_consolidated_id_map') }}
    GROUP BY hh_id
),

experian_attributes AS (
    SELECT
        -- Primary Keys
        olh.hh_id AS AKKIO_ID,
        olh.luid AS LUID,

        -- Temporal
        CURRENT_DATE() AS PARTITION_DATE,

        -- HOUSING TYPE (decode from segments)
        CASE
            WHEN e_cv.luxury_apt_renters THEN 'Luxury Apartment Renter'
            WHEN e_cv.high_rise_apt_renters THEN 'High-Rise Apartment Renter'
            WHEN e_cv.pet_friendly_apt_renters THEN 'Pet-Friendly Apartment Renter'
            WHEN e_cv.young_profess_apt_renters THEN 'Young Professional Apartment Renter'
            WHEN e_cv.family_focus_apt_renters THEN 'Family-Focused Apartment Renter'
            WHEN e_cv.fitness_apt_renters THEN 'Fitness-Oriented Apartment Renter'
            WHEN e_cv.outdoor_loving_apt_renters THEN 'Outdoor-Loving Apartment Renter'
            WHEN e_cv.urban_apt_renters THEN 'Urban Apartment Renter'
            WHEN e_cv.young_family_homeowners THEN 'Young Family Homeowner'
            WHEN e_cv.growing_family_homeowners THEN 'Growing Family Homeowner'
            WHEN e_cv.second_homeowners THEN 'Second Homeowner'
            WHEN e_cv.millennial_homeowners THEN 'Millennial Homeowner'
            ELSE 'Unknown'
        END AS HOUSING_SEGMENT,

        -- HOME OWNERSHIP (derived from segments)
        CASE
            WHEN e_cv.young_family_homeowners
                OR e_cv.growing_family_homeowners
                OR e_cv.second_homeowners
                OR e_cv.millennial_homeowners
            THEN 1
            WHEN e_cv.luxury_apt_renters
                OR e_cv.high_rise_apt_renters
                OR e_cv.pet_friendly_apt_renters
                OR e_cv.young_profess_apt_renters
                OR e_cv.family_focus_apt_renters
                OR e_cv.fitness_apt_renters
                OR e_cv.outdoor_loving_apt_renters
                OR e_cv.urban_apt_renters
            THEN 0
            ELSE NULL
        END AS HOME_OWNERSHIP,

        -- NET WORTH (lower bound in thousands)
        CASE
            WHEN e_cv.hh_net_worth_50M_plus THEN 50000
            WHEN e_cv.hh_net_worth_20M_50M THEN 20000
            WHEN e_cv.hh_net_worth_10M_20M THEN 10000
            WHEN e_cv.hh_net_worth_5M_10M THEN 5000
            WHEN e_cv.hh_net_worth_2M_5M THEN 2000
            WHEN e_cv.hh_net_worth_1M_2M THEN 1000
            WHEN e_cv.hh_net_worth_500K_1M THEN 500
            WHEN e_cv.hh_net_worth_100K_500K THEN 100
            WHEN e_cv.hh_net_worth_under100K THEN 0
            ELSE NULL
        END AS NET_WORTH_K,

        -- NET WORTH BUCKET (integer codes 1-9)
        CASE
            WHEN e_cv.hh_net_worth_under100K THEN 1
            WHEN e_cv.hh_net_worth_100K_500K THEN 2
            WHEN e_cv.hh_net_worth_500K_1M THEN 3
            WHEN e_cv.hh_net_worth_1M_2M THEN 4
            WHEN e_cv.hh_net_worth_2M_5M THEN 5
            WHEN e_cv.hh_net_worth_5M_10M THEN 6
            WHEN e_cv.hh_net_worth_10M_20M THEN 7
            WHEN e_cv.hh_net_worth_20M_50M THEN 8
            WHEN e_cv.hh_net_worth_50M_plus THEN 9
            ELSE NULL
        END AS NET_WORTH_BUCKET,

        -- ESTIMATED HOUSEHOLD INCOME (lower bound in thousands)
        CASE
            WHEN e_cv.rc_ehi_amount_2_5Mplus THEN 2500
            WHEN e_cv.rc_ehi_amount_2M_2_5_M THEN 2000
            WHEN e_cv.rc_ehi_amount_1_5M_2M THEN 1500
            WHEN e_cv.rc_ehi_amount_1M_1_5M THEN 1000
            WHEN e_cv.rc_ehi_amount_750K_1M THEN 750
            WHEN e_cv.rc_ehi_amount_500K_750K THEN 500
            WHEN e_cv.rc_ehi_amount_250K_500K THEN 250
            ELSE NULL
        END AS HOUSEHOLD_INCOME_K,

        -- INCOME BUCKET (integer codes)
        CASE
            WHEN e_cv.rc_ehi_amount_250K_500K THEN 1
            WHEN e_cv.rc_ehi_amount_500K_750K THEN 2
            WHEN e_cv.rc_ehi_amount_750K_1M THEN 3
            WHEN e_cv.rc_ehi_amount_1M_1_5M THEN 4
            WHEN e_cv.rc_ehi_amount_1_5M_2M THEN 5
            WHEN e_cv.rc_ehi_amount_2M_2_5_M THEN 6
            WHEN e_cv.rc_ehi_amount_2_5Mplus THEN 7
            ELSE NULL
        END AS INCOME_BUCKET,

        -- HIGH WEALTH INDICATOR
        CASE WHEN e_cv.high_wealth_5m_plus THEN 'Y' ELSE 'N' END AS HIGH_WEALTH_FLAG,

        -- EMPLOYMENT INDICATORS
        CASE WHEN e_cv.employ_public_traded_co THEN 'Y' ELSE 'N' END AS EMPLOY_PUBLIC_COMPANY,
        CASE WHEN e_cv.employ_private_held_co THEN 'Y' ELSE 'N' END AS EMPLOY_PRIVATE_COMPANY,
        CASE WHEN e_cv.exec_decision_maker THEN 'Y' ELSE 'N' END AS EXECUTIVE_DECISION_MAKER,
        CASE WHEN e_cv.company_founder THEN 'Y' ELSE 'N' END AS COMPANY_FOUNDER,
        CASE WHEN e_cv.receive_high_value_stock THEN 'Y' ELSE 'N' END AS HIGH_VALUE_STOCK_HOLDER,

        -- HOME SALE PROPENSITY (strongest signal)
        CASE
            WHEN e_cv.homeowner_likely_sell_3mo THEN 'Very Likely (3mo)'
            WHEN e_cv.homeowner_likely_sell_6mo THEN 'Very Likely (6mo)'
            WHEN e_cv.homeowner_likely_sell_9mo THEN 'Very Likely (9mo)'
            WHEN e_cv.homeowner_likely_sell_12mo THEN 'Very Likely (12mo)'
            WHEN e_cv.home_somewhat_likely_sell_3mo THEN 'Somewhat Likely (3mo)'
            WHEN e_cv.home_somewhat_likely_sell_6mo THEN 'Somewhat Likely (6mo)'
            WHEN e_cv.home_somewhat_likely_sell_9mo THEN 'Somewhat Likely (9mo)'
            WHEN e_cv.home_somewhat_likely_sell_12mo THEN 'Somewhat Likely (12mo)'
            ELSE NULL
        END AS HOME_SALE_PROPENSITY,

        -- Processing Metadata
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT

    FROM one_luid_per_household olh
    LEFT JOIN {{ source('locality_poc_share_silver', 'experian_consumerview') }} e_cv
        ON olh.luid = e_cv.recd_luid
)

SELECT * FROM experian_attributes
