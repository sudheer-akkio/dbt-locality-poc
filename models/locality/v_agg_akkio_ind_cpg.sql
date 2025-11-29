{{ config(
    materialized='table',
    alias='V_AGG_AKKIO_IND_CPG',
    post_hook=[
        "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_ID)",
    ]
)}}

/*
    Locality Individual CPG (Consumer Packaged Goods) Table

    Purpose: Purchase behavior and spending patterns for analytics.
    Source: experian_consumerview2 (joined via akkio_attributes_latest.LUID)
    Grain: One row per AKKIO_ID (household)

    Contains: Categories purchased, purchase frequency, spending levels
*/

SELECT
    -- Primary Keys
    attr.AKKIO_ID,
    attr.AKKIO_HH_ID,

    -- Weight (1.0 for equal weighting)
    1.0 AS WEIGHT,

    -- ============================================================
    -- CATEGORIES PURCHASED (from TRX_ transaction data)
    -- ============================================================
    CONCAT_WS(',',
        CASE WHEN e_cv.TRX_Apparel_Spenders = 'Y' THEN 'Apparel' END,
        CASE WHEN e_cv.TRX_Apparel_High_Spenders = 'Y' THEN 'Apparel (High)' END,
        CASE WHEN e_cv.TRX_Pets_and_Animals_Spenders = 'Y' THEN 'Pets' END,
        CASE WHEN e_cv.TRX_Pets_and_Animals_High_Spenders = 'Y' THEN 'Pets (High)' END,
        CASE WHEN e_cv.TRX_Food_Snacks_Beverages_Spenders = 'Y' THEN 'Food & Beverages' END,
        CASE WHEN e_cv.TRX_Food_Snacks_Beverages_High_Spenders = 'Y' THEN 'Food & Beverages (High)' END,
        CASE WHEN e_cv.TRX_Health_and_Nutrition_Spenders = 'Y' THEN 'Health & Nutrition' END,
        CASE WHEN e_cv.TRX_Health_and_Nutrition_High_Spenders = 'Y' THEN 'Health & Nutrition (High)' END,
        CASE WHEN e_cv.TRX_Cosmetics_and_Beauty_Products_Spenders = 'Y' THEN 'Cosmetics & Beauty' END,
        CASE WHEN e_cv.TRX_Cosmetics_and_Beauty_Products_High_Spenders = 'Y' THEN 'Cosmetics & Beauty (High)' END,
        CASE WHEN e_cv.TRX_Home_Improvement_Spenders = 'Y' THEN 'Home Improvement' END,
        CASE WHEN e_cv.TRX_Home_Improvement_High_Spenders = 'Y' THEN 'Home Improvement (High)' END,
        CASE WHEN e_cv.TRX_Home_Decor_and_Linens_Spenders = 'Y' THEN 'Home Decor' END,
        CASE WHEN e_cv.TRX_Gardening_Outdoor_Decor_Spenders = 'Y' THEN 'Gardening & Outdoor' END,
        CASE WHEN e_cv.TRX_Cooking_Products_Spenders = 'Y' THEN 'Cooking Products' END,
        CASE WHEN e_cv.TRX_Household_Goods_Domestics_Spenders = 'Y' THEN 'Household Goods' END,
        CASE WHEN e_cv.TRX_Business_and_Home_Office_Spenders = 'Y' THEN 'Home Office' END,
        CASE WHEN e_cv.TRX_Crafts_Spenders = 'Y' THEN 'Crafts' END,
        CASE WHEN e_cv.TRX_Games_and_Puzzles_Spenders = 'Y' THEN 'Games & Puzzles' END,
        CASE WHEN e_cv.TRX_Jewelry_Spenders = 'Y' THEN 'Jewelry' END,
        CASE WHEN e_cv.TRX_Booklovers_Spenders = 'Y' THEN 'Books' END,
        CASE WHEN e_cv.TRX_Womens_Fashion_Spenders = 'Y' THEN 'Womens Fashion' END,
        CASE WHEN e_cv.TRX_Mens_Apparel_and_Accessories_Spenders = 'Y' THEN 'Mens Apparel' END,
        CASE WHEN e_cv.TRX_Hobbies_Spenders = 'Y' THEN 'Hobbies' END,
        CASE WHEN e_cv.TRX_Nutraceuticals_and_Supplements_Spenders = 'Y' THEN 'Supplements' END
    ) AS CATEGORIES_PURCHASED,

    -- ============================================================
    -- PURCHASE BUCKETS (spending levels by category)
    -- ============================================================
    CONCAT_WS(',',
        CASE WHEN e_cv.ConsumerSpend_Clothing IS NOT NULL AND e_cv.ConsumerSpend_Clothing != '' THEN CONCAT('Clothing:', e_cv.ConsumerSpend_Clothing) END,
        CASE WHEN e_cv.ConsumerSpend_Dining_Out IS NOT NULL AND e_cv.ConsumerSpend_Dining_Out != '' THEN CONCAT('Dining:', e_cv.ConsumerSpend_Dining_Out) END,
        CASE WHEN e_cv.ConsumerSpend_Travel IS NOT NULL AND e_cv.ConsumerSpend_Travel != '' THEN CONCAT('Travel:', e_cv.ConsumerSpend_Travel) END,
        CASE WHEN e_cv.ConsumerSpend_Entertainment IS NOT NULL AND e_cv.ConsumerSpend_Entertainment != '' THEN CONCAT('Entertainment:', e_cv.ConsumerSpend_Entertainment) END,
        CASE WHEN e_cv.ConsumerSpend_Electronics IS NOT NULL AND e_cv.ConsumerSpend_Electronics != '' THEN CONCAT('Electronics:', e_cv.ConsumerSpend_Electronics) END,
        CASE WHEN e_cv.ConsumerSpend_Donations IS NOT NULL AND e_cv.ConsumerSpend_Donations != '' THEN CONCAT('Donations:', e_cv.ConsumerSpend_Donations) END,
        CASE WHEN e_cv.ConsumerSpend_Education IS NOT NULL AND e_cv.ConsumerSpend_Education != '' THEN CONCAT('Education:', e_cv.ConsumerSpend_Education) END,
        CASE WHEN e_cv.ConsumerSpend_Home_Furnishings IS NOT NULL AND e_cv.ConsumerSpend_Home_Furnishings != '' THEN CONCAT('Home Furnishings:', e_cv.ConsumerSpend_Home_Furnishings) END,
        CASE WHEN e_cv.ConsumerSpend_Jewelry IS NOT NULL AND e_cv.ConsumerSpend_Jewelry != '' THEN CONCAT('Jewelry:', e_cv.ConsumerSpend_Jewelry) END
    ) AS PURCHASE_BUCKETS,

    -- ============================================================
    -- DISCRETIONARY SPENDING ESTIMATES (DSE)
    -- ============================================================
    e_cv.RC_DSE_Discretionary_Spend_Estimate AS TOTAL_DISCRETIONARY_SPEND,
    e_cv.RC_DSE_Apparel AS DSE_APPAREL,
    e_cv.RC_DSE_Dine_Out AS DSE_DINING,
    e_cv.RC_DSE_Entertainment AS DSE_ENTERTAINMENT,
    e_cv.RC_DSE_Education AS DSE_EDUCATION,
    e_cv.RC_DSE_Donation AS DSE_DONATIONS,
    e_cv.RC_DSE_Furnishings AS DSE_FURNISHINGS,
    e_cv.RC_DSE_Reading AS DSE_READING,
    e_cv.RC_DSE_Personal AS DSE_PERSONAL,

    -- ============================================================
    -- SHOPPING BEHAVIOR
    -- ============================================================
    CASE WHEN e_cv.TRX_InStore_Transactors = 'Y' THEN 'In-Store'
         WHEN e_cv.TRX_Online_Transactors = 'Y' THEN 'Online'
         ELSE NULL
    END AS PREFERRED_CHANNEL,

    CASE WHEN e_cv.TRX_Credit_Card_High_Spenders = 'Y' THEN 'Credit Card (High)'
         WHEN e_cv.TRX_Credit_Card_Users = 'Y' THEN 'Credit Card'
         WHEN e_cv.TRX_Cash_Users = 'Y' THEN 'Cash'
         ELSE NULL
    END AS PAYMENT_PREFERENCE,

    -- Coupon usage
    CASE WHEN e_cv.`RC_Buyer_Coupon_Users` = 'Y' THEN 1 ELSE 0 END AS IS_COUPON_USER,

    -- Shopping frequency indicators
    CASE WHEN e_cv.TRX_InStore_Transactors_Frequent_Spenders = 'Y' THEN 'Frequent In-Store'
         WHEN e_cv.TRX_Online_Transactors_Frequent_Spenders = 'Y' THEN 'Frequent Online'
         ELSE NULL
    END AS SHOPPING_FREQUENCY,

    -- Temporal
    attr.PARTITION_DATE

FROM {{ ref('akkio_attributes_latest') }} attr
LEFT JOIN {{ source('locality_poc_share_silver', 'experian_consumerview2') }} e_cv
    ON attr.LUID = e_cv.recd_luid
