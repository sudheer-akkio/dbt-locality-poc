{{
    config(
        materialized='table',
        post_hook=[
            "alter table {{this}} cluster by (AKKIO_ID)",
        ]
    )
}}

-- Experian attributes with real demographic data from ConsumerView2
-- Replaces synthetic data with actual Experian demographics
-- Uses COALESCE for Person 1/Person 2 columns to get first available value

WITH one_luid_per_household AS (
    SELECT
        hh_id,
        MIN(luid) AS luid
    FROM {{ source('locality_poc_share_silver', 'experian_consolidated_id_map') }}
    GROUP BY hh_id
)

SELECT
    -- Primary Keys
    olh.hh_id AS AKKIO_ID,
    olh.hh_id AS AKKIO_HH_ID,
    olh.luid AS LUID,

    -- Temporal
    CURRENT_DATE() AS PARTITION_DATE,

    -- ============================================================
    -- INDIVIDUAL DEMOGRAPHICS (real data from ConsumerView2)
    -- ============================================================

    COALESCE(e_cv.`Person_RC_gndr_gndr_2`, NULL) AS GENDER,

    -- AGE: Parse range string to midpoint integer
    -- Age columns contain ranges like "25-34" - extract midpoint
    CASE
        WHEN COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '%-%' THEN
            CAST((
                CAST(SPLIT(COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`), '-')[0] AS INT) +
                CAST(SPLIT(COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`), '-')[1] AS INT)
            ) / 2 AS INT)
        ELSE NULL
    END AS AGE,

    -- AGE_BUCKET derived from AGE
    CASE
        WHEN COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '18%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '19%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '20%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '21%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '22%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '23%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '24%' THEN 1
        WHEN COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '25%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '26%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '27%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '28%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '29%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '30%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '31%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '32%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '33%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '34%' THEN 2
        WHEN COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '35%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '36%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '37%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '38%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '39%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '40%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '41%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '42%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '43%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '44%' THEN 3
        WHEN COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '45%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '46%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '47%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '48%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '49%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '50%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '51%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '52%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '53%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '54%' THEN 4
        WHEN COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '55%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '56%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '57%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '58%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '59%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '60%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '61%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '62%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '63%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '64%' THEN 5
        WHEN COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '65%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '66%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '67%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '68%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '69%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '70%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '71%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '72%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '73%' OR COALESCE(e_cv.`Person__RC_Person__Age_1`, e_cv.`Person__RC_Person__Age_2`) LIKE '74%' THEN 6
        ELSE 7
    END AS AGE_BUCKET,

    COALESCE(e_cv.`Person__RC_Ethnic_-_Group_1`, e_cv.`Person__RC_Ethnic_-_Group_2`) AS ETHNICITY,
    e_cv.`Person__RC_Person__Education_M_2` AS EDUCATION_LEVEL,
    e_cv.`Person__RC_Person__Marital_Status_2` AS MARITAL_STATUS,

    -- GEOGRAPHIC DATA
    e_cv.stat_abbr AS STATE,
    CAST(NULL AS STRING) AS ZIP11,
    CAST(NULL AS STRING) AS COUNTY_NAME,

    -- OCCUPATION
    COALESCE(e_cv.`Person__RC_Person__Occupation_1`, e_cv.`Person__RC_Person__Occupation_2`) AS OCCUPATION,
    e_cv.`Person_RC_Person_Title_1` AS OCCUPATION_TITLE,

    -- ============================================================
    -- HOUSEHOLD DATA
    -- ============================================================

    -- Home ownership
    e_cv.`RC_Homeowner_Combined_HomeownerRenter` AS HOME_OWNERSHIP,

    -- Income
    e_cv.`RC_Est_Household_Income_V6` AS INCOME_RANGE,

    -- Parse income to numeric - extract lower bound
    CASE
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%250%' AND e_cv.`RC_Est_Household_Income_V6` LIKE '%500%' THEN 250000
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%200%' THEN 200000
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%175%' THEN 175000
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%150%' THEN 150000
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%125%' THEN 125000
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%100%' THEN 100000
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%75%' THEN 75000
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%60%' THEN 60000
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%50%' THEN 50000
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%40%' THEN 40000
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%25%' THEN 25000
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%15%' THEN 15000
        ELSE NULL
    END AS INCOME,

    -- Income bucket for histograms
    CASE
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%250%' THEN 14
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%200%' THEN 13
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%175%' THEN 12
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%150%' THEN 11
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%125%' THEN 10
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%100%' THEN 9
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%75%' THEN 8
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%60%' THEN 7
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%50%' THEN 6
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%40%' THEN 5
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%25%' THEN 4
        WHEN e_cv.`RC_Est_Household_Income_V6` LIKE '%15%' THEN 3
        ELSE NULL
    END AS INCOME_BUCKET,

    -- Net worth from CFI score
    e_cv.`CFINet_Asset_Score` AS NET_WORTH_RANGE,

    -- Net worth bucket
    CASE
        WHEN e_cv.`CFINet_Asset_Score` LIKE '%9%' OR e_cv.`CFINet_Asset_Score` LIKE '%10%' THEN 9
        WHEN e_cv.`CFINet_Asset_Score` LIKE '%8%' THEN 8
        WHEN e_cv.`CFINet_Asset_Score` LIKE '%7%' THEN 7
        WHEN e_cv.`CFINet_Asset_Score` LIKE '%6%' THEN 6
        WHEN e_cv.`CFINet_Asset_Score` LIKE '%5%' THEN 5
        WHEN e_cv.`CFINet_Asset_Score` LIKE '%4%' THEN 4
        WHEN e_cv.`CFINet_Asset_Score` LIKE '%3%' THEN 3
        WHEN e_cv.`CFINet_Asset_Score` LIKE '%2%' THEN 2
        WHEN e_cv.`CFINet_Asset_Score` LIKE '%1%' THEN 1
        ELSE NULL
    END AS NET_WORTH_BUCKET,

    -- Home value
    e_cv.`RC_Estimated_Home_Value_range_` AS HOME_VALUE_RANGE,

    -- Household size
    e_cv.DU_size AS NUM_PEOPLE_IN_HOUSEHOLD_GROUP,

    -- ============================================================
    -- CHILDREN DATA
    -- ============================================================

    -- Child age groups (comma-separated)
    CONCAT_WS(',',
        CASE WHEN e_cv.`mom_0-3yrs_hh` = 'Y' THEN '0-3' END,
        CASE WHEN e_cv.`mom_4-6yrs_hh` = 'Y' THEN '4-6' END,
        CASE WHEN e_cv.mom_tween_hh = 'Y' THEN 'Tween' END,
        CASE WHEN e_cv.mom_teen_hh = 'Y' THEN 'Teen' END
    ) AS CHILD_AGE_GROUP,

    -- Number of children
    CASE
        WHEN e_cv.mom_2child_hh = 'Y' THEN '2+'
        WHEN e_cv.mom_1chld_hh = 'Y' THEN '1'
        ELSE NULL
    END AS NUMBER_OF_CHILDREN,

    -- Presence of children
    CASE
        WHEN e_cv.`mom_0-3yrs_hh` = 'Y' OR e_cv.`mom_4-6yrs_hh` = 'Y' OR e_cv.mom_tween_hh = 'Y' OR e_cv.mom_teen_hh = 'Y' OR e_cv.mom_1chld_hh = 'Y' OR e_cv.mom_2child_hh = 'Y' THEN 1
        ELSE 0
    END AS PRESENCE_OF_CHILDREN,

    -- ============================================================
    -- INTERESTS (comma-separated aggregations)
    -- ============================================================

    -- General interests
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_ActInt_Arts_and_Crafts` = 'Y' THEN 'Arts & Crafts' END,
        CASE WHEN e_cv.`RC_ActInt_Audio_Book_Listener` = 'Y' THEN 'Audio Books' END,
        CASE WHEN e_cv.`RC_ActInt_Book_Reader` = 'Y' THEN 'Reading' END,
        CASE WHEN e_cv.`RC_ActInt_Cat_Owners` = 'Y' THEN 'Cat Owner' END,
        CASE WHEN e_cv.`RC_ActInt_Coffee_Connoisseurs` = 'Y' THEN 'Coffee' END,
        CASE WHEN e_cv.`RC_ActInt_Cultural_Arts` = 'Y' THEN 'Cultural Arts' END,
        CASE WHEN e_cv.`RC_ActInt_Dog_Owners` = 'Y' THEN 'Dog Owner' END,
        CASE WHEN e_cv.`RC_ActInt_Do-it-yourselfers` = 'Y' THEN 'DIY' END,
        CASE WHEN e_cv.`RC_ActInt_E-Book_Reader` = 'Y' THEN 'E-Books' END,
        CASE WHEN e_cv.`RC_ActInt_Fitness_Enthusiast` = 'Y' THEN 'Fitness' END,
        CASE WHEN e_cv.`RC_ActInt_Gourmet_Cooking` = 'Y' THEN 'Gourmet Cooking' END,
        CASE WHEN e_cv.`RC_ActInt_Healthy_Living` = 'Y' THEN 'Healthy Living' END,
        CASE WHEN e_cv.`RC_ActInt_Home_Improvement_Spenders` = 'Y' THEN 'Home Improvement' END,
        CASE WHEN e_cv.`RC_ActInt_Music_Download` = 'Y' THEN 'Music Download' END,
        CASE WHEN e_cv.`RC_ActInt_Music_Streaming` = 'Y' THEN 'Music Streaming' END,
        CASE WHEN e_cv.`RC_ActInt_Outdoor_Enthusiast` = 'Y' THEN 'Outdoors' END,
        CASE WHEN e_cv.`RC_ActInt_Pet_Enthusiast` = 'Y' THEN 'Pets' END,
        CASE WHEN e_cv.`RC_ActInt_Photography` = 'Y' THEN 'Photography' END,
        CASE WHEN e_cv.`RC_ActIntVideo_Gamer` = 'Y' THEN 'Video Games' END,
        CASE WHEN e_cv.`RC_ActInt_Wine_Lovers` = 'Y' THEN 'Wine' END,
        CASE WHEN e_cv.`RC_Hobbies_Gardening` = 'Y' THEN 'Gardening' END
    ) AS GENERAL_INTERESTS,

    -- Sports interests
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_ActInt_Avid_Runners` = 'Y' THEN 'Running' END,
        CASE WHEN e_cv.`RC_ActInt_Boating` = 'Y' THEN 'Boating' END,
        CASE WHEN e_cv.`RC_ActInt_Fishing` = 'Y' THEN 'Fishing' END,
        CASE WHEN e_cv.`RC_ActInt_Hunting_Enthusiasts` = 'Y' THEN 'Hunting' END,
        CASE WHEN e_cv.`RC_ActIntMLB_Enthusiast` = 'Y' THEN 'MLB' END,
        CASE WHEN e_cv.`RC_ActIntNASCAR_Enthusiast` = 'Y' THEN 'NASCAR' END,
        CASE WHEN e_cv.`RC_ActIntNBA_Enthusiast` = 'Y' THEN 'NBA' END,
        CASE WHEN e_cv.`RC_ActIntNFL_Enthusiast` = 'Y' THEN 'NFL' END,
        CASE WHEN e_cv.`RC_ActIntNHL_Enthusiast` = 'Y' THEN 'NHL' END,
        CASE WHEN e_cv.`RC_ActIntPGA_Tour_Enthusiast` = 'Y' THEN 'Golf/PGA' END,
        CASE WHEN e_cv.`RC_ActIntPlay_Golf` = 'Y' THEN 'Golf' END,
        CASE WHEN e_cv.`RC_ActInt_Plays_Hockey` = 'Y' THEN 'Hockey' END,
        CASE WHEN e_cv.`RC_ActInt_Plays_Soccer` = 'Y' THEN 'Soccer' END,
        CASE WHEN e_cv.`RC_ActInt_Plays_Tennis` = 'Y' THEN 'Tennis' END,
        CASE WHEN e_cv.`RC_ActInt_Snow_Sports` = 'Y' THEN 'Snow Sports' END,
        CASE WHEN e_cv.`RC_ActInt_Sports_Enthusiast` = 'Y' THEN 'Sports General' END,
        CASE WHEN e_cv.`RC_ActIntCanoeingKayaking` = 'Y' THEN 'Canoeing/Kayaking' END
    ) AS SPORTS_INTERESTS,

    -- Reading interests
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_ActInt_Book_Reader` = 'Y' THEN 'Books' END,
        CASE WHEN e_cv.`RC_ActInt_E-Book_Reader` = 'Y' THEN 'E-Books' END,
        CASE WHEN e_cv.`RC_ActInt_Audio_Book_Listener` = 'Y' THEN 'Audio Books' END,
        CASE WHEN e_cv.`RC_ActInt_Digital_MagazineNewspapers_Buyers` = 'Y' THEN 'Digital Magazines' END
    ) AS READING_INTERESTS,

    -- Travel interests
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_Lifestyle_High_Frequency_Business_Traveler` = 'Y' THEN 'Business Travel' END,
        CASE WHEN e_cv.`RC_Lifestyle_High_Frequency_Cruise_Enthusiast` = 'Y' THEN 'Cruises' END,
        CASE WHEN e_cv.`RC_Lifestyle_High_Frequency_Domestic_Vacationer` = 'Y' THEN 'Domestic Vacation' END,
        CASE WHEN e_cv.`RC_Lifestyle_High_Frequency_Foreign_Vacationer` = 'Y' THEN 'Foreign Vacation' END,
        CASE WHEN e_cv.`RC_Lifestyle_Frequent_Flyer_Program_Member` = 'Y' THEN 'Frequent Flyer' END,
        CASE WHEN e_cv.`RC_Lifestyle_Hotel_Guest_Loyalty_Program` = 'Y' THEN 'Hotel Loyalty' END,
        CASE WHEN e_cv.`RC_ActInt_Amusement_Park_Visitors` = 'Y' THEN 'Amusement Parks' END,
        CASE WHEN e_cv.`RC_ActInt_Zoo_Visitors` = 'Y' THEN 'Zoos' END
    ) AS TRAVEL_INTERESTS,

    -- ============================================================
    -- VEHICLE DATA (comma-separated aggregations)
    -- ============================================================

    -- Vehicle makes owned
    CONCAT_WS(',',
        CASE WHEN e_cv.Auto_Seg_Own_Ford = 'Y' THEN 'Ford' END,
        CASE WHEN e_cv.Auto_Seg_Own_Chevrolet = 'Y' THEN 'Chevrolet' END,
        CASE WHEN e_cv.Auto_Seg_Own_Toyota = 'Y' THEN 'Toyota' END,
        CASE WHEN e_cv.Auto_Seg_Own_Honda = 'Y' THEN 'Honda' END,
        CASE WHEN e_cv.Auto_Seg_Own_Nissan = 'Y' THEN 'Nissan' END,
        CASE WHEN e_cv.Auto_Seg_Own_Jeep = 'Y' THEN 'Jeep' END,
        CASE WHEN e_cv.Auto_Seg_Own_GMC = 'Y' THEN 'GMC' END,
        CASE WHEN e_cv.Auto_Seg_Own_Hyundai = 'Y' THEN 'Hyundai' END,
        CASE WHEN e_cv.Auto_Seg_Own_Kia = 'Y' THEN 'Kia' END,
        CASE WHEN e_cv.Auto_Seg_Own_Subaru = 'Y' THEN 'Subaru' END,
        CASE WHEN e_cv.Auto_Seg_Own_BMW = 'Y' THEN 'BMW' END,
        CASE WHEN e_cv.Auto_Seg_Own_Mercedes_Benz = 'Y' THEN 'Mercedes-Benz' END,
        CASE WHEN e_cv.Auto_Seg_Own_Lexus = 'Y' THEN 'Lexus' END,
        CASE WHEN e_cv.Auto_Seg_Own_Audi = 'Y' THEN 'Audi' END,
        CASE WHEN e_cv.Auto_Seg_Own_Mazda = 'Y' THEN 'Mazda' END,
        CASE WHEN e_cv.Auto_Seg_Own_Volkswagen = 'Y' THEN 'Volkswagen' END,
        CASE WHEN e_cv.Auto_Seg_Own_Dodge = 'Y' THEN 'Dodge' END,
        CASE WHEN e_cv.Auto_Seg_Own_Ram = 'Y' THEN 'Ram' END,
        CASE WHEN e_cv.Auto_Seg_Own_Chrysler = 'Y' THEN 'Chrysler' END,
        CASE WHEN e_cv.Auto_Seg_Own_Buick = 'Y' THEN 'Buick' END,
        CASE WHEN e_cv.Auto_Seg_Own_Cadillac = 'Y' THEN 'Cadillac' END,
        CASE WHEN e_cv.Auto_Seg_Own_Acura = 'Y' THEN 'Acura' END,
        CASE WHEN e_cv.Auto_Seg_Own_Infiniti = 'Y' THEN 'Infiniti' END,
        CASE WHEN e_cv.Auto_Seg_Own_Lincoln = 'Y' THEN 'Lincoln' END,
        CASE WHEN e_cv.Auto_Seg_Own_Volvo = 'Y' THEN 'Volvo' END,
        CASE WHEN e_cv.Auto_Seg_Own_Tesla = 'Y' THEN 'Tesla' END,
        CASE WHEN e_cv.Auto_Seg_Own_Porsche = 'Y' THEN 'Porsche' END,
        CASE WHEN e_cv.Auto_Seg_Own_Land_Rover = 'Y' THEN 'Land Rover' END
    ) AS VEHICLE_MAKES,

    -- Vehicle styles
    CONCAT_WS(',',
        CASE WHEN e_cv.Auto_Seg_Own_Car = 'Y' THEN 'Car' END,
        CASE WHEN e_cv.Auto_Seg_Own_Suv_Cuv = 'Y' THEN 'SUV/CUV' END,
        CASE WHEN e_cv.Auto_Seg_Own_Suv = 'Y' THEN 'SUV' END,
        CASE WHEN e_cv.Auto_Seg_Own_Cuv = 'Y' THEN 'CUV' END,
        CASE WHEN e_cv.Auto_Seg_Own_Truck = 'Y' THEN 'Truck' END,
        CASE WHEN e_cv.Auto_Seg_Own_Van_Minivan = 'Y' THEN 'Van/Minivan' END,
        CASE WHEN e_cv.Auto_Seg_Own_Minivan = 'Y' THEN 'Minivan' END,
        CASE WHEN e_cv.Auto_Seg_Own_Sports_Car = 'Y' THEN 'Sports Car' END
    ) AS VEHICLE_STYLES,

    -- Vehicle class (luxury, etc.)
    CONCAT_WS(',',
        CASE WHEN e_cv.Auto_Seg_Own_Luxury_Car = 'Y' THEN 'Luxury Car' END,
        CASE WHEN e_cv.Auto_Seg_Own_Luxury_Suv = 'Y' THEN 'Luxury SUV' END,
        CASE WHEN e_cv.Auto_Seg_Own_Luxury_Cuv = 'Y' THEN 'Luxury CUV' END,
        CASE WHEN e_cv.Auto_Seg_Own_Midsize_Car = 'Y' THEN 'Midsize Car' END,
        CASE WHEN e_cv.Auto_Seg_Own_Comp_Car = 'Y' THEN 'Compact Car' END,
        CASE WHEN e_cv.Auto_Seg_Own_Full_Size_Car = 'Y' THEN 'Full Size Car' END,
        CASE WHEN e_cv.Auto_Seg_Own_Full_Size_Truck = 'Y' THEN 'Full Size Truck' END,
        CASE WHEN e_cv.Auto_Seg_Own_Full_Size_Suv = 'Y' THEN 'Full Size SUV' END
    ) AS VEHICLE_CLASS,

    -- Fuel type
    CONCAT_WS(',',
        CASE WHEN e_cv.Own_Electric_Y = 'Y' THEN 'Electric' END,
        CASE WHEN e_cv.Own_Hybrid_Y = 'Y' THEN 'Hybrid' END,
        CASE WHEN e_cv.Auto_Seg_Own_Alt_Fuel_Car = 'Y' THEN 'Alt Fuel' END
    ) AS FUEL_CODE,

    -- ============================================================
    -- FINANCIAL DATA
    -- ============================================================

    -- Financial health
    e_cv.Fin_Ability_to_Pay AS FINANCIAL_HEALTH_BUCKET,

    -- Credit card info
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_Financial_Credit_Card_User` = 'Y' THEN 'Credit Card User' END,
        CASE WHEN e_cv.`RC_Financial_Premium_Credit_Card_User` = 'Y' THEN 'Premium Card' END,
        CASE WHEN e_cv.`RC_Financial_Corporate_Credit_Card_User` = 'Y' THEN 'Corporate Card' END,
        CASE WHEN e_cv.`RC_Financial_Debit_Card_User` = 'Y' THEN 'Debit Card' END,
        CASE WHEN e_cv.`RC_Financial_Store_Credit_Card_User` = 'Y' THEN 'Store Card' END,
        CASE WHEN e_cv.`RC_Financial_Major_Credit_Card_User` = 'Y' THEN 'Major Card' END
    ) AS CREDIT_CARD_INFO,

    -- Investment types
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_Invest_Active_Investor` = 'Y' THEN 'Active Investor' END,
        CASE WHEN e_cv.`RC_Invest_Brokerage_Account_Owner` = 'Y' THEN 'Brokerage Account' END,
        CASE WHEN e_cv.`RC_Invest_Mutual_Fund_Investor` = 'Y' THEN 'Mutual Funds' END,
        CASE WHEN e_cv.`RC_InvestHave_Retirement_Plan` = 'Y' THEN 'Retirement Plan' END,
        CASE WHEN e_cv.`RC_InvestOnline_Trading` = 'Y' THEN 'Online Trading' END
    ) AS INVESTMENT_TYPE,

    -- Processing Metadata
    CURRENT_TIMESTAMP() AS DBT_UPDATED_AT

FROM one_luid_per_household olh
LEFT JOIN {{ source('locality_poc_share_silver', 'experian_consumerview2') }} e_cv
    ON olh.luid = e_cv.recd_luid
