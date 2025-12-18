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

    CASE
        WHEN e_cv.`PDM_Gender_Male` = 'Y' AND COALESCE(e_cv.`PDM_Gender_Female`, '') != 'Y' THEN 'M'
        WHEN e_cv.`PDM_Gender_Female` = 'Y' AND COALESCE(e_cv.`PDM_Gender_Male`, '') != 'Y' THEN 'F'
        WHEN e_cv.`Person_RC_gndr_gndr_2` = 'M' THEN 'M'
        WHEN e_cv.`Person_RC_gndr_gndr_2` = 'F' THEN 'F'
        ELSE NULL
    END AS GENDER,

    -- AGE: Use Age_Range_* boolean flags, return midpoint of range
    -- IMPORTANT: Ordered from most specific to least specific ranges to handle
    -- Experian's overlapping age range flags (34% of records have multiple flags set)
    -- See: Age_Range_1924 (19-24) overlaps with Age_Range_2124 (21-24)
    -- See: Age_Range_75 (75+) overlaps with Age_Range_7579, 8084, etc.
    CASE
        WHEN e_cv.Age_Range_9599 = 'Y' THEN 97
        WHEN e_cv.Age_Range_9094 = 'Y' THEN 92
        WHEN e_cv.Age_Range_8589 = 'Y' THEN 87
        WHEN e_cv.Age_Range_8084 = 'Y' THEN 82
        WHEN e_cv.Age_Range_7579 = 'Y' THEN 77
        WHEN e_cv.Age_Range_7074 = 'Y' THEN 72
        WHEN e_cv.Age_Range_6569 = 'Y' THEN 67
        WHEN e_cv.Age_Range_5559 = 'Y' THEN 57
        WHEN e_cv.Age_Range_5054 = 'Y' THEN 52
        WHEN e_cv.Age_Range_4549 = 'Y' THEN 47
        WHEN e_cv.Age_Range_4044 = 'Y' THEN 42
        WHEN e_cv.Age_Range_3539 = 'Y' THEN 37
        WHEN e_cv.Age_Range_3034 = 'Y' THEN 32
        WHEN e_cv.Age_Range_2529 = 'Y' THEN 27
        WHEN e_cv.Age_Range_1820 = 'Y' THEN 19
        WHEN e_cv.Age_Range_2124 = 'Y' THEN 23
        WHEN e_cv.Age_Range_1924 = 'Y' THEN 22
        WHEN e_cv.Age_Range_75 = 'Y' THEN 80
        ELSE NULL
    END AS AGE,

    -- AGE_BUCKET: Map to Horizon buckets (1=18-24, 2=25-34, 3=35-44, 4=45-54, 5=55-64, 6=65-74, 7=75+)
    -- IMPORTANT: Same ordering logic as AGE - specific ranges before broad catch-alls
    CASE
        WHEN e_cv.Age_Range_9599 = 'Y' THEN 7
        WHEN e_cv.Age_Range_9094 = 'Y' THEN 7
        WHEN e_cv.Age_Range_8589 = 'Y' THEN 7
        WHEN e_cv.Age_Range_8084 = 'Y' THEN 7
        WHEN e_cv.Age_Range_7579 = 'Y' THEN 7
        WHEN e_cv.Age_Range_7074 = 'Y' THEN 6
        WHEN e_cv.Age_Range_6569 = 'Y' THEN 6
        WHEN e_cv.Age_Range_5559 = 'Y' THEN 5
        WHEN e_cv.Age_Range_5054 = 'Y' THEN 4
        WHEN e_cv.Age_Range_4549 = 'Y' THEN 4
        WHEN e_cv.Age_Range_4044 = 'Y' THEN 3
        WHEN e_cv.Age_Range_3539 = 'Y' THEN 3
        WHEN e_cv.Age_Range_3034 = 'Y' THEN 2
        WHEN e_cv.Age_Range_2529 = 'Y' THEN 2
        WHEN e_cv.Age_Range_1820 = 'Y' THEN 1
        WHEN e_cv.Age_Range_2124 = 'Y' THEN 1
        WHEN e_cv.Age_Range_1924 = 'Y' THEN 1
        WHEN e_cv.Age_Range_75 = 'Y' THEN 7
        ELSE NULL
    END AS AGE_BUCKET,

    -- ETHNICITY: Decode A-O codes to human-readable
    CASE COALESCE(e_cv.`Person__RC_Ethnic_-_Group_1`, e_cv.`Person__RC_Ethnic_-_Group_2`)
        WHEN 'A' THEN 'African American'
        WHEN 'B' THEN 'Southeast Asian'
        WHEN 'C' THEN 'South Asian'
        WHEN 'D' THEN 'Central Asian'
        WHEN 'E' THEN 'Mediterranean'
        WHEN 'F' THEN 'Native American'
        WHEN 'G' THEN 'Scandinavian'
        WHEN 'H' THEN 'Polynesian'
        WHEN 'I' THEN 'Middle Eastern'
        WHEN 'J' THEN 'Jewish'
        WHEN 'K' THEN 'Western European'
        WHEN 'L' THEN 'Eastern European'
        WHEN 'M' THEN 'Caribbean Non-Hispanic'
        WHEN 'N' THEN 'East Asian'
        WHEN 'O' THEN 'Hispanic'
        ELSE NULL
    END AS ETHNICITY,

    -- EDUCATION_LEVEL: PDM (Primary Decision Maker) education flags
    CASE
        WHEN e_cv.`PDM_Education_04_Grad_Degree` = 'Y' THEN 'Graduate'
        WHEN e_cv.`PDM_Education_03_Bach_Degree` = 'Y' THEN 'College'
        WHEN e_cv.`PDM_Education_02_Some_College` = 'Y' THEN 'Some College'
        WHEN e_cv.`PDM_Education_01_High_School_Diploma` = 'Y' THEN 'High School'
        WHEN e_cv.`PDM_Education_05_Less_Than_HS` = 'Y' THEN 'High School'
        ELSE NULL
    END AS EDUCATION_LEVEL,

    -- MARITAL_STATUS: PDM (Primary Decision Maker) flags
    CASE
        WHEN e_cv.`PDM_Married` = 'Y' THEN 'Married'
        WHEN e_cv.`PDM_Single` = 'Y' THEN 'Single'
        ELSE NULL
    END AS MARITAL_STATUS,

    -- GEOGRAPHIC DATA
    e_cv.stat_abbr AS STATE,
    CAST(NULL AS STRING) AS ZIP11,
    CAST(NULL AS STRING) AS COUNTY_NAME,

    -- OCCUPATION: Decode A-I codes to human-readable
    CASE COALESCE(e_cv.`Person__RC_Person__Occupation_1`, e_cv.`Person__RC_Person__Occupation_2`)
        WHEN 'A' THEN 'Management'
        WHEN 'B' THEN 'Technical'
        WHEN 'C' THEN 'Professional'
        WHEN 'D' THEN 'Sales'
        WHEN 'E' THEN 'Office Administration'
        WHEN 'F' THEN 'Blue Collar'
        WHEN 'G' THEN 'Farmer'
        WHEN 'H' THEN 'Other'
        WHEN 'I' THEN 'Retired'
        ELSE NULL
    END AS OCCUPATION,

    -- OCCUPATION_TITLE: Decode A-H codes to human-readable
    CASE e_cv.`Person_RC_Person_Title_1`
        WHEN 'A' THEN 'Chief Level Executive'
        WHEN 'B' THEN 'Executive/Management'
        WHEN 'C' THEN 'Finance'
        WHEN 'D' THEN 'IT/Technical'
        WHEN 'E' THEN 'Marketing'
        WHEN 'F' THEN 'Owner'
        WHEN 'G' THEN 'Professional/Sales'
        WHEN 'H' THEN 'Other Business Exec'
        ELSE NULL
    END AS OCCUPATION_TITLE,

    -- ============================================================
    -- HOUSEHOLD DATA
    -- ============================================================

    -- Home ownership: A=Homeowner, B=Renter
    e_cv.`RC_Homeowner_Combined_HomeownerRenter` AS HOME_OWNERSHIP_CODE,
    CASE e_cv.`RC_Homeowner_Combined_HomeownerRenter`
        WHEN 'A' THEN 'Homeowner'
        WHEN 'B' THEN 'Renter'
        ELSE NULL
    END AS HOME_OWNERSHIP,

    -- Income range code (A-J)
    e_cv.`RC_Est_Household_Income_V6` AS INCOME_RANGE,

    -- INCOME: Convert Experian letter codes to numeric midpoints
    -- A=$1K-25K, B=$25K-50K, C=$50K-75K, D=$75K-100K, E=$100K-125K,
    -- F=$125K-150K, G=$150K-175K, H=$175K-200K, I=$200K-250K, J=$250K+
    CASE e_cv.`RC_Est_Household_Income_V6`
        WHEN 'A' THEN 12500
        WHEN 'B' THEN 37500
        WHEN 'C' THEN 62500
        WHEN 'D' THEN 87500
        WHEN 'E' THEN 112500
        WHEN 'F' THEN 137500
        WHEN 'G' THEN 162500
        WHEN 'H' THEN 187500
        WHEN 'I' THEN 225000
        WHEN 'J' THEN 300000
        ELSE NULL
    END AS INCOME,

    -- INCOME_BUCKET: Experian A-J mapped to buckets 1-10 (1=lowest, 10=highest)
    CASE e_cv.`RC_Est_Household_Income_V6`
        WHEN 'A' THEN 1
        WHEN 'B' THEN 2
        WHEN 'C' THEN 3
        WHEN 'D' THEN 4
        WHEN 'E' THEN 5
        WHEN 'F' THEN 6
        WHEN 'G' THEN 7
        WHEN 'H' THEN 8
        WHEN 'I' THEN 9
        WHEN 'J' THEN 10
        ELSE NULL
    END AS INCOME_BUCKET,

    -- Net worth from CFI score (Experian A-K, where A=highest >$5M, K=lowest <$25K)
    e_cv.`CFINet_Asset_Score` AS NET_WORTH_RANGE,

    -- NET_WORTH: Convert Experian letter codes to numeric midpoints
    CASE e_cv.`CFINet_Asset_Score`
        WHEN 'A' THEN 7500000   -- >$5M
        WHEN 'B' THEN 3750000   -- $2.5-5M
        WHEN 'C' THEN 1750000   -- $1-2.5M
        WHEN 'D' THEN 875000    -- $750K-1M
        WHEN 'E' THEN 625000    -- $500-750K
        WHEN 'F' THEN 375000    -- $250-500K
        WHEN 'G' THEN 175000    -- $100-250K
        WHEN 'H' THEN 87500     -- $75-100K
        WHEN 'I' THEN 62500     -- $50-75K
        WHEN 'J' THEN 37500     -- $25-50K
        WHEN 'K' THEN 12500     -- <$25K
        ELSE NULL
    END AS NET_WORTH,

    -- NET_WORTH_BUCKET: Map Experian A-K (inverted) to Horizon A-I format
    -- Horizon expects: A=<$1, B=$1-5K, C=$5-10K, D=$10-25K, E=$25-50K, F=$50-100K, G=$100-250K, H=$250-500K, I=>$500K
    -- Experian provides: A=>$5M, B=$2.5-5M, C=$1-2.5M, D=$750K-1M, E=$500-750K, F=$250-500K, G=$100-250K, H=$75-100K, I=$50-75K, J=$25-50K, K=<$25K
    -- Note: Experian's lowest bucket (<$25K) maps to Horizon D; Horizon A-C (very low net worth) have no Experian equivalent
    CASE e_cv.`CFINet_Asset_Score`
        WHEN 'A' THEN 'I'  -- >$5M → >$500K
        WHEN 'B' THEN 'I'  -- $2.5-5M → >$500K
        WHEN 'C' THEN 'I'  -- $1-2.5M → >$500K
        WHEN 'D' THEN 'I'  -- $750K-1M → >$500K
        WHEN 'E' THEN 'I'  -- $500-750K → >$500K
        WHEN 'F' THEN 'H'  -- $250-500K → $250-500K
        WHEN 'G' THEN 'G'  -- $100-250K → $100-250K
        WHEN 'H' THEN 'F'  -- $75-100K → $50-100K
        WHEN 'I' THEN 'F'  -- $50-75K → $50-100K
        WHEN 'J' THEN 'E'  -- $25-50K → $25-50K
        WHEN 'K' THEN 'D'  -- <$25K → $10-25K
        ELSE NULL
    END AS NET_WORTH_BUCKET,

    -- Home value
    e_cv.`RC_Estimated_Home_Value_range_` AS HOME_VALUE_RANGE,

    -- Household size: DU_size is "Dwelling Unit Size" (building type), NOT number of people
    -- No direct household size column available in ConsumerView2
    CAST(NULL AS STRING) AS NUM_PEOPLE_IN_HOUSEHOLD_GROUP,

    -- ============================================================
    -- CHILDREN DATA
    -- Note: mom_* columns use 'A' for yes, not 'Y'
    -- ============================================================

    -- Child age groups (comma-separated)
    CONCAT_WS(',',
        CASE WHEN e_cv.`mom_0-3yrs_hh` = 'A' THEN '0-3' END,
        CASE WHEN e_cv.`mom_4-6yrs_hh` = 'A' THEN '4-6' END,
        CASE WHEN e_cv.mom_tween_hh = 'A' THEN 'Tween' END,
        CASE WHEN e_cv.mom_teen_hh = 'A' THEN 'Teen' END
    ) AS CHILD_AGE_GROUP,

    -- Number of children
    CASE
        WHEN e_cv.mom_2child_hh = 'A' THEN '2+'
        WHEN e_cv.mom_1chld_hh = 'A' THEN '1'
        ELSE NULL
    END AS NUMBER_OF_CHILDREN,

    -- Presence of children
    CASE
        WHEN e_cv.`mom_0-3yrs_hh` = 'A' OR e_cv.`mom_4-6yrs_hh` = 'A' OR e_cv.mom_tween_hh = 'A' OR e_cv.mom_teen_hh = 'A' OR e_cv.mom_1chld_hh = 'A' OR e_cv.mom_2child_hh = 'A' THEN 1
        ELSE 0
    END AS PRESENCE_OF_CHILDREN,

    -- ============================================================
    -- INTERESTS (comma-separated aggregations)
    -- ============================================================

    -- General interests (RC_ActInt_* and RC_Hobbies_* use 'A' for yes)
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_ActInt_Arts_and_Crafts` = 'A' THEN 'Arts & Crafts' END,
        CASE WHEN e_cv.`RC_ActInt_Audio_Book_Listener` = 'A' THEN 'Audio Books' END,
        CASE WHEN e_cv.`RC_ActInt_Book_Reader` = 'A' THEN 'Reading' END,
        CASE WHEN e_cv.`RC_ActInt_Cat_Owners` = 'A' THEN 'Cat Owner' END,
        CASE WHEN e_cv.`RC_ActInt_Coffee_Connoisseurs` = 'A' THEN 'Coffee' END,
        CASE WHEN e_cv.`RC_ActInt_Cultural_Arts` = 'A' THEN 'Cultural Arts' END,
        CASE WHEN e_cv.`RC_ActInt_Dog_Owners` = 'A' THEN 'Dog Owner' END,
        CASE WHEN e_cv.`RC_ActInt_Do-it-yourselfers` = 'A' THEN 'DIY' END,
        CASE WHEN e_cv.`RC_ActInt_E-Book_Reader` = 'A' THEN 'E-Books' END,
        CASE WHEN e_cv.`RC_ActInt_Fitness_Enthusiast` = 'A' THEN 'Fitness' END,
        CASE WHEN e_cv.`RC_ActInt_Gourmet_Cooking` = 'A' THEN 'Gourmet Cooking' END,
        CASE WHEN e_cv.`RC_ActInt_Healthy_Living` = 'A' THEN 'Healthy Living' END,
        CASE WHEN e_cv.`RC_ActInt_Home_Improvement_Spenders` = 'A' THEN 'Home Improvement' END,
        CASE WHEN e_cv.`RC_ActInt_Music_Download` = 'A' THEN 'Music Download' END,
        CASE WHEN e_cv.`RC_ActInt_Music_Streaming` = 'A' THEN 'Music Streaming' END,
        CASE WHEN e_cv.`RC_ActInt_Outdoor_Enthusiast` = 'A' THEN 'Outdoors' END,
        CASE WHEN e_cv.`RC_ActInt_Pet_Enthusiast` = 'A' THEN 'Pets' END,
        CASE WHEN e_cv.`RC_ActInt_Photography` = 'A' THEN 'Photography' END,
        CASE WHEN e_cv.`RC_ActIntVideo_Gamer` = 'A' THEN 'Video Games' END,
        CASE WHEN e_cv.`RC_ActInt_Wine_Lovers` = 'A' THEN 'Wine' END,
        CASE WHEN e_cv.`RC_Hobbies_Gardening` = 'A' THEN 'Gardening' END
    ) AS GENERAL_INTERESTS,

    -- Sports interests
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_ActInt_Avid_Runners` = 'A' THEN 'Running' END,
        CASE WHEN e_cv.`RC_ActInt_Boating` = 'A' THEN 'Boating' END,
        CASE WHEN e_cv.`RC_ActInt_Fishing` = 'A' THEN 'Fishing' END,
        CASE WHEN e_cv.`RC_ActInt_Hunting_Enthusiasts` = 'A' THEN 'Hunting' END,
        CASE WHEN e_cv.`RC_ActIntMLB_Enthusiast` = 'A' THEN 'MLB' END,
        CASE WHEN e_cv.`RC_ActIntNASCAR_Enthusiast` = 'A' THEN 'NASCAR' END,
        CASE WHEN e_cv.`RC_ActIntNBA_Enthusiast` = 'A' THEN 'NBA' END,
        CASE WHEN e_cv.`RC_ActIntNFL_Enthusiast` = 'A' THEN 'NFL' END,
        CASE WHEN e_cv.`RC_ActIntNHL_Enthusiast` = 'A' THEN 'NHL' END,
        CASE WHEN e_cv.`RC_ActIntPGA_Tour_Enthusiast` = 'A' THEN 'Golf/PGA' END,
        CASE WHEN e_cv.`RC_ActIntPlay_Golf` = 'A' THEN 'Golf' END,
        CASE WHEN e_cv.`RC_ActInt_Plays_Hockey` = 'A' THEN 'Hockey' END,
        CASE WHEN e_cv.`RC_ActInt_Plays_Soccer` = 'A' THEN 'Soccer' END,
        CASE WHEN e_cv.`RC_ActInt_Plays_Tennis` = 'A' THEN 'Tennis' END,
        CASE WHEN e_cv.`RC_ActInt_Snow_Sports` = 'A' THEN 'Snow Sports' END,
        CASE WHEN e_cv.`RC_ActInt_Sports_Enthusiast` = 'A' THEN 'Sports General' END,
        CASE WHEN e_cv.`RC_ActIntCanoeingKayaking` = 'A' THEN 'Canoeing/Kayaking' END
    ) AS SPORTS_INTERESTS,

    -- Reading interests
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_ActInt_Book_Reader` = 'A' THEN 'Books' END,
        CASE WHEN e_cv.`RC_ActInt_E-Book_Reader` = 'A' THEN 'E-Books' END,
        CASE WHEN e_cv.`RC_ActInt_Audio_Book_Listener` = 'A' THEN 'Audio Books' END,
        CASE WHEN e_cv.`RC_ActInt_Digital_MagazineNewspapers_Buyers` = 'A' THEN 'Digital Magazines' END
    ) AS READING_INTERESTS,

    -- Travel interests (RC_Lifestyle_* use 'A' for yes)
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_Lifestyle_High_Frequency_Business_Traveler` = 'A' THEN 'Business Travel' END,
        CASE WHEN e_cv.`RC_Lifestyle_High_Frequency_Cruise_Enthusiast` = 'A' THEN 'Cruises' END,
        CASE WHEN e_cv.`RC_Lifestyle_High_Frequency_Domestic_Vacationer` = 'A' THEN 'Domestic Vacation' END,
        CASE WHEN e_cv.`RC_Lifestyle_High_Frequency_Foreign_Vacationer` = 'A' THEN 'Foreign Vacation' END,
        CASE WHEN e_cv.`RC_Lifestyle_Frequent_Flyer_Program_Member` = 'A' THEN 'Frequent Flyer' END,
        CASE WHEN e_cv.`RC_Lifestyle_Hotel_Guest_Loyalty_Program` = 'A' THEN 'Hotel Loyalty' END,
        CASE WHEN e_cv.`RC_ActInt_Amusement_Park_Visitors` = 'A' THEN 'Amusement Parks' END,
        CASE WHEN e_cv.`RC_ActInt_Zoo_Visitors` = 'A' THEN 'Zoos' END
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

    -- Credit card info (RC_Financial_* use 'A' for yes)
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_Financial_Credit_Card_User` = 'A' THEN 'Credit Card User' END,
        CASE WHEN e_cv.`RC_Financial_Premium_Credit_Card_User` = 'A' THEN 'Premium Card' END,
        CASE WHEN e_cv.`RC_Financial_Corporate_Credit_Card_User` = 'A' THEN 'Corporate Card' END,
        CASE WHEN e_cv.`RC_Financial_Debit_Card_User` = 'A' THEN 'Debit Card' END,
        CASE WHEN e_cv.`RC_Financial_Store_Credit_Card_User` = 'A' THEN 'Store Card' END,
        CASE WHEN e_cv.`RC_Financial_Major_Credit_Card_User` = 'A' THEN 'Major Card' END
    ) AS CREDIT_CARD_INFO,

    -- Investment types (RC_Invest_* use 'A' for yes)
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_Invest_Active_Investor` = 'A' THEN 'Active Investor' END,
        CASE WHEN e_cv.`RC_Invest_Brokerage_Account_Owner` = 'A' THEN 'Brokerage Account' END,
        CASE WHEN e_cv.`RC_Invest_Mutual_Fund_Investor` = 'A' THEN 'Mutual Funds' END,
        CASE WHEN e_cv.`RC_InvestHave_Retirement_Plan` = 'A' THEN 'Retirement Plan' END,
        CASE WHEN e_cv.`RC_InvestOnline_Trading` = 'A' THEN 'Online Trading' END
    ) AS INVESTMENT_TYPE,

    -- Processing Metadata
    CURRENT_TIMESTAMP() AS DBT_UPDATED_AT

FROM one_luid_per_household olh
LEFT JOIN {{ source('locality_poc_share_silver', 'experian_consumerview2') }} e_cv
    ON olh.luid = e_cv.recd_luid
