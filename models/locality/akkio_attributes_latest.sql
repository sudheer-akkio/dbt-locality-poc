{{
    config(
        materialized='table'
    )
}}

-- Experian attributes with derived categorical columns for common query patterns
-- Keeps ALL 528 original boolean columns for detailed segmentation

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
    olh.hh_id AS AKKIO_HH_ID,  -- Same as AKKIO_ID for household-level data
    olh.luid AS LUID,

    -- Temporal
    CURRENT_DATE() AS PARTITION_DATE,

    -- ============================================================
    -- INDIVIDUAL DEMOGRAPHICS (placeholders - not available in Experian Consumerview)
    -- ============================================================

    CAST(NULL AS STRING) AS GENDER,
    CAST(NULL AS INT) AS AGE,
    CAST(NULL AS INT) AS AGE_BUCKET,
    CAST(NULL AS STRING) AS ETHNICITY,
    CAST(NULL AS STRING) AS EDUCATION_LEVEL,
    CAST(NULL AS STRING) AS MARITAL_STATUS,
    CAST(NULL AS STRING) AS STATE,
    CAST(NULL AS STRING) AS ZIP11,

    -- ============================================================
    -- DERIVED CATEGORICAL COLUMNS (following Horizon standards)
    -- ============================================================

    -- HOUSING TYPE (from 13 mutually exclusive flags → categorical)
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
        ELSE NULL
    END AS HOUSING_TYPE,

    -- HOME OWNERSHIP (Owner/Renter)
    CASE
        WHEN e_cv.young_family_homeowners
            OR e_cv.growing_family_homeowners
            OR e_cv.second_homeowners
            OR e_cv.millennial_homeowners
        THEN 'Owner'
        WHEN e_cv.luxury_apt_renters
            OR e_cv.high_rise_apt_renters
            OR e_cv.pet_friendly_apt_renters
            OR e_cv.young_profess_apt_renters
            OR e_cv.family_focus_apt_renters
            OR e_cv.fitness_apt_renters
            OR e_cv.outdoor_loving_apt_renters
            OR e_cv.urban_apt_renters
        THEN 'Renter'
        ELSE NULL
    END AS HOME_OWNERSHIP,

    -- INCOME (from 7 mutually exclusive flags → numeric + categorical)
    CASE
        WHEN e_cv.rc_ehi_amount_2_5Mplus THEN 2500
        WHEN e_cv.rc_ehi_amount_2M_2_5_M THEN 2000
        WHEN e_cv.rc_ehi_amount_1_5M_2M THEN 1500
        WHEN e_cv.rc_ehi_amount_1M_1_5M THEN 1000
        WHEN e_cv.rc_ehi_amount_750K_1M THEN 750
        WHEN e_cv.rc_ehi_amount_500K_750K THEN 500
        WHEN e_cv.rc_ehi_amount_250K_500K THEN 250
        ELSE NULL
    END AS INCOME,

    CASE
        WHEN e_cv.rc_ehi_amount_2_5Mplus THEN '2500000+'
        WHEN e_cv.rc_ehi_amount_2M_2_5_M THEN '2000000-2499999'
        WHEN e_cv.rc_ehi_amount_1_5M_2M THEN '1500000-1999999'
        WHEN e_cv.rc_ehi_amount_1M_1_5M THEN '1000000-1499999'
        WHEN e_cv.rc_ehi_amount_750K_1M THEN '750000-999999'
        WHEN e_cv.rc_ehi_amount_500K_750K THEN '500000-749999'
        WHEN e_cv.rc_ehi_amount_250K_500K THEN '250000-499999'
        ELSE NULL
    END AS INCOME_RANGE,

    -- NET WORTH (from 9 mutually exclusive flags → numeric + categorical)
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
    END AS NET_WORTH,

    CASE
        WHEN e_cv.hh_net_worth_50M_plus THEN '50000000+'
        WHEN e_cv.hh_net_worth_20M_50M THEN '20000000-49999999'
        WHEN e_cv.hh_net_worth_10M_20M THEN '10000000-19999999'
        WHEN e_cv.hh_net_worth_5M_10M THEN '5000000-9999999'
        WHEN e_cv.hh_net_worth_2M_5M THEN '2000000-4999999'
        WHEN e_cv.hh_net_worth_1M_2M THEN '1000000-1999999'
        WHEN e_cv.hh_net_worth_500K_1M THEN '500000-999999'
        WHEN e_cv.hh_net_worth_100K_500K THEN '100000-499999'
        WHEN e_cv.hh_net_worth_under100K THEN '0-99999'
        ELSE NULL
    END AS NET_WORTH_RANGE,

    -- HOME SALE PROPENSITY (from 8 mutually exclusive flags → categorical)
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

    -- LUXURY VEHICLE OWNERSHIP (aggregate from luxury car flags)
    CASE WHEN (
        e_cv.own_aud_aston_martin OR
        e_cv.own_aud_bentley OR
        e_cv.own_aud_ferrari OR
        e_cv.own_aud_lamborghini OR
        e_cv.own_aud_maserati OR
        e_cv.itm_aud_aston_martin OR
        e_cv.itm_aud_bentley OR
        e_cv.itm_aud_lamborghini OR
        e_cv.itm_aud_maserati
    ) THEN 'Ultra-Luxury'
    WHEN (
        e_cv.itm_aud_mercedes_s_class OR
        e_cv.own_aud_mercedes_amg_gt OR
        e_cv.own_aud_mercedes_sl_convertible OR
        e_cv.own_aud_mercedes_g_class
    ) THEN 'Luxury'
    ELSE NULL
    END AS LUXURY_VEHICLE_SEGMENT,

    -- ELECTRIC VEHICLE OWNERSHIP (aggregate from EV flags)
    CASE WHEN (
        e_cv.own_aud_tesla_cybertruck_ev OR
        e_cv.own_aud_rivian_r1t_ev OR
        e_cv.own_aud_ford_f150_lightning_ev OR
        e_cv.own_aud_electric_truck
    ) THEN 'Electric Truck'
    WHEN (
        e_cv.own_aud_ev_lucid OR
        e_cv.own_aud_ev_polestar OR
        e_cv.own_aud_ev_rivian OR
        e_cv.own_aud_ev_porsche_taycan OR
        e_cv.own_aud_ev_bmw_i7 OR
        e_cv.own_aud_ev_lucid_air OR
        e_cv.own_aud_ev_audi_etron_gt
    ) THEN 'Luxury EV'
    WHEN (
        e_cv.own_aud_hyundai_ioniq_5_ev OR
        e_cv.own_aud_hyundai_ioniq_6_ev OR
        e_cv.own_aud_nissan_ariya_ev OR
        e_cv.own_aud_volkswagen_id4_ev OR
        e_cv.own_aud_kia_ev6 OR
        e_cv.own_aud_kia_ev9 OR
        e_cv.own_aud_chevrolet_blazer_ev OR
        e_cv.own_aud_chevrolet_equinox_ev
    ) THEN 'Mainstream EV'
    ELSE NULL
    END AS EV_OWNERSHIP_SEGMENT,

    -- EMPLOYMENT TYPE (aggregate from employment flags)
    CASE
        WHEN e_cv.exec_decision_maker OR e_cv.company_founder THEN 'Executive/Founder'
        WHEN e_cv.employ_public_traded_co THEN 'Public Company Employee'
        WHEN e_cv.employ_private_held_co THEN 'Private Company Employee'
        ELSE NULL
    END AS EMPLOYMENT_TYPE,

    -- ============================================================
    -- MULTI-SELECT FLAGS (keep as-is - households can have multiple)
    -- ============================================================

    -- WEALTH & CAREER INDICATORS
    e_cv.high_wealth_5m_plus,
    e_cv.receive_high_value_stock,
    e_cv.recent_promoted_12mo,
    e_cv.former_expatriate,

    -- HIGH SPEND CATEGORIES (~150 flags - keep all)
    e_cv.rc_airline_travel_high_spend,
    e_cv.rc_art_deal_gall_high_spend,
    e_cv.rc_artisit_supply_high_spend,
    e_cv.rc_auto_lease_repair_high_spend,
    e_cv.rc_auto_rental_high_spend,
    e_cv.rc_auto_body_repair_high_spend,
    e_cv.rc_auto_parts_access_high_spend,
    e_cv.rc_auto_service_high_spend,
    e_cv.rc_auto_tire_high_spend,
    e_cv.rc_bakeries_high_spend,
    e_cv.rc_band_orch_high_spend,
    e_cv.rc_barber_salon_high_spend,
    e_cv.rc_bicycle_shop_service_high_spend,
    e_cv.rc_book_store_high_spend,
    e_cv.rc_road_fee_tolls_high_spend,
    e_cv.rc_building_lumber_high_spend,
    e_cv.rc_camera_photo_high_spend,
    e_cv.rc_candy_nut_high_spend,
    e_cv.rc_car_rentals_high_spend,
    e_cv.rc_car_washes_high_spend,
    e_cv.rc_caterers_high_spend,
    e_cv.rc_clock_watch_silver_high_spend,
    e_cv.rc_country_club_member_high_spend,
    e_cv.rc_commercial_equip_high_spend,
    e_cv.rc_commercial_footwear_high_spend,
    e_cv.rc_computer_software_high_spend,
    e_cv.rc_construction_material_high_spend,
    e_cv.rc_cosmetic_store_high_spend,
    e_cv.rc_courier_freight_high_spend,
    e_cv.rc_cruise_steamship_high_spend,
    e_cv.rc_dating_service_high_spend,
    e_cv.rc_department_store_high_spend,
    e_cv.rc_digital_audiovisual_media_high_spend,
    e_cv.rc_digital_games_high_spend,
    e_cv.rc_digital_multicategory_high_spend,
    e_cv.rc_digital_sofware_apps_high_spend,
    e_cv.rc_direct_marketing_subscription_high_spend,
    e_cv.rc_direct_marketing_insurance_high_spend,
    e_cv.rc_discount_store_high_spend,
    e_cv.rc_drapery_window_high_spend,
    e_cv.rc_eating_restaurants_high_spend,
    e_cv.rc_electric_vehicle_charging_high_spend,
    e_cv.rc_electronic_sales_high_spend,
    e_cv.rc_equip_home_furn_store_high_spend,
    e_cv.rc_equip_tool_rental_high_spend,
    e_cv.rc_family_clothing_store_high_spend,
    e_cv.rc_fast_food_rest_high_spend,
    e_cv.rc_floor_covering_high_spend,
    e_cv.rc_florists_high_spend,
    e_cv.rc_trucking_moving_high_spend,
    e_cv.rc_fuel_dispenser_high_spend,
    e_cv.rc_game_toy_hobby_shop_high_spend,
    e_cv.rc_novelty_souvenir_high_spend,
    e_cv.rc_glass_paint_wallpaper_high_spend,
    e_cv.rc_grocery_supermarket_high_spend,
    e_cv.rc_hardware_equip_supplies_high_spend,
    e_cv.rc_hardware_stores_high_spend,
    e_cv.rc_health_beauty_spa_high_spend,
    e_cv.rc_home_supply_warehouse_high_spend,
    e_cv.rc_hotels_high_spend,
    e_cv.rc_lawn_garden_supply_high_spend,
    e_cv.rc_leather_luggage_store_high_spend,
    e_cv.rc_hotel_motel_resorts_high_spend,
    e_cv.rc_men_boy_clothing_store_high_spend,
    e_cv.rc_men_women_clothing_store_high_spend,
    e_cv.rc_men_women_child_uniform_high_spend,
    e_cv.rc_convenience_vending_high_spend,
    e_cv.rc_auto_aircraft_farm_equip_high_spend,
    e_cv.rc_misc_specialty_retail_high_spend,
    e_cv.rc_movie_theater_high_spend,
    e_cv.rc_motor_vehicle_supplies_high_spend,
    e_cv.rc_motorcycle_shop_high_spend,
    e_cv.rc_music_stores_high_spend,
    e_cv.rc_office_school_supply_high_spend,
    e_cv.rc_office_comm_furn_high_spend,
    e_cv.rc_charitable_organization_high_spend,
    e_cv.rc_not_classified_org_member_high_spend,
    e_cv.rc_organizations_political_high_spend,
    e_cv.rc_beer_wine_liquor_store_high_spend,
    e_cv.rc_railway_high_spend,
    e_cv.rc_pet_shop_supplies_high_spend,
    e_cv.rc_stones_watches_jewelry_high_spend,
    e_cv.rc_real_estate_agent_rental_high_spend,
    e_cv.rc_rec_sport_camps_high_spend,
    e_cv.rc_trucks_trailers_rental_high_spend,
    e_cv.rc_second_hand_store_high_spend,
    e_cv.rc_shoe_stores_high_spend,
    e_cv.rc_sporting_goods_high_spend,
    e_cv.rc_sport_riding_apparel_high_spend,
    e_cv.rc_stamp_coin_store_high_spend,
    e_cv.rc_print_writing_office_supp_high_spend,
    e_cv.rc_tax_prep_service_high_spend,
    e_cv.rc_prepaid_phone_high_spend,
    e_cv.rc_theater_prod_ticket_high_spend,
    e_cv.rc_timeshares_high_spend,
    e_cv.rc_tourist_attract_exhibit_high_spend,
    e_cv.rc_travel_agency_tour_high_spend,
    e_cv.rc_veterinary_service_high_spend,
    e_cv.rc_video_game_supply_high_spend,
    e_cv.rc_video_ent_rental_high_spend,
    e_cv.rc_arcade_high_spend,
    e_cv.rc_wholesale_club_high_spend,
    e_cv.rc_women_access_store_high_spend,
    e_cv.rc_women_ready_to_wear_high_spend,

    -- VEHICLE INTEREST/OWNERSHIP (~200 flags)
    -- Note: EV flags used in EV_OWNERSHIP_SEGMENT and luxury flags used in LUXURY_VEHICLE_SEGMENT are dropped per bucketing strategy
    e_cv.itm_aud_hybrid_toyota_camry,
    e_cv.itm_aud_hybrid_toyota_corolla,
    e_cv.itm_aud_hybrid_honda_crv,
    e_cv.itm_aud_hybrid_mitsu_outlander_phev,
    e_cv.itm_aud_hybrid_toyota_highlander,
    e_cv.itm_aud_hybrid_toyota_rav4,
    e_cv.itm_aud_infiniti_qx55,
    e_cv.own_aud_ford_maverick,
    e_cv.own_aud_kia_k4,
    e_cv.own_aud_mini_countryman_crossover,
    e_cv.own_aud_toyota_corolla_cross,
    e_cv.own_aud_jeep_grand_wagoneer,
    e_cv.own_aud_mini_clubman,
    e_cv.own_aud_hybrid_toyota_venza,
    e_cv.own_aud_hybrid_honda_hybrid,
    e_cv.own_aud_hybrid_toyota_camry,
    e_cv.own_aud_hybrid_toyota_corolla,
    e_cv.own_aud_hybrid_honda_crv,
    e_cv.own_aud_hybrid_mistubishi_outlander_phev,
    e_cv.own_aud_hybrid_toyota_highlander,
    e_cv.own_aud_hybrid_toyota_rav4,
    -- Note: Ultra-luxury and luxury vehicle flags (used in LUXURY_VEHICLE_SEGMENT) dropped
    -- Note: Luxury EV flags (used in EV_OWNERSHIP_SEGMENT) dropped
    e_cv.own_aud_hybrid_alfa_romeo_tonale,
    e_cv.own_aud_hybrid_bmw_xm,
    e_cv.own_aud_hybrid_lexus_rx,
    e_cv.own_aud_chrysler_voyager,
    e_cv.own_aud_sedan_audi_s5_sportback,
    e_cv.own_aud_sedan_audi_s6,
    e_cv.own_aud_sedan_bmw_m5,
    e_cv.own_aud_sedan_cadillac_ct5,
    e_cv.own_aud_sedan_genesis_g90,
    e_cv.own_aud_sedan_jaguar_xe,
    e_cv.own_aud_sedan_mercedes_cls,
    e_cv.own_aud_sedan_volvo_s90,
    e_cv.own_aud_acura_integra,
    e_cv.own_aud_bmw_8_series,
    e_cv.own_aud_bmw_car_m8,
    e_cv.own_aud_jaguar_ftype,
    -- Note: own_aud_lamborghini dropped (used in LUXURY_VEHICLE_SEGMENT)
    e_cv.own_aud_lexus_lc,
    e_cv.own_aud_porsche_718_boxster,
    e_cv.own_aud_porsche_718_cayman,
    e_cv.own_aud_bmw_m2,
    e_cv.own_aud_bmw_m4,
    e_cv.own_aud_bmw_coupe_m8,
    e_cv.own_aud_audi_rs_q8,
    e_cv.own_aud_audi_sq7,
    e_cv.own_aud_audi_sq8,
    e_cv.own_aud_buick_encore_gx,
    e_cv.own_aud_cadillac_escalade_esv,
    e_cv.own_aud_cadillac_escalade_v,
    e_cv.own_aud_cadillac_lyriq,
    e_cv.own_aud_sports_bmw_m_models,
    e_cv.own_aud_genesis_gv70,
    e_cv.own_aud_jeep_wagoneer,
    e_cv.own_aud_land_rover_defender,
    e_cv.own_aud_lexus_tx,
    e_cv.own_aud_mercedes_eqe,
    e_cv.own_aud_mercedes_g_class,
    e_cv.own_aud_mercedes_gla,
    e_cv.own_aud_mercedes_glb,
    -- Note: own_aud_mercedes_eqs dropped (luxury EV, used in EV_OWNERSHIP_SEGMENT)
    e_cv.own_aud_kia_sportage_phev,
    e_cv.own_aud_toyota_rav4_phev,
    e_cv.own_aud_bmw_m3,
    e_cv.own_aud_toyota_crown,
    e_cv.own_aud_toyota_gr_corolla,
    e_cv.own_aud_toyota_gr86,
    e_cv.own_aud_volkswagen_golf_gti,
    e_cv.own_aud_volkswagen_golf_r,
    e_cv.own_aud_subaru_brz,
    -- Note: own_aud_chevrolet_blazer_ev, own_aud_chevrolet_equinox_ev dropped (mainstream EVs, used in EV_OWNERSHIP_SEGMENT)
    e_cv.own_aud_dodge_hornet,
    e_cv.own_aud_ford_bronco_sport,
    e_cv.own_aud_hybrid_hyundai_santa_fe,
    e_cv.own_aud_hybrid_hyundai_tuscon,
    -- Note: own_aud_kia_ev6, own_aud_kia_ev9 dropped (mainstream EVs, used in EV_OWNERSHIP_SEGMENT)
    e_cv.own_aud_mazda_cx50,
    e_cv.own_aud_mazda_cx70,
    e_cv.own_aud_mazda_cx90,
    e_cv.own_aud_mitsubishi_eclipse_cross,
    e_cv.own_aud_toyota_grand_highlander,
    e_cv.own_aud_toyota_land_cruiser,
    e_cv.own_aud_mercedes_sprinter,
    e_cv.own_aud_ram_promaster,
    e_cv.own_aud_volvo_v60_cross_country,
    e_cv.own_aud_volvo_v90_cross_country,
    e_cv.own_aud_motorcycle_aprilia,
    e_cv.own_aud_motorcycle_ducati,
    e_cv.own_aud_motorcycle_ktm,
    e_cv.own_aud_motorcycle_suzuki,
    e_cv.own_aud_motorcycle_triumph,
    -- Note: Luxury vehicle flags dropped (own_aud_mercedes_sl_convertible, own_aud_mercedes_amg_gt - used in LUXURY_VEHICLE_SEGMENT)
    e_cv.own_aud_hybrid_hyundai_tuscon_phev,
    e_cv.itm_aud_motorcycle_aprilia,
    -- Note: Ultra-luxury and luxury interest flags dropped (itm_aud_mercedes_amg_gt, itm_aud_aston_martin, itm_aud_bentley, itm_aud_lamborghini, itm_aud_maserati, itm_aud_mercedes_s_class - used in LUXURY_VEHICLE_SEGMENT)
    e_cv.itm_aud_ev_audi_etron_gt,
    e_cv.itm_aud_ev_bmw_i4,
    e_cv.itm_aud_ev_bmw_i5,
    e_cv.itm_aud_ev_bmw_i7,
    e_cv.itm_aud_ev_lucid_air,
    e_cv.itm_aud_ev_mercedes_eqe,
    e_cv.itm_aud_ev_audi_q4_etron,
    e_cv.itm_aud_ev_audi_q8_etron,
    e_cv.itm_aud_ev_bmw_ix,
    e_cv.itm_aud_ev_cadillac_lyriq,
    e_cv.itm_aud_ev_lexus_rz,
    e_cv.itm_aud_ev_mercedes_eqs,
    e_cv.itm_aud_ev_rivian_r1s,
    e_cv.itm_aud_ev_rivian_r1t,
    e_cv.itm_aud_ev_tesla_cybertruck,
    e_cv.itm_aud_liftback_polestar2,
    e_cv.itm_aud_alfa_romeo_giulia,
    e_cv.itm_aud_audi_a3,
    e_cv.itm_aud_audi_a5_sportback,
    e_cv.itm_aud_audi_a6,
    e_cv.itm_aud_bmw_7_series,
    e_cv.itm_aud_bmw_m3,
    e_cv.itm_aud_genesis_g80,
    e_cv.itm_aud_genesis_g90,
    e_cv.itm_aud_lexus_ls,
    e_cv.itm_aud_volvo_s60,
    e_cv.itm_aud_audi_s5_sportback,
    e_cv.itm_aud_bmw_8_series,
    e_cv.itm_aud_bmw_m_models,
    e_cv.itm_aud_bmw_m2,
    e_cv.itm_aud_bmw_m4,
    e_cv.itm_aud_lexus_lc,
    e_cv.itm_aud_mercedes_cle,
    e_cv.itm_aud_porsche_718_boxster,
    e_cv.itm_aud_porsche_718_cayman,
    e_cv.itm_aud_porsche_panamera,
    e_cv.itm_aud_porsche_taycan,
    e_cv.itm_aud_mercedes_sl_class,
    e_cv.itm_aud_mini_clubman,
    e_cv.itm_aud_alfa_romeo_stelvio,
    e_cv.itm_aud_alfa_romeo_tonale,
    e_cv.itm_aud_audi_q8,
    e_cv.itm_aud_audi_rs_q8,
    e_cv.itm_aud_audi_sq5,
    e_cv.itm_aud_audi_sq7,
    e_cv.itm_aud_bmw_x4,
    e_cv.itm_aud_bmw_xm,
    e_cv.itm_aud_buick_encore_gx,
    e_cv.itm_aud_buick_envista,
    e_cv.itm_aud_cadillac_escalade_esv,
    e_cv.itm_aud_cadillac_escalade_v,
    e_cv.itm_aud_jeep_grand_wagoneer,
    e_cv.itm_aud_land_rover_discovery,
    e_cv.itm_aud_land_rover_discovery_sport,
    e_cv.itm_aud_land_rover_evoque,
    e_cv.itm_aud_lexus_rx_hybrid,
    e_cv.itm_aud_lexus_tx,
    e_cv.itm_aud_lexus_ux,
    e_cv.itm_aud_mercedes_g_class,
    e_cv.itm_aud_mercedes_gla,
    e_cv.itm_aud_mini_countryman,
    e_cv.itm_aud_cadillac_ct5,
    e_cv.itm_aud_toyota_land_cruiser,
    e_cv.itm_aud_mercedes_sprinter,
    e_cv.itm_aud_plugin_hybrid_toyota_rav4,
    e_cv.itm_aud_hybrid_honda_accord,
    e_cv.itm_aud_kia_k4,
    e_cv.itm_aud_toyota_crown,
    e_cv.itm_aud_toyota_gr86,
    e_cv.itm_aud_acura_integra,
    e_cv.itm_aud_volkswagen_golf_gti,
    e_cv.itm_aud_volkswagen_golf_r,
    e_cv.itm_aud_subaru_brz,
    e_cv.itm_aud_toyota_gr_corolla,
    e_cv.itm_aud_bmw_x6,
    e_cv.itm_aud_chevrolet_blazer_ev,
    e_cv.itm_aud_chevrolet_equinox_ev,
    e_cv.itm_aud_dodge_hornet,
    e_cv.itm_aud_ford_bronco_sport,
    e_cv.itm_aud_hybrid_hyundai_santa_fe,
    e_cv.itm_aud_hybrid_hyundai_tucson,
    e_cv.itm_aud_hyundai_tucson_phev,
    e_cv.itm_aud_kia_sportage_phev,
    e_cv.itm_aud_lexus_lx,
    e_cv.itm_aud_mazda_cx_50,
    e_cv.itm_aud_mazda_cx_70,
    e_cv.itm_aud_mazda_cx_90,
    e_cv.itm_aud_mitsubishi_eclipse_cross,
    e_cv.itm_aud_nissan_ariya,
    e_cv.itm_aud_toyota_corolla_cross,
    e_cv.itm_aud_toyota_grand_highlander,
    e_cv.itm_aud_ford_maverick,
    e_cv.itm_aud_gmc_hummer_ev,
    e_cv.itm_aud_ram_promaster,

    -- SHOPPING BEHAVIORS (~100 flags - keep all)
    e_cv.hand_body_lotion_shop,
    e_cv.body_wash_shop,
    e_cv.liquid_hand_soap_shop,
    e_cv.eye_shadow_shop,
    e_cv.mascara_shop,
    e_cv.facial_cleaner_shop,
    e_cv.facial_cosmetic_shop,
    e_cv.foundation_shop,
    e_cv.lipstick_shop,
    e_cv.facial_moisturizer_shop,
    e_cv.facial_anti_aging_shop,
    e_cv.lip_balm_shop,
    e_cv.shaving_mens_frag_shop,
    e_cv.hair_access_shop,
    e_cv.hair_color_shop,
    e_cv.styling_prod_shop,
    e_cv.shaving_cream_shop,
    e_cv.nail_polish_shop,
    e_cv.gift_card_shop,
    e_cv.baby_food_shop,
    e_cv.bagel_bialys_shop,
    e_cv.cupcake_brownie_shop,
    e_cv.fresh_bread_shop,
    e_cv.hamb_hot_dog_bun_shop,
    e_cv.donut_shop,
    e_cv.frosting_shop,
    e_cv.brownie_mix_shop,
    e_cv.pancake_waffle_shop,
    e_cv.olive_oil_shop,
    e_cv.ready_pie_crust_shop,
    e_cv.sugar_shop,
    e_cv.sugar_substitute_shop,
    e_cv.beer_ale_cider_shop,
    e_cv.low_cal_soft_drink_shop,
    e_cv.regular_soft_drink_shop,
    e_cv.cocktail_mix_shop,
    e_cv.ground_coffee_shop,
    e_cv.instant_coffee_shop,
    e_cv.single_cup_coffee_shop,
    e_cv.tea_bag_loose_shop,
    e_cv.can_bottle_tea_shop,
    e_cv.energy_drink_shop,
    e_cv.sports_drink_shop,
    e_cv.bottle_water_shop,
    e_cv.sparkle_water_shop,
    e_cv.cereal_energy_bar_shop,
    e_cv.cold_cereal_shop,
    e_cv.english_muffin_shop,
    e_cv.hot_cereal_oatmeal_shop,
    e_cv.toaster_pastry_shop,
    e_cv.chocolate_shop,
    e_cv.choc_candy_shop,
    e_cv.sugarless_gum_shop,
    e_cv.gummy_chewy_snack_shop,
    e_cv.hard_candy_shop,
    e_cv.marshmallow_shop,
    e_cv.mint_candy_shop,
    e_cv.ramen_shop,
    e_cv.ready_soup_shop,
    e_cv.bbq_sauce_shop,
    e_cv.frozen_pizza_shop,
    e_cv.steak_sauce_shop,
    e_cv.honey_shop,
    e_cv.ketchup_shop,
    e_cv.mayo_sand_spread_shop,
    e_cv.mustard_shop,
    e_cv.peanut_butter_shop,
    e_cv.special_nut_butter_shop,
    e_cv.olive_shop,
    e_cv.pickle_shop,
    e_cv.salad_dressing_shop,
    e_cv.syrup_shop,
    e_cv.cookie_shop,
    e_cv.cracker_shop,
    e_cv.dip_spread_shop,
    e_cv.dried_meat_jerky_shop,
    e_cv.salty_snack_shop,
    e_cv.potato_chip_shop,
    e_cv.pretzel_shop,
    e_cv.tortilla_chip_shop,
    e_cv.margarine_shop,
    e_cv.butter_shop,
    e_cv.cream_cheese_shop,
    e_cv.cheese_slices_shop,
    e_cv.cheese_string_shop,
    e_cv.refrig_coffee_cream_shop,
    e_cv.shelf_coffee_cream_shop,
    e_cv.milk_shop,
    e_cv.refrig_almond_milk_shop,
    e_cv.refrig_skim_milk_shop,
    e_cv.refrig_whole_milk_shop,
    e_cv.yogurt_shop,
    e_cv.deli_meat_shop,
    e_cv.tortilla_taco_kit_shop,
    e_cv.salsa_shop,
    e_cv.fruit_shop,
    e_cv.frozen_food_shop,
    e_cv.frozen_waffle_shop,
    e_cv.frozen_meal_shop,
    e_cv.frozen_potato_shop,
    e_cv.frozen_seafood_shop,
    e_cv.ice_cream_shop,
    e_cv.frozen_meat_shop,
    e_cv.bacon_shop,
    e_cv.dinner_sausage_shop,
    e_cv.hot_dog_shop,
    e_cv.pork_shop,
    e_cv.instant_potato_shop,
    e_cv.mac_cheese_shop,
    e_cv.refrig_entree_shop,
    e_cv.refrig_lunch_shop,
    e_cv.refrig_sides_shop,
    e_cv.prepared_chili_shop,
    e_cv.spag_pasta_shop,
    e_cv.dry_rice_shop,
    e_cv.nutrition_weight_shop,
    e_cv.mouthwash_shop,
    e_cv.dental_floss_shop,
    e_cv.toothpaste_shop,
    e_cv.suntan_lotion_shop,
    e_cv.multi_vitamin_shop,
    e_cv.charcoal_shop,
    e_cv.air_freshener_shop,
    e_cv.dispos_plates_shop,
    e_cv.dish_detergent_shop,
    e_cv.storage_bag_shop,
    e_cv.all_purpose_cleaner_shop,
    e_cv.laundry_detergent_shop,
    e_cv.pest_control_shop,
    e_cv.outdoor_insect_pest_shop,
    e_cv.cat_litter_shop,
    e_cv.wet_cat_food_shop,
    e_cv.dry_dog_food_shop,
    e_cv.wet_dog_food_shop,
    e_cv.other_pet_food_shop,

    -- Processing Metadata
    CURRENT_TIMESTAMP() AS DBT_UPDATED_AT

FROM one_luid_per_household olh
LEFT JOIN {{ source('locality_poc_share_silver', 'experian_consumerview') }} e_cv
    ON olh.luid = e_cv.recd_luid
