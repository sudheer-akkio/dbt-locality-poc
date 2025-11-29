{{ config(
    materialized='table',
    alias='V_AGG_AKKIO_IND_MEDIA',
    post_hook=[
        "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_ID)",
    ]
)}}

/*
    Locality Individual Media Consumption Table

    Purpose: Media and entertainment preferences for analytics.
    Source: experian_consumerview2 (joined via akkio_attributes_latest.LUID)
    Grain: One row per AKKIO_ID (household)

    Contains: Streaming services, TV networks, devices, genres watched
*/

SELECT
    -- Primary Keys
    attr.AKKIO_ID,
    attr.AKKIO_HH_ID,

    -- Weight (1.0 for equal weighting)
    1.0 AS WEIGHT,

    -- ============================================================
    -- STREAMING SERVICES / APP SERVICES USED
    -- ============================================================
    CONCAT_WS(',',
        CASE WHEN e_cv.rc_rs_video_brand_netflix = 'Y' THEN 'Netflix' END,
        CASE WHEN e_cv.rc_rs_video_brand_hulu = 'Y' THEN 'Hulu' END,
        CASE WHEN e_cv.rc_rs_video_brand_hbo = 'Y' THEN 'HBO' END,
        CASE WHEN e_cv.rc_rs_video_brand_sling_tv = 'Y' THEN 'Sling TV' END,
        CASE WHEN e_cv.rc_rs_video_brand_vudu = 'Y' THEN 'Vudu' END,
        CASE WHEN e_cv.rc_rs_audio_brand_spotify = 'Y' THEN 'Spotify' END,
        CASE WHEN e_cv.rc_rs_audio_brand_pandora = 'Y' THEN 'Pandora' END,
        CASE WHEN e_cv.`rc_rs_audio_brand_sirius_xm` = 'Y' THEN 'SiriusXM' END
    ) AS APP_SERVICES_USED,

    -- ============================================================
    -- TV NETWORKS / CABLE PROVIDERS
    -- Note: rc_rs_* use 'Y', RC_TVMovies_* use 'A'
    -- ============================================================
    CONCAT_WS(',',
        CASE WHEN e_cv.rc_rs_tv_brand_comcast = 'Y' THEN 'Comcast' END,
        CASE WHEN e_cv.rc_rs_tv_brand_directv = 'Y' THEN 'DirecTV' END,
        CASE WHEN e_cv.rc_rs_tv_brand_dish_network = 'Y' THEN 'Dish Network' END,
        CASE WHEN e_cv.rc_rs_tv_brand_spectrum = 'Y' THEN 'Spectrum' END,
        CASE WHEN e_cv.rc_rs_tv_brand_xfinity = 'Y' THEN 'Xfinity' END,
        CASE WHEN e_cv.`RC_TVMovies_HBO_Watcher` = 'A' THEN 'HBO' END
    ) AS NETWORKS_WATCHED,

    -- ============================================================
    -- INPUT DEVICES USED
    -- Note: RC_Buyer_*/RC_CompElect_* use 'A', tv_brand_* use 'Y'
    -- ============================================================
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_Buyer_Laptop_Owners` = 'A' THEN 'Laptop' END,
        CASE WHEN e_cv.`RC_Buyer_Tablet_Owners` = 'A' THEN 'Tablet' END,
        CASE WHEN e_cv.`RC_CompElect_Apple_iPhone_` = 'A' THEN 'iPhone' END,
        CASE WHEN e_cv.`RC_CompElect_Apple_Mac_User` = 'A' THEN 'Mac' END,
        CASE WHEN e_cv.`RC_CompElect_Dell_Computer_User` = 'A' THEN 'Dell PC' END,
        CASE WHEN e_cv.tv_brand_samsung = 'Y' THEN 'Samsung TV' END,
        CASE WHEN e_cv.tv_brand_vizio = 'Y' THEN 'Vizio TV' END,
        CASE WHEN e_cv.tv_brand_lg = 'Y' THEN 'LG TV' END,
        CASE WHEN e_cv.tv_brand_sony = 'Y' THEN 'Sony TV' END
    ) AS INPUT_DEVICES_USED,

    -- ============================================================
    -- TV/MOVIE GENRES WATCHED
    -- ============================================================
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_TVMovies_Comedy_Fan` = 'A' THEN 'Comedy' END,
        CASE WHEN e_cv.`RC_TVMovies_Drama_Fan` = 'A' THEN 'Drama' END,
        CASE WHEN e_cv.`RC_TVMovies_Thriller_Movie_Buff` = 'A' THEN 'Thriller' END,
        CASE WHEN e_cv.`RC_TVMoves_Horror_Movies` = 'A' THEN 'Horror' END,
        CASE WHEN e_cv.`RC_TVMovies_Scifi_Movie_Buff` = 'A' THEN 'Sci-Fi' END,
        CASE WHEN e_cv.`RC_TVMovies_Adventure_Movies` = 'A' THEN 'Adventure' END,
        CASE WHEN e_cv.`RC_TVMovies_Drama_Movies` = 'A' THEN 'Drama Movies' END,
        CASE WHEN e_cv.`RC_TVMovies_DocuForeign_Movies` = 'A' THEN 'Documentary/Foreign' END,
        CASE WHEN e_cv.`RC_TVMovies_Family_Films_` = 'A' THEN 'Family' END,
        CASE WHEN e_cv.`RC_TVMovies_Romantic_Comedy_Movies` = 'A' THEN 'Romantic Comedy' END,
        CASE WHEN e_cv.`RC_TVMovies_Cult_Classic` = 'A' THEN 'Cult Classic' END,
        CASE WHEN e_cv.`RC_TVMovies_Reality_TV_Shows` = 'A' THEN 'Reality TV' END,
        CASE WHEN e_cv.`RC_TVMovies_Game_Shows` = 'A' THEN 'Game Shows' END,
        CASE WHEN e_cv.`RC_TVMovies_TV_News` = 'A' THEN 'News' END,
        CASE WHEN e_cv.`RC_TVMovies_TV_History_Genre` = 'A' THEN 'History' END,
        CASE WHEN e_cv.`RC_TVMovies_TV_Animation_Genre` = 'A' THEN 'Animation' END
    ) AS GENRES_WATCHED,

    -- ============================================================
    -- TITLES / SHOWS WATCHED (specific shows)
    -- ============================================================
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_TVMovies_Top_Chef_TV_` = 'A' THEN 'Top Chef' END,
        CASE WHEN e_cv.`RC_TVMovies_Oprah_Fan` = 'A' THEN 'Oprah' END,
        CASE WHEN e_cv.`RC_TVMovies_Grammy_Watcher` = 'A' THEN 'Grammy Awards' END,
        CASE WHEN e_cv.`RC_TVMovies_College_Basketball_` = 'A' THEN 'College Basketball' END,
        CASE WHEN e_cv.`RC_TVMovies_College_Football` = 'A' THEN 'College Football' END,
        CASE WHEN e_cv.`RC_TVMovies_Female_TV_Shows` = 'A' THEN 'Female TV Shows' END,
        CASE WHEN e_cv.`RC:_TVMovies:_Guy_Shows_on_TV_V1` = 'A' THEN 'Guy TV Shows' END
    ) AS TITLES_WATCHED,

    -- ============================================================
    -- TV VIEWING BEHAVIOR
    -- ============================================================
    -- AD_BEHAVIORS: May not be mutually exclusive
    CONCAT_WS(',',
        CASE WHEN e_cv.tv_extreme_ad_avoider = 'Y' THEN 'Extreme Ad Avoider' END,
        CASE WHEN e_cv.tv_ad_avoider = 'Y' THEN 'Ad Avoider' END,
        CASE WHEN e_cv.tv_ad_acceptor = 'Y' THEN 'Ad Acceptor' END
    ) AS AD_BEHAVIORS,

    -- VIEWING_MODES: May not be mutually exclusive
    CONCAT_WS(',',
        CASE WHEN e_cv.tv_solo_viewer = 'Y' THEN 'Solo' END,
        CASE WHEN e_cv.tv_cowatcher = 'Y' THEN 'Co-Watcher' END,
        CASE WHEN e_cv.tv_cowatching_children = 'Y' THEN 'Co-Watching with Children' END,
        CASE WHEN e_cv.tv_cowatching_no_children = 'Y' THEN 'Co-Watching (No Children)' END
    ) AS VIEWING_MODES,

    -- SCREEN_SIZES: May not be mutually exclusive
    CONCAT_WS(',',
        CASE WHEN e_cv.tv_small_screen = 'Y' THEN 'Small' END,
        CASE WHEN e_cv.tv_large_screen = 'Y' THEN 'Large' END
    ) AS SCREEN_SIZES,

    -- Cord cutting status
    CASE WHEN e_cv.`RC_OBM_cordcuttersV1` = 'Y' THEN 1 ELSE 0 END AS IS_CORD_CUTTER,

    -- Temporal
    attr.PARTITION_DATE

FROM {{ ref('akkio_attributes_latest') }} attr
LEFT JOIN {{ source('locality_poc_share_silver', 'experian_consumerview2') }} e_cv
    ON attr.LUID = e_cv.recd_luid
