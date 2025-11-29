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
    -- ============================================================
    CONCAT_WS(',',
        CASE WHEN e_cv.rc_rs_tv_brand_comcast = 'Y' THEN 'Comcast' END,
        CASE WHEN e_cv.rc_rs_tv_brand_directv = 'Y' THEN 'DirecTV' END,
        CASE WHEN e_cv.rc_rs_tv_brand_dish_network = 'Y' THEN 'Dish Network' END,
        CASE WHEN e_cv.rc_rs_tv_brand_spectrum = 'Y' THEN 'Spectrum' END,
        CASE WHEN e_cv.rc_rs_tv_brand_xfinity = 'Y' THEN 'Xfinity' END,
        CASE WHEN e_cv.`RC_TVMovies_HBO_Watcher` = 'Y' THEN 'HBO' END
    ) AS NETWORKS_WATCHED,

    -- ============================================================
    -- INPUT DEVICES USED
    -- ============================================================
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_Buyer_Laptop_Owners` = 'Y' THEN 'Laptop' END,
        CASE WHEN e_cv.`RC_Buyer_Tablet_Owners` = 'Y' THEN 'Tablet' END,
        CASE WHEN e_cv.`RC_CompElect_Apple_iPhone_` = 'Y' THEN 'iPhone' END,
        CASE WHEN e_cv.`RC_CompElect_Apple_Mac_User` = 'Y' THEN 'Mac' END,
        CASE WHEN e_cv.`RC_CompElect_Dell_Computer_User` = 'Y' THEN 'Dell PC' END,
        CASE WHEN e_cv.tv_brand_samsung = 'Y' THEN 'Samsung TV' END,
        CASE WHEN e_cv.tv_brand_vizio = 'Y' THEN 'Vizio TV' END,
        CASE WHEN e_cv.tv_brand_lg = 'Y' THEN 'LG TV' END,
        CASE WHEN e_cv.tv_brand_sony = 'Y' THEN 'Sony TV' END
    ) AS INPUT_DEVICES_USED,

    -- ============================================================
    -- TV/MOVIE GENRES WATCHED
    -- ============================================================
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_TVMovies_Comedy_Fan` = 'Y' THEN 'Comedy' END,
        CASE WHEN e_cv.`RC_TVMovies_Drama_Fan` = 'Y' THEN 'Drama' END,
        CASE WHEN e_cv.`RC_TVMovies_Thriller_Movie_Buff` = 'Y' THEN 'Thriller' END,
        CASE WHEN e_cv.`RC_TVMoves_Horror_Movies` = 'Y' THEN 'Horror' END,
        CASE WHEN e_cv.`RC_TVMovies_Scifi_Movie_Buff` = 'Y' THEN 'Sci-Fi' END,
        CASE WHEN e_cv.`RC_TVMovies_Adventure_Movies` = 'Y' THEN 'Adventure' END,
        CASE WHEN e_cv.`RC_TVMovies_Drama_Movies` = 'Y' THEN 'Drama Movies' END,
        CASE WHEN e_cv.`RC_TVMovies_DocuForeign_Movies` = 'Y' THEN 'Documentary/Foreign' END,
        CASE WHEN e_cv.`RC_TVMovies_Family_Films_` = 'Y' THEN 'Family' END,
        CASE WHEN e_cv.`RC_TVMovies_Romantic_Comedy_Movies` = 'Y' THEN 'Romantic Comedy' END,
        CASE WHEN e_cv.`RC_TVMovies_Cult_Classic` = 'Y' THEN 'Cult Classic' END,
        CASE WHEN e_cv.`RC_TVMovies_Reality_TV_Shows` = 'Y' THEN 'Reality TV' END,
        CASE WHEN e_cv.`RC_TVMovies_Game_Shows` = 'Y' THEN 'Game Shows' END,
        CASE WHEN e_cv.`RC_TVMovies_TV_News` = 'Y' THEN 'News' END,
        CASE WHEN e_cv.`RC_TVMovies_TV_History_Genre` = 'Y' THEN 'History' END,
        CASE WHEN e_cv.`RC_TVMovies_TV_Animation_Genre` = 'Y' THEN 'Animation' END
    ) AS GENRES_WATCHED,

    -- ============================================================
    -- TITLES / SHOWS WATCHED (specific shows)
    -- ============================================================
    CONCAT_WS(',',
        CASE WHEN e_cv.`RC_TVMovies_Top_Chef_TV_` = 'Y' THEN 'Top Chef' END,
        CASE WHEN e_cv.`RC_TVMovies_Oprah_Fan` = 'Y' THEN 'Oprah' END,
        CASE WHEN e_cv.`RC_TVMovies_Grammy_Watcher` = 'Y' THEN 'Grammy Awards' END,
        CASE WHEN e_cv.`RC_TVMovies_College_Basketball_` = 'Y' THEN 'College Basketball' END,
        CASE WHEN e_cv.`RC_TVMovies_College_Football` = 'Y' THEN 'College Football' END,
        CASE WHEN e_cv.`RC_TVMovies_Female_TV_Shows` = 'Y' THEN 'Female TV Shows' END,
        CASE WHEN e_cv.`RC:_TVMovies:_Guy_Shows_on_TV_V1` = 'Y' THEN 'Guy TV Shows' END
    ) AS TITLES_WATCHED,

    -- ============================================================
    -- TV VIEWING BEHAVIOR
    -- ============================================================
    CASE WHEN e_cv.tv_ad_avoider = 'Y' THEN 'Ad Avoider'
         WHEN e_cv.tv_extreme_ad_avoider = 'Y' THEN 'Extreme Ad Avoider'
         WHEN e_cv.tv_ad_acceptor = 'Y' THEN 'Ad Acceptor'
         ELSE NULL
    END AS AD_BEHAVIOR,

    CASE WHEN e_cv.tv_solo_viewer = 'Y' THEN 'Solo'
         WHEN e_cv.tv_cowatcher = 'Y' THEN 'Co-Watcher'
         WHEN e_cv.tv_cowatching_children = 'Y' THEN 'Co-Watching with Children'
         WHEN e_cv.tv_cowatching_no_children = 'Y' THEN 'Co-Watching (No Children)'
         ELSE NULL
    END AS VIEWING_MODE,

    CASE WHEN e_cv.tv_small_screen = 'Y' THEN 'Small'
         WHEN e_cv.tv_large_screen = 'Y' THEN 'Large'
         ELSE NULL
    END AS SCREEN_SIZE,

    -- Cord cutting status
    CASE WHEN e_cv.`RC_OBM_cordcuttersV1` = 'Y' THEN 1 ELSE 0 END AS IS_CORD_CUTTER,

    -- Temporal
    attr.PARTITION_DATE

FROM {{ ref('akkio_attributes_latest') }} attr
LEFT JOIN {{ source('locality_poc_share_silver', 'experian_consumerview2') }} e_cv
    ON attr.LUID = e_cv.recd_luid
