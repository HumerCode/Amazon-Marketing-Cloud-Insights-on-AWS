--Product Mix Query
--This query provides a holistic overview of performance of users exposed to DSP and SP campaigns
--This query can be expanded to campaign level by introducing a campaign field with input provided from the user to understand which DSP campaigns are to be compared with which Sponsored Products Advertiser campaigns

--Gather all sales information for targeted products
WITH SALES AS (
              SELECT USER_ID
                   , CASE WHEN ADVERTISER_ID IS NULL THEN 'SP' ELSE 'ADSP' END AS sales_source
                   , SUM(purchases)                                            AS CONVERSIONS
                   , SUM(product_sales)                                        AS sales_amount
                   , SUM(new_to_brand_purchases)                               AS ntb_conversions
              FROM amazon_attributed_events_by_conversion_time
              WHERE conversion_event_subtype = 'order'
                AND purchases >= 1

              GROUP BY USER_ID, sales_source),


   -- Gather all display campaign KPI information
    ADSP_overall AS (
                    SELECT A.user_id
                         , SUM(A.IMPRESSIONS)         AS adsp_impressions
                         , SUM(A.total_cost) / 100000 AS adsp_cost
                         , SUM(B.CLICKS)              AS adsp_clicks
                    FROM DISPLAY_IMPRESSIONS A
                             LEFT JOIN DISPLAY_CLICKS B ON A.request_tag = B.request_tag
                    GROUP BY A.user_id),


--Gather all Sponsored products information
    SP_overall AS (
                  SELECT A.USER_ID
                       , SUM(A.IMPRESSIONS)       AS SP_IMPRESSIONS
                       , SUM(A.CLICKS)            AS SP_CLICKS
                       , SUM(A.SPEND) / 100000000 AS SP_COST
                  FROM sponsored_ads_traffic A
                  GROUP BY A.user_id),

   --Combine the sponsored ads and display information and create the mix of products column
    Combined AS (
                SELECT DISTINCT
    --Case statement to identify users exposed to DSP campaigns and SP campaigns
                CASE
                                    WHEN A.USER_ID IS NOT NULL
                                        AND B.USER_ID IS NOT NULL
                                        AND A.USER_ID = B.USER_ID THEN
                                        'SP + ADSP'
                                    WHEN A.USER_ID IS NOT NULL
                                        AND B.USER_ID IS NULL THEN
                                        'ADSP'
                                    WHEN A.USER_ID IS NULL
                                        AND B.USER_ID IS NOT NULL THEN
                                        'SP'
                                    ELSE
                                        'NOT FOUND IN ADSP OR SP'
                    END                                        AS MIX_OF_PRODUCTS
                              , COALESCE(A.USER_ID, B.USER_ID) AS USER_ID
                              , A.adsp_impressions
                              , A.adsp_cost
                              , B.SP_IMPRESSIONS
                              , B.SP_CLICKS
                              , B.SP_COST
                              , A.adsp_clicks
                FROM ADSP_overall A
                         FULL JOIN SP_overall B ON a.user_id = b.user_id
    )


--Bring in the fields from above tables along with individual SP and ADSP KPIs
SELECT BUILT_IN_PARAMETER('TIME_WINDOW_START')                                                        AS time_window_start
     , BUILT_IN_PARAMETER('TIME_WINDOW_END')                                                          AS time_window_end
     , A.MIX_OF_PRODUCTS
     , COUNT(DISTINCT a.user_Id)                                                                      AS TOTAL_UNIQUE_USERS
     , COUNT(DISTINCT B.USER_ID)                                                                      AS TOTAL_UNIQUE_USERS_CONVERSIONS
     , (COUNT(DISTINCT a.user_Id) - COUNT(DISTINCT B.USER_ID))                                        AS TOTAL_UNIQUE_USERS_NOT_CONVERTED
     , SUM(B.CONVERSIONS)                                                                             AS TOTAL_CONVERSIONS
     , SUM(B.ntb_conversions)                                                                         AS NTB_CONVERSIONS
     , SUM(B.SALES_AMOUNT)                                                                            AS SALES_AMOUNT
     , SUM(CASE WHEN B.SALES_SOURCE = 'ADSP' THEN B.SALES_AMOUNT ELSE 0 END)                          AS ADSP_SALES
     , SUM(CASE WHEN B.SALES_SOURCE = 'SP' THEN B.SALES_AMOUNT ELSE 0 END)                            AS SP_SALES
     , SUM(A.ADSP_impressions)                                                                        AS OVERALL_ADSP_impressions
     , SUM(A.adsp_clicks)                                                                             AS OVERALL_ADSP_CLICKS
     , COUNT(A.ADSP_impressions)                                                                      AS OVERALL_ADSP_UNIQUE_USERS
     , SUM(A.ADSP_COST)                                                                               AS OVERALL_ADSP_COST
     , SUM(CASE WHEN B.USER_ID IS NOT NULL THEN A.ADSP_impressions ELSE 0 END)                        AS CONVERTED_ADSP_IMPRESSIONS
     , SUM(CASE
               WHEN B.USER_ID IS NOT NULL AND COALESCE(A.ADSP_impressions, 0) > 0 THEN 1
               ELSE 0 END)                                                                            AS CONVERTED_ADSP_UNIQUE_USERS
     , SUM(CASE WHEN B.USER_ID IS NOT NULL THEN A.ADSP_COST ELSE 0 END)                               AS CONVERTED_ADSP_COST
     , (SUM(CASE WHEN B.SALES_SOURCE = 'ADSP' THEN B.SALES_AMOUNT ELSE 0 END) / SUM(A.ADSP_COST))     AS ADSP_ROAS
     , SUM(A.SP_IMPRESSIONS)                                                                          AS OVERALL_SP_IMPRESSIONS
     , SUM(A.SP_CLICKS)                                                                               AS OVERALL_SP_CLICKS
     , COUNT(A.SP_IMPRESSIONS)                                                                        AS OVERALL_SP_UNIQUE_USERS
     , SUM(A.SP_COST)                                                                                 AS OVERALL_SP_COST
     , SUM(CASE WHEN B.USER_ID IS NOT NULL THEN A.SP_IMPRESSIONS ELSE 0 END)                          AS CONVERTED_SP_IMPRESSIONS
     , SUM(CASE WHEN B.USER_ID IS NOT NULL THEN A.SP_CLICKS ELSE 0 END)                               AS CONVERTED_SP_CLICKS
     , SUM(CASE WHEN B.USER_ID IS NOT NULL AND COALESCE(A.SP_IMPRESSIONS, 0) > 0 THEN 1
       ELSE 0 END)                                                                                    AS CONVERTED_SP_UNIQUE_USERS
     , SUM(CASE WHEN B.USER_ID IS NOT NULL THEN A.SP_COST ELSE 0 END)                                 AS CONVERTED_SP_COST
     , (SUM(CASE WHEN B.SALES_SOURCE = 'SP' THEN B.SALES_AMOUNT ELSE 0 END) / SUM(A.SP_COST))         AS SP_ROAS
     , COUNT(a.user_id)                                                                               AS TOTAL_RECORDS
     , (SUM(B.SALES_AMOUNT) / (SUM(A.SP_COST) + SUM(A.ADSP_COST)))                                    AS COMBINED_ROAS
FROM Combined A
         LEFT JOIN SALES B ON a.user_id = b.user_id
GROUP BY 3
