--Geo Analysis Query
--This query provides a holistic performance of DSP Campaigns based on geographic locations
--Note: You can also filter out the desired Campaigns and perform analysis only for those campaigns by adding adding a campaign filter in the where clause

--Gather impressions, clicks , geo information per user,advertiser and campaign
WITH IMP_CLK_INFO AS (
    SELECT
        A.ADVERTISER,
        A.advertiser_id,
        A.CAMPAIGN_ID,
        A.CAMPAIGN,
        A.campaign_start_date,
        A.campaign_end_date,
        A.device_type,
--Mapping between dma_code, city, region , latitude and longitude is available today and can be accessed for any visualization purposes or to identify any city or region in specific
        A.dma_code,
        A.user_id,
        SUM(A.IMPRESSIONS) AS IMPRESSIONS,
        SUM(B.CLICKS) AS TOTAL_CLICKS,
        SUM(A.TOTAL_COST / 100000) AS TOTAL_COST,
        BUILT_IN_PARAMETER ('TIME_WINDOW_START') AS time_window_start,
        BUILT_IN_PARAMETER ('TIME_WINDOW_END') AS time_window_end
    FROM
        display_impressions A
    LEFT JOIN DISPLAY_CLICKS B ON A.request_tag = B.request_tag and A.user_id =b.user_id
GROUP BY
    A.ADVERTISER,
    A.advertiser_id,
    A.CAMPAIGN_ID,
    A.CAMPAIGN,
    A.campaign_start_date,
    A.campaign_end_date,
    A.device_type,
    A.dma_code,
    A.user_id),

--Gather sales, geo information per user,advertiser and campaign
CONVERSIONS AS (
    SELECT
        A.ADVERTISER,
        A.advertiser_id,
        A.user_id,
        A.CAMPAIGN_ID,
        A.CAMPAIGN,
        A.campaign_start_date,
        A.campaign_end_date,
        A.device_type,
        A.dma_code,
        A.conversion_event_subtype,
        SUM(A.CONVERSIONS) AS CONVERSIONS,
        MAX(new_to_brand_purchases)     AS ntb -- Flag that indicates that within this timeframe the customer became a new to brand - not necessarily used here
       , SUM(purchases)                  AS purchases
       , SUM(product_sales)              AS product_sales
       , SUM(new_to_brand_purchases)     AS ntb_purchases
       , SUM(new_to_brand_product_sales) AS ntb_product_sales
        ,BUILT_IN_PARAMETER ('TIME_WINDOW_START') AS time_window_start
       , BUILT_IN_PARAMETER ('TIME_WINDOW_END') AS time_window_end
    from
        amazon_attributed_events_by_conversion_time A
    group by
        A.ADVERTISER,
        A.advertiser_id,
        A.user_id,
        A.CAMPAIGN_ID,
        A.CAMPAIGN,
        A.campaign_start_date,
        A.campaign_end_date,
        A.dma_code,
        A.device_type,
        A.conversion_event_subtype
)

--Combine the sales and impressions , clicks and geo information from the above created tables and identify fields to be a part of the report
--This aggregation is only at the advertiser and the campaign level
--Geo level fields dma_code is only available for US only advertisers. Additional Geo fields like iso_state_province_code, postal_code can be used otherwise
SELECT
    A.ADVERTISER,
    A.CAMPAIGN_ID,
    A.CAMPAIGN,
    A.campaign_start_date,
    A.campaign_end_date,
    A.device_type,
    A.dma_code,
     B.conversion_event_subtype,
    SUM(A.IMPRESSIONS) AS IMPRESSIONS,
    SUM(A.TOTAL_CLICKS) AS TOTAL_CLICKS,
    SUM(A.TOTAL_COST) as TOTAL_COST,
    SUM(B.CONVERSIONS)                                      AS Conversions
   ,COUNT(DISTINCT a.user_id)                               AS total_unique_users
  , SUM(B.ntb)                                              AS total_converted_ntb_users
  --Identified only purchases and product_sales here. This can be expanded to bring the total_purchases and total_product_sales
  , SUM(B.purchases)                                        AS purchases
  , SUM(B.product_sales)                                    AS product_sales
  , SUM(B.ntb_purchases)                                    AS total_ntb_purchases
  , SUM(B.ntb_product_sales)                                AS total_ntb_product_sales
  , BUILT_IN_PARAMETER ('TIME_WINDOW_START') AS time_window_start
  , BUILT_IN_PARAMETER ('TIME_WINDOW_END') AS time_window_end

 --Calculated ROAS values using Sales and Cost information and Click thorugh rate based on Clicks and Impression KPIs. Query can be expanded by adding additional calculated fields
    , SUM(B.product_sales) / SUM(A.total_cost) as ROAS
    , SUM(A.TOTAL_CLICKS )/ SUM(A.IMPRESSIONS) * 100 as CTR
FROM
    IMP_CLK_INFO A
    LEFT JOIN CONVERSIONS B ON A.ADVERTISER = B.ADVERTISER AND A.user_id = b.user_id
    AND A.CAMPAIGN_ID = B.CAMPAIGN_ID
    AND A.device_type = B.device_type
    AND A.dma_code = b.dma_code
    group by 1,2,3,4,5,6,7,8
