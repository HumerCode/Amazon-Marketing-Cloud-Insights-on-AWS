--Audience Analysis Query

--This query provides holistic overview of Advertisers Audience Analysis that includes targeted and untargeted audiences for DSP campaigns
--Note: You can also filter out the desired Campaigns and perform analysis only for those campaigns by adding adding a campaign filter in the where clause

--Identify KPIs from the Display Impressions Table per campaign
WITH IMP_CLICKSSEG AS (
    SELECT
        A.ADVERTISER,
        A.campaign,
        A.behavior_segment_name AS SEGMENT,
        SUM(A.total_cost / 100000) AS impression_cost,
        behavior_segment_matched AS MATCHED_SEGMENT,
        SUM(A.IMPRESSIONS) AS IMPRESSIONS,
        SUM(B.clicks) as CLICKS,
        COUNT(DISTINCT A.USER_ID) AS REACH
    FROM
        display_impressions_by_user_segments A
--request_tag is used to match Display_Clicks and display_impressions_by_user_segments table
    LEFT JOIN DISPLAY_CLICKS B ON A.request_tag = B.request_tag
GROUP BY
    A.ADVERTISER,
    A.campaign,
    A.behavior_segment_name,
    A.behavior_segment_matched),


--Identify user level information per campaign from the display_impressions_by_user_segments
--Note: User level information can only be accessed via sub queries
USR_SEG_IMP as (
    select
        user_id,
        advertiser_id,
        campaign,
        behavior_segment_name,
        behavior_segment_matched,
        sum(total_cost) / 100000 as impression_cost
    from
        display_impressions_by_user_segments
    group by
        user_id,
        campaign,
        advertiser_id,
        behavior_segment_name,
        behavior_segment_matched),


-- Identify Sales information for all users present in the display impressions Table
--You can use either amazon_attributed_events_by_conversion_time or the amazon_attributed_events_by_traffic_time based on your use case
CONV_SEG AS (
    SELECT
        A.ADVERTISER,
        A.campaign,
        A.tracked_asin,
        B.behavior_segment_name AS SEGMENT,
        B.behavior_segment_matched AS MATCHED_SEGMENT,
--Total Conversions, Note: This will be same as Total Product Sales as we have added a condition of conversion_event_subtype = 'order'
        SUM(A.CONVERSIONS) AS PURCHASES,
--Product Sales is total sales (in local currency) of promoted ASINs purchased by customers on Amazon after delivering an ad.
--Total Product Sales is total sales (in local currency) of promoted ASINs and ASINs from the same brands as promoted ASINs purchased by customers on Amazon after delivering an ad.
        sum(A.product_sales) as sales_tracked,
        sum(A.total_product_sales) as sales_tracked_brand,
--Calculated fields
        ROUND(sum(A.total_product_sales) / sum(B.impression_cost), 2) as roas_brand,
        ROUND(sum(A.product_sales) / sum(B.impression_cost), 2) as roas_salestracked,


        SUM(B.impression_cost) AS total_cost_fromconvtable
    From
        amazon_attributed_events_by_conversion_time A
    INNER JOIN USR_SEG_IMP B ON A.USER_ID = B.USER_ID
        AND A.ADVERTISER_ID = B.ADVERTISER_ID
        AND A.campaign = B.campaign
    WHERE
  --This condition only fetches information for purchases made
        conversion_event_subtype = 'order'
    GROUP BY
        A.ADVERTISER,
        A.tracked_asin,
        B.behavior_segment_name,
        B.behavior_segment_matched,
        A.campaign
)

--Identify the columns required to from the above tables to be used in an Audience Analysis Report.
SELECT
    A.ADVERTISER,
    A.campaign,
    BUILT_IN_PARAMETER ('TIME_WINDOW_START') AS time_window_start,
    BUILT_IN_PARAMETER ('TIME_WINDOW_END') AS time_window_end,
    A.SEGMENT,
    A.MATCHED_SEGMENT,
    A.IMPRESSIONS,
    A.REACH,
    B.tracked_asin,
    B.PURCHASES,
    B.SALES_TRACKED,
    B.sales_tracked_brand,
    A.CLICKS,
    roas_brand,
    roas_salestracked,
    A.impression_cost, (B.PURCHASES / A.REACH) as conversion_rate_perc,
    B.total_cost_fromconvtable
FROM
    IMP_CLICKSSEG A
    LEFT JOIN CONV_SEG B ON A.ADVERTISER = B.ADVERTISER
    AND A.SEGMENT = B.SEGMENT
    AND A.MATCHED_SEGMENT = B.MATCHED_SEGMENT
    AND A.campaign = B.campaign
