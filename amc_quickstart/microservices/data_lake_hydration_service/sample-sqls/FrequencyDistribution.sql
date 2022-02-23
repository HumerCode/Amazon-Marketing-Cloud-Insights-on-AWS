--Frequency Distribution Query
--This query provides a breakdown of the  Display campaign performances based on the frequency.

--Note: You can also filter out the desired Campaigns and perform analysis only for those campaigns by adding adding a campaign filter in the where clause
--Gather Impressions and Click Information from display_impressions and display_clicks tables
With IMPCLICK as
(SELECT A.ADVERTISER,
        A.CAMPAIGN,
        A.device_type,
        SUM(A.total_cost/100000)  as TOTAL_COST,
        SUM(A.IMPRESSIONS) AS IMPRESSIONS,
        SUM(B.clicks) as CLICKS,
        A.USER_ID
FROM display_impressions A LEFT JOIN DISPLAY_CLICKS B ON A.request_tag = B.request_tag
GROUP BY A.ADVERTISER, A.CAMPAIGN, A.USER_ID, A.device_type),

--Gather all Sales Information from amazon_attributed_events_by_traffic_time
CONV AS (SELECT c.advertiser,
                c.user_id,
                c.campaign,
                c.device_type,
                SUM(c.conversions) as conversions,
                SUM(c.purchases) as purchases,
                 SUM(c.total_purchases) as total_purchases,
                SUM(c.total_product_sales) AS total_product_sales,
                SUM(c.product_sales) AS product_sales

     FROM amazon_attributed_events_by_traffic_time c
     -- Condition to only extract sales information for purchases made
     WHERE c.conversion_event_subtype = 'order'
     GROUP BY c.advertiser, c.user_id,c.campaign,c.device_type)


--Query to generate a report by combining information from the above tables
SELECT d.advertiser,
    d.campaign,
    d.device_type,

    --Identify the different frequency buckets. This can be modified based on which frequency buckets the customer wants to focus on
   CASE
           WHEN d.impressions BETWEEN 1 AND 5 THEN 'Freq 01 to 05'
           WHEN d.impressions BETWEEN 6 AND 10 THEN 'Freq 06 to 10'
           WHEN d.impressions BETWEEN 11 AND 15 THEN 'Freq 11 to 15'
           WHEN d.impressions BETWEEN 16 AND 20 THEN 'Freq 16 to 20'
           WHEN d.impressions BETWEEN 21 AND 25 THEN 'Freq 21 to 25'
           WHEN d.impressions BETWEEN 26 AND 30 THEN 'Freq 26 to 30'
           WHEN d.impressions BETWEEN 31 AND 35 THEN 'Freq 31 to 35'
           ELSE 'Freq 35+' END as frequency_buckets,
         COUNT(DISTINCT d.user_id) AS users_in_bucket,
         sum(d.impressions) as impressions,
         sum(d.TOTAL_COST) as total_cost,
         sum(c.conversions) as conversions,
         sum(c.purchases) as purchases,
         sum(c.total_purchases) as total_purchases,

         --Product Sales is total sales (in local currency) of promoted ASINs purchased by customers on Amazon after delivering an ad.
         --Total Product Sales is The total sales (in local currency) of promoted ASINs and ASINs from the same brands as promoted ASINs purchased by customers on Amazon after delivering an ad.
         SUM(c.product_sales) AS product_sales,
         sum(c.total_product_sales) as total_product_sales,

      --Calculated ROAS values using Sales and Cost information. Query can be expanded by adding additional calculated fields
         ROUND(sum(c.total_product_sales)/sum(d.total_cost),2) as ROAS_Totalproductsales,
         ROUND(sum(c.product_sales)/sum(d.total_cost),2) as ROAS_Productsales
         ,sum(c.total_purchases)/COUNT(DISTINCT d.user_id) as conversion_rate_total_purchases
         ,sum(c.purchases)/COUNT(DISTINCT d.user_id)    as conversion_rate_purchases
         ,sum(c.total_purchases)/SUM(d.impressions)                      as conversion_rate_impressions_total_purchases
         ,sum(c.purchases)/SUM(d.impressions)                      as conversion_rate_impressions_purchases
         ,sum(d.clicks) as Clicks,
         BUILT_IN_PARAMETER('TIME_WINDOW_START') AS time_window_start,
         BUILT_IN_PARAMETER('TIME_WINDOW_END') AS time_window_end
 FROM IMPCLICK d
LEFT JOIN CONV c on d.advertiser = c.advertiser and d.campaign = c.campaign
and d.user_id =c.user_id  and d.device_type =c.device_type
GROUP BY d.advertiser,
d.campaign, frequency_buckets,d.device_type
