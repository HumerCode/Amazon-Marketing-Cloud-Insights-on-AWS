WITH user_exposure AS
    (SELECT A.user_id,
         A.campaign,
         A.Advertiser,
         0 AS phone_purch,
         0 AS tablet_purch,
         0 AS TV_purch,
         0 AS PC_purch,
         0 AS other_purch,
         0 AS phone_rev,
         0 AS tablet_rev,
         0 AS TV_rev,
         0 AS PC_rev,
         0 AS other_rev,
         SUM(
        CASE
        WHEN A.device_type = 'Phone' THEN
        A.impressions
        ELSE 0
        END ) AS phone_imp, SUM(
        CASE
        WHEN A.device_type = 'Tablet' THEN
        A.impressions
        ELSE 0
        END ) AS tablet_imp, SUM(
        CASE
        WHEN A.device_type = 'TV' THEN
        A.impressions
        ELSE 0
        END ) AS TV_imp, SUM(
        CASE
        WHEN A.device_type = 'PC' THEN
        A.impressions
        ELSE 0
        END ) AS PC_imp, SUM(
        CASE
        WHEN A.device_type != 'PC'
            AND A.device_type != 'TV'
            AND A.device_type != 'Phone'
            AND A.device_type != 'Tablet' THEN
        A.impressions
        ELSE 0
        END ) AS other_imp, SUM(
        CASE
        WHEN A.device_type = 'Phone' THEN
        A.total_cost
        ELSE 0
        END ) AS phone_cost, SUM(
        CASE
        WHEN A.device_type = 'Tablet' THEN
        A.total_cost
        ELSE 0
        END ) AS tablet_cost, SUM(
        CASE
        WHEN A.device_type = 'TV' THEN
        A.total_cost
        ELSE 0
        END ) AS TV_cost, SUM(
        CASE
        WHEN A.device_type = 'PC' THEN
        A.total_cost
        ELSE 0
        END ) AS PC_cost, SUM(
        CASE
        WHEN A.device_type != 'PC'
            AND A.device_type != 'TV'
            AND A.device_type != 'Phone'
            AND A.device_type != 'Tablet' THEN
        A.total_cost
        ELSE 0
        END ) AS other_cost,0 AS phone_clicks, 0 AS tablet_clicks, 0 AS TV_clicks, 0 AS PC_clicks, 0 AS other_clicks
    FROM display_impressions A
    GROUP BY  user_id, campaign, Advertiser
    UNION ALL
    SELECT B.user_id,
         campaign,
         Advertiser,
        0 AS phone_purch,
         0 AS tablet_purch,
         0 AS TV_purch,
         0 AS PC_purch,
         0 AS other_purch,
         0 AS phone_rev,
         0 AS tablet_rev,
         0 AS TV_rev,
         0 AS PC_rev,
         0 AS other_rev,
         0 AS phone_imp,
         0 AS tablet_imp,
         0 AS TV_imp,
         0 AS PC_imp,
         0 AS other_imp,
         0 AS phone_cost,
         0 AS tablet_cost,
         0 AS TV_cost,
         0 AS PC_cost,
         0 AS other_cost,
         SUM(
        CASE
        WHEN B.device_type = 'Phone' THEN
        B.clicks
        ELSE 0
        END ) AS phone_clicks, SUM(
        CASE
        WHEN B.device_type = 'Tablet' THEN
        B.clicks
        ELSE 0
        END ) AS tablet_clicks, SUM(
        CASE
        WHEN B.device_type = 'TV' THEN
        B.clicks
        ELSE 0
        END ) AS TV_clicks, SUM(
        CASE
        WHEN B.device_type = 'PC' THEN
        B.clicks
        ELSE 0
        END ) AS PC_clicks, SUM(
        CASE
        WHEN B.device_type != 'PC'
            AND B.device_type != 'TV'
            AND B.device_type != 'Phone'
            AND B.device_type != 'Tablet' THEN
        B.clicks
        ELSE 0
        END ) AS other_clicks
    FROM DISPLAY_CLICKS B
    GROUP BY  user_id, campaign, Advertiser
    UNION ALL
    SELECT user_id,
         campaign,
         Advertiser,
         SUM(
        CASE
        WHEN device_type = 'Phone' THEN
        purchases
        ELSE 0
        END ) AS phone_purch, SUM(
        CASE
        WHEN device_type = 'Tablet' THEN
        purchases
        ELSE 0
        END ) AS tablet_purch, SUM(
        CASE
        WHEN device_type = 'TV' THEN
        purchases
        ELSE 0
        END ) AS TV_purch, SUM(
        CASE
        WHEN device_type = 'PC' THEN
        purchases
        ELSE 0
        END ) AS PC_purch, SUM(
        CASE
        WHEN device_type != 'PC'
            AND device_type != 'TV'
            AND device_type != 'Phone'
            AND device_type != 'Tablet' THEN
        purchases
        ELSE 0
        END ) AS other_purch, SUM(
        CASE
        WHEN device_type = 'Phone' THEN
        product_sales
        ELSE 0
        END ) AS phone_rev, SUM(
        CASE
        WHEN device_type = 'Tablet' THEN
        product_sales
        ELSE 0
        END ) AS tablet_rev, SUM(
        CASE
        WHEN device_type = 'TV' THEN
        product_sales
        ELSE 0
        END ) AS TV_rev, SUM(
        CASE
        WHEN device_type = 'PC' THEN
        product_sales
        ELSE 0
        END ) AS PC_rev, SUM(
        CASE
        WHEN device_type != 'PC'
            AND device_type != 'TV'
            AND device_type != 'Phone'
            AND device_type != 'Tablet' THEN
        product_sales
        ELSE 0
        END ) AS other_rev, 0 AS phone_imp, 0 AS tablet_imp, 0 AS TV_imp, 0 AS PC_imp, 0 AS other_imp, 0 AS phone_cost, 0 AS tablet_cost, 0 AS TV_cost, 0 AS PC_cost, 0 AS other_cost ,0 AS phone_clicks, 0 AS tablet_clicks, 0 AS TV_clicks, 0 AS PC_clicks, 0 AS other_clicks
    FROM amazon_attributed_events_by_traffic_time
    GROUP BY  user_id,campaign,Advertiser), aggregated AS
    (SELECT user_id,
         campaign,
         Advertiser,
         sum(phone_purch) AS phone_purch,
         sum(tablet_purch) AS tablet_purch,
         sum(TV_purch) AS TV_purch,
         sum(PC_purch) AS PC_purch,
         sum(other_purch) AS other_purch,
         sum(phone_rev) AS phone_rev,
         sum(tablet_rev) AS tablet_rev,
         sum(TV_rev) AS TV_rev,
         sum(PC_rev) AS PC_rev,
         sum(other_rev) AS other_rev,
         sum(phone_imp) AS phone_imp,
         sum(tablet_imp) AS tablet_imp,
         sum(TV_imp) AS TV_imp,
         sum(PC_imp) AS PC_imp,
         sum(other_imp) AS other_imp,
         sum(phone_cost) AS phone_cost,
         sum(tablet_cost) AS tablet_cost,
         sum(TV_cost) AS TV_cost,
         sum(PC_cost) AS PC_cost,
         sum(other_cost) AS other_cost ,
        sum(phone_clicks) AS phone_clicks,
         sum(tablet_clicks) AS tablet_clicks,
         sum(TV_clicks) AS TV_clicks,
         sum(PC_clicks) AS PC_clicks,
         sum(other_clicks) AS other_clicks
    FROM user_exposure
    GROUP BY  user_id ,campaign, Advertiser)
SELECT Advertiser,
        campaign,
         BUILT_IN_PARAMETER('TIME_WINDOW_START') AS time_window_start, BUILT_IN_PARAMETER('TIME_WINDOW_END') AS time_window_end ,CASE
    WHEN (phone_imp > 0
        OR phone_purch > 0)
        AND tablet_imp = 0
        AND tablet_purch = 0
        AND PC_imp = 0
        AND PC_purch = 0
        AND TV_imp = 0
        AND TV_purch = 0
        AND other_imp = 0
        AND other_purch = 0 THEN
    'phone_only'
    WHEN phone_imp = 0
        AND phone_purch = 0
        AND (tablet_imp > 0
        OR tablet_purch > 0)
        AND PC_imp = 0
        AND PC_purch = 0
        AND TV_imp = 0
        AND TV_purch = 0
        AND other_imp = 0
        AND other_purch = 0 THEN
    'tablet_only'
    WHEN phone_imp = 0
        AND phone_purch = 0
        AND tablet_imp = 0
        AND tablet_purch = 0
        AND (PC_imp > 0
        OR PC_purch > 0)
        AND TV_imp = 0
        AND TV_purch = 0
        AND other_imp = 0
        AND other_purch = 0 THEN
    'PC_only'
    WHEN phone_imp = 0
        AND phone_purch = 0
        AND tablet_imp = 0
        AND tablet_purch = 0
        AND PC_imp = 0
        AND PC_purch = 0
        AND (TV_imp > 0
        OR TV_purch > 0)
        AND other_imp = 0
        AND other_purch = 0 THEN
    'TV_only'
    WHEN (phone_imp > 0
        OR phone_purch > 0)
        AND tablet_imp = 0
        AND tablet_purch = 0
        AND PC_imp = 0
        AND PC_purch = 0
        AND (TV_imp > 0
        OR TV_purch > 0)
        AND other_imp = 0
        AND other_purch = 0 THEN
    'Phone_and_TV'
    WHEN phone_imp = 0
        AND phone_purch = 0
        AND tablet_imp = 0
        AND tablet_purch = 0
        AND (PC_imp > 0
        OR PC_purch > 0)
        AND (TV_imp > 0
        OR TV_purch > 0)
        AND other_imp = 0
        AND other_purch = 0 THEN
    'PC_and_TV'
    WHEN phone_imp = 0
        AND phone_purch = 0
        AND (tablet_imp > 0
        OR tablet_purch > 0)
        AND PC_imp = 0
        AND PC_purch = 0
        AND (TV_imp > 0
        OR TV_purch > 0)
        AND other_imp = 0
        AND other_purch = 0 THEN
    'Tablet_and_TV'
    WHEN (phone_imp > 0
        OR phone_purch > 0)
        AND tablet_imp = 0
        AND tablet_purch = 0
        AND (PC_imp > 0
        OR PC_purch > 0)
        AND TV_imp = 0
        AND TV_purch = 0
        AND other_imp = 0
        AND other_purch = 0 THEN
    'PC_and_Phone'
    WHEN (phone_imp > 0
        OR phone_purch > 0)
        AND (tablet_imp > 0
        OR tablet_purch > 0)
        AND PC_imp = 0
        AND PC_purch = 0
        AND TV_imp = 0
        AND TV_purch = 0
        AND other_imp = 0
        AND other_purch = 0 THEN
    'Tablet_and_Phone'
    WHEN phone_imp = 0
        AND phone_purch = 0
        AND (tablet_imp > 0
        OR tablet_purch > 0)
        AND (PC_imp > 0
        OR PC_purch > 0)
        AND TV_imp = 0
        AND TV_purch = 0
        AND other_imp = 0
        AND other_purch = 0 THEN
    'PC_and_Tablet'
    WHEN (phone_imp > 0
        OR phone_purch > 0)
        AND tablet_imp = 0
        AND tablet_purch = 0
        AND (PC_imp > 0
        OR PC_purch > 0)
        AND (TV_imp > 0
        OR TV_purch > 0)
        AND other_imp = 0
        AND other_purch = 0 THEN
    'Phone_and_TV_and_PC'
    WHEN (phone_imp > 0
        OR phone_purch > 0)
        AND (tablet_imp > 0
        OR tablet_purch > 0)
        AND PC_imp = 0
        AND PC_purch = 0
        AND (TV_imp > 0
        OR TV_purch > 0)
        AND other_imp = 0
        AND other_purch = 0 THEN
    'Phone_and_TV_and_Tablet'
    WHEN phone_imp = 0
        AND phone_purch = 0
        AND (tablet_imp > 0
        OR tablet_purch > 0)
        AND (PC_imp > 0
        OR PC_purch > 0)
        AND (TV_imp > 0
        OR TV_purch > 0)
        AND other_imp = 0
        AND other_purch = 0 THEN
    'PC_and_TV_and_Tablet'
    WHEN (phone_imp > 0
        OR phone_purch > 0)
        AND (tablet_imp > 0
        OR tablet_purch > 0)
        AND (PC_imp > 0
        OR PC_purch > 0)
        AND TV_imp = 0
        AND TV_purch = 0
        AND other_imp = 0
        AND other_purch = 0 THEN
    'Phone_and_PC_and_Tablet'
    WHEN (phone_imp > 0
        OR phone_purch > 0)
        AND (tablet_imp > 0
        OR tablet_purch > 0)
        AND (PC_imp > 0
        OR PC_purch > 0)
        AND (TV_imp > 0
        OR TV_purch > 0)
        AND other_imp = 0
        AND other_purch = 0 THEN
    'PC_and_TV_and_Tablet_and_Phone'
    ELSE 'NA'
    END AS exposure_group, SUM(phone_imp) AS phone_impressions, SUM(tablet_imp) AS tablet_impressions, SUM(TV_imp) AS TV_impressions, SUM(PC_imp) AS PC_impressions, SUM(other_imp) AS other_impressions, (SUM(phone_imp)+SUM(tablet_imp)+SUM(TV_imp)+SUM(PC_imp)+SUM(other_imp)) AS exposure_group_imp, SUM(phone_cost)/100000 AS phone_cost, SUM(tablet_cost)/100000 AS tablet_cost, SUM(TV_cost)/100000 AS TV_cost, SUM(PC_cost)/100000 AS PC_cost, SUM(other_cost)/100000 AS other_cost, ((SUM(phone_cost)+SUM(tablet_cost)+SUM(TV_cost)+SUM(PC_cost)+SUM(other_cost))/100000) AS exposure_group_cost, SUM(phone_clicks) AS phone_clicks, SUM(tablet_clicks) AS tablet_clicks, SUM(TV_clicks) AS TV_clicks, SUM(PC_clicks) AS PC_clicks, SUM(other_clicks) AS other_clicks, ((SUM(phone_clicks)+SUM(tablet_clicks)+SUM(TV_clicks)+SUM(PC_clicks)+SUM(other_clicks))) AS exposure_group_clicks, SUM(phone_purch) AS phone_purchases, SUM(tablet_purch) AS tablet_purchases, SUM(TV_purch) AS TV_purchases, SUM(PC_purch) AS PC_purchases, SUM(other_purch) AS other_purchases, (SUM(phone_purch)+SUM(tablet_purch)+SUM(TV_purch)+SUM(PC_purch)+SUM(other_purch)) AS exposure_group_purchases, SUM(phone_rev) AS phone_revenue, SUM(tablet_rev) AS tablet_revenue, SUM(TV_rev) AS TV_revenue, SUM(PC_rev) AS PC_revenue, SUM(other_rev) AS other_revenue, (SUM(phone_rev)+SUM(tablet_rev)+SUM(TV_rev)+SUM(PC_rev)+SUM(other_rev)) AS exposure_group_revenue, SUM(phone_rev)/(SUM(phone_cost)/100000) AS phone_roas, SUM(tablet_rev)/(SUM(tablet_cost)/100000) AS tablet_roas, SUM(TV_rev)/(SUM(TV_cost)/100000) AS TV_roas, SUM(PC_rev)/(SUM(PC_cost)/100000) AS PC_roas, SUM(other_rev)/(SUM(other_cost)/100000) AS Other_roas, (SUM(phone_rev)+SUM(tablet_rev)+SUM(TV_rev)+SUM(PC_rev)+SUM(other_rev))/((SUM(phone_cost)+SUM(tablet_cost) +SUM(TV_cost)+SUM(PC_cost)+SUM(other_cost))/100000) AS exposure_group_roas
FROM aggregated
GROUP BY  exposure_group, campaign, Advertiser