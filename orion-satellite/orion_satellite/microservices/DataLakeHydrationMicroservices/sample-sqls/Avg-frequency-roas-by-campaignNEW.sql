With IMPCLICK AS
    (SELECT A.ADVERTISER,
         A.CAMPAIGN,
         SUM(A.total_cost/100000) AS TOTAL_COST,
         SUM(A.IMPRESSIONS) AS IMPRESSIONS,
         SUM(B.clicks) AS CLICKS,
         A.USER_ID
    FROM display_impressions A
    LEFT JOIN DISPLAY_CLICKS B
        ON A.request_tag = B.request_tag
    GROUP BY  A.ADVERTISER, A.CAMPAIGN, A.USER_ID), CONV AS
    (SELECT c.advertiser,
         c.user_id,
         c.campaign,
         SUM(c.conversions) AS conversions,
         SUM(c.total_product_sales) AS total_product_sales,
         SUM(c.product_sales) AS product_sales,
         sum(c.total_purchases) AS total_purchases ,
         sum(c.purchases) AS purchases
    FROM amazon_attributed_events_by_conversion_time c
    WHERE c.conversion_event_subtype = 'order'
    GROUP BY  c.advertiser, c.user_id,c.campaign), combined AS
    (SELECT d.advertiser,
         d.campaign,
         sum(d.impressions) AS impressions,
         sum(c.conversions) AS conversions,
         COUNT(DISTINCT d.user_id) AS users_in_bucket,
         sum(c.total_product_sales) AS total_product_sales,
         sum(c.product_sales) AS product_Sales,
         sum(d.TOTAL_COST) AS total_cost,
         ROUND(sum(d.total_cost)/sum(c.total_purchases),
        2) AS CPA_Total_Purchases,
         ROUND(sum(d.total_cost)/sum(c.purchases),
        2) AS CPA_Purchases,
         ROUND(sum(c.total_product_sales)/sum(d.total_cost),
        2) AS ROAS_Totalproductsales,
         sum(d.impressions)/COUNT(DISTINCT d.user_id) AS freq,
         sum(c.total_purchases)/COUNT(DISTINCT d.user_id) AS conversion_rate_total_purchases,
         sum(c.purchases)/COUNT(DISTINCT d.user_id) AS conversion_rate_purchases,
         ROUND(sum(c.product_sales)/sum(d.total_cost),
        2) AS ROAS_Productsales ,
         sum(d.clicks) AS Clicks,
         BUILT_IN_PARAMETER('TIME_WINDOW_START') AS time_window_start, BUILT_IN_PARAMETER('TIME_WINDOW_END') AS time_window_end , sum(total_purchases) AS total_purchases ,sum(purchases) AS purchases
    FROM IMPCLICK d
    LEFT JOIN CONV c
        ON d.advertiser = c.advertiser
            AND d.campaign = c.campaign
            AND d.user_id =c.user_id
    WHERE d.USER_ID IS NOT NULL
    GROUP BY  d.advertiser, d.campaign), CONVERTED_brand AS
    (SELECT DISTINCT user_id
    FROM amazon_attributed_events_by_conversion_time
    WHERE conversion_event_subtype = 'order' ), CONVERTED AS
    (SELECT DISTINCT user_id
    FROM amazon_attributed_events_by_conversion_time
    WHERE conversion_event_subtype = 'order'
            AND purchases >= 1 ), conv_freq_brand AS
    (SELECT a.advertiser,
        a.CAMPAIGN,
         SUM(A.IMPRESSIONS)/COUNT(DISTINCT B.USER_ID) AS conv_FREQUENCY_brand
    FROM DISPLAY_IMPRESSIONS A
    INNER JOIN CONVERTED_brand B
        ON A.USER_ID = B.USER_ID
    WHERE A.USER_ID IS NOT NULL
    GROUP BY  a.advertiser,A.CAMPAIGN) , conv_freq AS
    (SELECT a.advertiser,
        a.CAMPAIGN,
         SUM(A.IMPRESSIONS)/COUNT(DISTINCT B.USER_ID) AS conv_FREQUENCY
    FROM DISPLAY_IMPRESSIONS A
    INNER JOIN CONVERTED B
        ON A.USER_ID = B.USER_ID
    WHERE A.USER_ID IS NOT NULL
    GROUP BY  a.advertiser,A.CAMPAIGN)
SELECT a.advertiser,
        a.campaign,
         a.impressions,
        users_in_bucket,
        Clicks,
         total_cost ,
        conversions,
        a.conversion_rate_purchases,
         a.conversion_rate_total_purchases,
         a.CPA_Purchases,
        a.CPA_Total_Purchases,
         t2.avgfreq,
        t2.avgconvfreq,
        t2.avgconvfrequency_brand,
        total_product_sales,
        (ROAS_Totalproductsales) AS roastotalproductsales,
         (product_sales) AS productsales,
        (ROAS_Productsales) AS roasproductsales,
         (total_purchases) AS totalpurchases,
         purchases,
         BUILT_IN_PARAMETER('TIME_WINDOW_START') AS time_window_start, BUILT_IN_PARAMETER('TIME_WINDOW_END') AS time_window_end
FROM combined a
LEFT JOIN
    (SELECT a.campaign,
         round(avg(freq) ,
        2) AS avgfreq,
         round(avg(conv_FREQUENCY) ,
        2) AS avgconvfreq,
         round(avg(conv_FREQUENCY_brand) ,
        2) AS avgconvfrequency_brand
    FROM combined a
    LEFT JOIN conv_freq_brand b
        ON a.advertiser=b.advertiser
            AND a.CAMPAIGN =b.CAMPAIGN
    LEFT JOIN conv_freq c
        ON a.advertiser=c.advertiser
            AND a.CAMPAIGN =c.CAMPAIGN
    GROUP BY  a.advertiser,a.campaign ) t2
    ON a.campaign = t2.campaign