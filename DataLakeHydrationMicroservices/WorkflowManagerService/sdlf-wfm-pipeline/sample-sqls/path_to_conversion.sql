WITH ranked AS
    (SELECT NAMED_ROW('device', a.device_type) AS device, a.user_id
    FROM display_impressions a
    WHERE user_id is NOT null), assembled AS
    (SELECT ARRAY_SORT(COLLECT(distinct a.device)) AS path,
         a.user_id
    FROM ranked a
    GROUP BY  a.user_id ), impressions AS
    (SELECT user_id,
         sum(impressions) AS impressions,
         sum(total_cost) AS total_cost
    FROM display_impressions
    GROUP BY  user_id ), clicks AS
    (SELECT user_id,
         sum(clicks) AS clicks
    FROM display_clicks
    GROUP BY  user_id ), converted AS
    (SELECT user_id,
         sum(product_sales) AS product_sales,
         sum(purchases) AS purchases,
         sum(total_product_sales) AS total_product_sales
    FROM amazon_attributed_events_by_traffic_time
    WHERE conversion_event_subtype = 'order'
            AND user_id is NOT null
    GROUP BY  user_id ), assembled_with_imp_conv AS
    (SELECT path,
         count(distinct a.user_id) AS reach,
         sum(b.impressions) AS impressions,
         sum(b.total_cost)/100000 AS imp_total_cost,
         SUM(cl.CLICKS) AS TOTAL_CLICKS,
         count(distinct c.user_id) AS conversions,
         sum(c.product_sales) AS sales_amount,
         sum(c.purchases) AS purchases,
         sum(c.total_product_sales) AS sales_amount_brand
    FROM assembled a
    LEFT JOIN impressions b
        ON a.user_id = b.user_id
    LEFT JOIN clicks cl
        ON a.user_id = cl.user_id
    LEFT JOIN converted c
        ON a.user_id = c.user_id
    GROUP BY  path)
SELECT path,
         reach AS path_occurrences,
         impressions,
         imp_total_cost,
         conversions,
         sales_amount,
         sales_amount_brand,
         purchases,
         (purchases/reach) AS conversion_rate_perc_target,
         (conversions/reach) AS conversion_rate_perc_all,
         TOTAL_CLICKS,
         (sales_amount_brand/imp_total_cost) AS ROAS_sales_amount_brand ,
         (sales_amount/imp_total_cost) AS ROAS_sales_amount,
         BUILT_IN_PARAMETER('TIME_WINDOW_START') AS time_window_start, BUILT_IN_PARAMETER('TIME_WINDOW_END') AS time_window_end
FROM assembled_with_imp_conv