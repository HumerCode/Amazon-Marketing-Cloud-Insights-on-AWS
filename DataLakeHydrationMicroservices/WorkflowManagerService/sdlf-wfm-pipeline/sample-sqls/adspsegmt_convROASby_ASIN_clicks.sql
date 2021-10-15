WITH IMP_CLICKSSEG AS
    (SELECT A.ADVERTISER,
         A.behavior_segment_name AS SEGMENT,
         SUM(A.total_cost/100000) AS impression_cost,
         behavior_segment_matched AS MATCHED_SEGMENT,
         SUM(A.IMPRESSIONS) AS IMPRESSIONS,
         SUM(B.clicks) AS CLICKS,
         COUNT(DISTINCT A.USER_ID) AS REACH
    FROM display_impressions_by_user_segments A
    LEFT JOIN DISPLAY_CLICKS B
        ON A.request_tag = B.request_tag
    GROUP BY  A.ADVERTISER, A.behavior_segment_name, A.behavior_segment_matched), USR_SEG_IMP AS
    (SELECT user_id,
         advertiser_id,
         behavior_segment_name,
         behavior_segment_matched,
         sum(total_cost)/100000 AS impression_cost
    FROM display_impressions_by_user_segments
    GROUP BY  user_id, advertiser_id, behavior_segment_name, behavior_segment_matched ), CONV_SEG AS
    (SELECT A.ADVERTISER,
         A.tracked_asin,
         B.behavior_segment_name AS SEGMENT,
         B.behavior_segment_matched AS MATCHED_SEGMENT,
         SUM(A.CONVERSIONS) AS PURCHASES,
         sum(A.product_sales) AS sales_tracked,
         sum(A.total_product_sales) AS sales_tracked_brand,
         ROUND(sum(A.total_product_sales)/sum(B.impression_cost),
        2) AS roas_brand,
         ROUND(sum(A.product_sales)/sum(B.impression_cost),
        2) AS roas_salestracked,
         SUM(B.impression_cost) AS total_cost_fromconvtable
    FROM amazon_attributed_events_by_conversion_time A
    INNER JOIN USR_SEG_IMP B
        ON A.USER_ID = B.USER_ID
            AND A.ADVERTISER_ID = B.ADVERTISER_ID
    WHERE conversion_event_subtype = 'order'
    GROUP BY  A.ADVERTISER, A.tracked_asin, B.behavior_segment_name, B.behavior_segment_matched )
SELECT A.ADVERTISER,
         BUILT_IN_PARAMETER('TIME_WINDOW_START') AS time_window_start, BUILT_IN_PARAMETER('TIME_WINDOW_END') AS time_window_end, A.SEGMENT, A.MATCHED_SEGMENT, A.IMPRESSIONS, A.REACH, B.tracked_asin, B.PURCHASES, B.SALES_TRACKED, B.sales_tracked_brand, A.CLICKS, roas_brand, roas_salestracked, A.impression_cost, (B.PURCHASES/A.REACH) AS conversion_rate_perc, B.total_cost_fromconvtable
FROM IMP_CLICKSSEG A
LEFT JOIN CONV_SEG B
    ON A.ADVERTISER = B.ADVERTISER
        AND A.SEGMENT = B.SEGMENT
        AND A.MATCHED_SEGMENT = B.MATCHED_SEGMENT