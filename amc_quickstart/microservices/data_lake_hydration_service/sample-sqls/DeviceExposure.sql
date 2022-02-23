--Device Exposure Query
--This query provides a holistic view of customer’s path to conversion based on different devices vs combination of devices (PC, TV, Mobile, Tablet) for DSP campaigns

--Note: You can also filter out the desired Campaigns and perform analysis only for those campaigns by adding adding a campaign filter in the where clause

--Perform a union all from three tables to collect KPIs from the display_impressions,display_clicks and amazon_attributed_events_by_traffic_time tables
WITH user_exposure AS (
    (
    SELECT  'dsp' as ad_product_type,
     A.user_id,
     A.campaign,
     A.Advertiser,
     1 as user_count,
     0 as new_to_brand,
     0 as DPV_dsp,
     0 as unique_dpv,
     0 as purchases,
     0 as unique_purchases,
     0 as phone_purch,
     0 as tablet_purch,
     0 as TV_purch,
     0 as PC_purch,
     0 as other_purch,
     0.0 as phone_rev,
     0.0 as tablet_rev,
     0.0 as TV_rev,
     0.0 as PC_rev,
     0.0 as other_rev,
--The case statements are used to identify the different device types the user was exposed to
     SUM( CASE WHEN A.device_type = 'Phone' THEN A.impressions ELSE 0 END ) as phone_imp,
     SUM( CASE WHEN A.device_type = 'Tablet' THEN A.impressions ELSE 0 END ) as tablet_imp,
     SUM( CASE WHEN A.device_type = 'TV' THEN A.impressions ELSE 0 END ) as TV_imp,
     SUM( CASE WHEN A.device_type = 'PC' THEN A.impressions ELSE 0 END ) as PC_imp,
     SUM( CASE WHEN A.device_type != 'PC' AND A.device_type != 'TV' AND A.device_type != 'Phone' AND A.device_type != 'Tablet' THEN A.impressions ELSE 0 END ) as other_imp,
     SUM( CASE WHEN A.device_type = 'Phone' THEN A.total_cost ELSE 0.0 END ) as phone_cost,
     SUM( CASE WHEN A.device_type = 'Tablet' THEN A.total_cost ELSE 0.0 END ) as tablet_cost,
     SUM( CASE WHEN A.device_type = 'TV' THEN A.total_cost ELSE 0.0 END ) as TV_cost,
     SUM( CASE WHEN A.device_type = 'PC' THEN A.total_cost ELSE 0.0 END ) as PC_cost,
     SUM( CASE WHEN A.device_type != 'PC' AND A.device_type != 'TV' AND A.device_type != 'Phone' AND A.device_type != 'Tablet' THEN A.total_cost ELSE 0.0 END ) as other_cost,
     0 as phone_clicks,
     0 as tablet_clicks,
     0 as TV_clicks,
     0 as PC_clicks,
     0 as other_clicks
     FROM display_impressions A
     WHERE user_id is not null
     GROUP BY user_id,campaign,Advertiser
     )
 UNION ALL
     (
     SELECT  'dsp' as ad_product_type,
     B.user_id,
     campaign,
     Advertiser,
     1 as user_count,
     0 as new_to_brand,
     0 as DPV_dsp,
     0 as unique_dpv,
     0 as purchases,
     0 as unique_purchases,
     0 as phone_purch,
     0 as tablet_purch,
     0 as TV_purch,
     0 as PC_purch,
     0 as other_purch,
     0.0 as phone_rev,
     0.0 as tablet_rev,
     0.0 as TV_rev,
     0.0 as PC_rev,
     0.0 as other_rev,
     0 as phone_imp,
     0 as tablet_imp,
     0 as TV_imp,
     0 as PC_imp,
     0 as other_imp,
     0.0 as phone_cost,
     0.0 as tablet_cost,
     0.0 as TV_cost,
     0.0 as PC_cost,
     0.0 as other_cost,
     SUM( CASE WHEN B.device_type = 'Phone' THEN B.clicks ELSE 0 END ) as phone_clicks,
     SUM( CASE WHEN B.device_type = 'Tablet' THEN B.clicks ELSE 0 END ) as tablet_clicks,
     SUM( CASE WHEN B.device_type = 'TV' THEN B.clicks ELSE 0 END ) as TV_clicks,
     SUM( CASE WHEN B.device_type = 'PC' THEN B.clicks ELSE 0 END ) as PC_clicks,
     SUM( CASE WHEN B.device_type != 'PC' AND B.device_type != 'TV' AND B.device_type != 'Phone' AND B.device_type != 'Tablet' THEN B.clicks ELSE 0 END ) as other_clicks
     from display_clicks B
     WHERE user_id is not null
     GROUP BY user_id,campaign,Advertiser
     )
     UNION ALL
     (
     SELECT  COALESCE(ad_product_type,'dsp'),
     user_id,
     campaign,
     Advertiser,
     1 as user_count,
     SUM(new_to_brand_purchases) as new_to_brand,
     SUM(case when conversion_event_subtype = 'detailPageView' then 1 else 0 end) as DPV_dsp,
     case when SUM(case when conversion_event_subtype = 'detailPageView' then 1 else 0 end) > 0 then 1 else 0 end as unique_dpv,
     SUM(purchases) as purchases,
     case when SUM(purchases) > 0 then 1 else 0 end as unique_purchases,
     --Identified only purchases and product_sales here. This can be expanded to bring the total_purchases and total_product_sales
     SUM( CASE WHEN device_type = 'Phone' THEN purchases ELSE 0 END ) as phone_purch,
     SUM( CASE WHEN device_type = 'Tablet' THEN purchases ELSE 0 END ) as tablet_purch,
     SUM( CASE WHEN device_type = 'TV' THEN purchases ELSE 0 END ) as TV_purch,
     SUM( CASE WHEN device_type = 'PC' THEN purchases ELSE 0 END ) as PC_purch,
     SUM( CASE WHEN device_type != 'PC' AND device_type != 'TV' AND device_type != 'Phone' AND device_type != 'Tablet' THEN purchases ELSE 0 END ) as other_purch,
     SUM( CASE WHEN device_type = 'Phone' THEN product_sales ELSE 0.0 END ) as phone_rev,
     SUM( CASE WHEN device_type = 'Tablet' THEN product_sales ELSE 0.0 END ) as tablet_rev,
     SUM( CASE WHEN device_type = 'TV' THEN product_sales ELSE 0.0 END ) as TV_rev,
     SUM( CASE WHEN device_type = 'PC' THEN product_sales ELSE 0.0 END ) as PC_rev,
     SUM( CASE WHEN device_type != 'PC' AND device_type != 'TV' AND device_type != 'Phone' AND device_type != 'Tablet' THEN product_sales ELSE 0.0 END ) as other_rev,
     0 as phone_imp,
     0 as tablet_imp,
     0 as TV_imp,
     0 as PC_imp,
     0 as other_imp,
     0.0 as phone_cost,
     0.0 as tablet_cost,
     0.0 as TV_cost,
     0.0 as PC_cost,
     0.0 as other_cost ,
     0 as phone_clicks,
     0 as tablet_clicks,
     0 as TV_clicks,
     0 as PC_clicks,
     0 as other_clicks
     FROM amazon_attributed_events_by_traffic_time
     WHERE user_id is not null
     and ad_product_type is null
     GROUP BY user_id,ad_product_type,campaign,Advertiser)
),


--Combine the values from the above table and identify the pre aggregated values
pre_aggregated as (
select case when ad_product_type='dsp' THEN 'dsp' else '' end as ad_product_type,
 user_id,
 campaign,
 Advertiser,
 SUM(user_count) as reach,
 SUM(new_to_brand) as new_to_brand,
 SUM(DPV_dsp) as DPV_dsp,
 SUM(unique_dpv) as unique_dpv,
 SUM(purchases) as purchases,
 SUM(unique_purchases) as unique_purchases,
 SUM(phone_purch) as phone_purch,
 SUM(tablet_purch) as tablet_purch,
 SUM(TV_purch) as TV_purch,
 SUM(PC_purch) as PC_purch,
 SUM(other_purch) as other_purch,
 SUM(phone_rev) as phone_rev,
 SUM(tablet_rev) as tablet_rev,
 SUM(TV_rev) as TV_rev,
 SUM(PC_rev) as PC_rev,
 SUM(cast(other_rev as double)) as other_rev,
 SUM(phone_imp) as phone_imp,
 SUM(tablet_imp) as tablet_imp,
 SUM(TV_imp) as TV_imp,
 SUM(PC_imp) as PC_imp,
 SUM(other_imp) as other_imp,
 SUM(phone_cost) as phone_cost,
 SUM(tablet_cost) as tablet_cost,
 SUM(TV_cost) as TV_cost,
 SUM(PC_cost) as PC_cost,
 SUM(other_cost) as other_cost ,
 SUM(phone_clicks) as phone_clicks,
 SUM(tablet_clicks) as tablet_clicks,
 SUM(TV_clicks) as TV_clicks,
 SUM(PC_clicks) as PC_clicks,
 SUM(other_clicks) as other_clicks
 from user_exposure
 group by ad_product_type, user_id, campaign, Advertiser
 ),

--Identify the exposure group using the case statements and KPIs per exposure group and users
aggregated as (
select ad_product_type,
 user_id,
 campaign,
 Advertiser,
 reach,
 new_to_brand,
 DPV_dsp,
 unique_dpv,
 purchases,
 unique_purchases,
 CASE
    WHEN (phone_imp > 0 OR phone_purch > 0) AND tablet_imp = 0 AND tablet_purch = 0 AND PC_imp = 0 AND PC_purch = 0 AND TV_imp = 0 AND TV_purch = 0 AND other_imp = 0 AND other_purch = 0 THEN 'phone_only'
    WHEN phone_imp = 0 AND phone_purch = 0 AND (tablet_imp > 0 OR tablet_purch > 0) AND PC_imp = 0 AND PC_purch = 0 AND TV_imp = 0 AND TV_purch = 0 AND other_imp = 0 AND other_purch = 0 THEN 'tablet_only'
    WHEN phone_imp = 0 AND phone_purch = 0 AND tablet_imp = 0 AND tablet_purch = 0 AND (PC_imp > 0 OR PC_purch > 0) AND TV_imp = 0 AND TV_purch = 0 AND other_imp = 0 AND other_purch = 0 THEN 'PC_only'
    WHEN phone_imp = 0 AND phone_purch = 0 AND tablet_imp = 0 AND tablet_purch = 0 AND PC_imp = 0 AND PC_purch = 0 AND (TV_imp > 0 OR TV_purch > 0) AND other_imp = 0 AND other_purch = 0 THEN 'TV_only'
    WHEN (phone_imp > 0 OR phone_purch > 0) AND tablet_imp = 0 AND tablet_purch = 0 AND PC_imp = 0 AND PC_purch = 0 AND (TV_imp > 0 OR TV_purch > 0) AND other_imp = 0 AND other_purch = 0 THEN 'Phone_and_TV'
    WHEN phone_imp = 0 AND phone_purch = 0 AND tablet_imp = 0 AND tablet_purch = 0 AND (PC_imp > 0 OR PC_purch > 0) AND (TV_imp > 0 OR TV_purch > 0) AND other_imp = 0 AND other_purch = 0 THEN 'PC_and_TV'
    WHEN phone_imp = 0 AND phone_purch = 0 AND (tablet_imp > 0 OR tablet_purch > 0) AND PC_imp = 0 AND PC_purch = 0 AND (TV_imp > 0 OR TV_purch > 0) AND other_imp = 0 AND other_purch = 0 THEN 'Tablet_and_TV'
    WHEN (phone_imp > 0 OR phone_purch > 0) AND tablet_imp = 0 AND tablet_purch = 0 AND (PC_imp > 0 OR PC_purch > 0) AND TV_imp = 0 AND TV_purch = 0 AND other_imp = 0 AND other_purch = 0 THEN 'PC_and_Phone'
    WHEN (phone_imp > 0 OR phone_purch > 0) AND (tablet_imp > 0 OR tablet_purch > 0) AND PC_imp = 0 AND PC_purch = 0 AND TV_imp = 0 AND TV_purch = 0 AND other_imp = 0 AND other_purch = 0 THEN 'Tablet_and_Phone'
    WHEN phone_imp = 0 AND phone_purch = 0 AND (tablet_imp > 0 OR tablet_purch > 0) AND (PC_imp > 0 OR PC_purch > 0) AND TV_imp = 0 AND TV_purch = 0 AND other_imp = 0 AND other_purch = 0 THEN 'PC_and_Tablet'
    WHEN (phone_imp > 0 OR phone_purch > 0) AND tablet_imp = 0 AND tablet_purch = 0 AND (PC_imp > 0 OR PC_purch > 0) AND (TV_imp > 0 OR TV_purch > 0) AND other_imp = 0 AND other_purch = 0 THEN 'Phone_and_TV_and_PC'
    WHEN (phone_imp > 0 OR phone_purch > 0) AND (tablet_imp > 0 OR tablet_purch > 0) AND PC_imp = 0 AND PC_purch = 0 AND (TV_imp > 0 OR TV_purch > 0) AND other_imp = 0 AND other_purch = 0 THEN 'Phone_and_TV_and_Tablet'
    WHEN phone_imp = 0 AND phone_purch = 0 AND (tablet_imp > 0 OR tablet_purch > 0) AND (PC_imp > 0 OR PC_purch > 0) AND (TV_imp > 0 OR TV_purch > 0) AND other_imp = 0 AND other_purch = 0 THEN 'PC_and_TV_and_Tablet'
    WHEN (phone_imp > 0 OR phone_purch > 0) AND (tablet_imp > 0 OR tablet_purch > 0) AND (PC_imp > 0 OR PC_purch > 0) AND TV_imp = 0 AND TV_purch = 0 AND other_imp = 0 AND other_purch = 0 THEN 'Phone_and_PC_and_Tablet'
    WHEN (phone_imp > 0 OR phone_purch > 0) AND (tablet_imp > 0 OR tablet_purch > 0) AND (PC_imp > 0 OR PC_purch > 0) AND (TV_imp > 0 OR TV_purch > 0) AND other_imp = 0 AND other_purch = 0 THEN 'PC_and_TV_and_Tablet_and_Phone'
ELSE 'NA' END as exposure_group,
 phone_purch,
 tablet_purch,
 TV_purch,
 PC_purch,
 other_purch,
 phone_rev,
 tablet_rev,
 TV_rev,
 PC_rev,
 other_rev,
 phone_imp,
 tablet_imp,
 TV_imp,
 PC_imp,
 other_imp,
 phone_cost,
 tablet_cost,
 TV_cost,
 PC_cost,
 other_cost ,
 phone_clicks,
 tablet_clicks,
 TV_clicks,
 PC_clicks,
 other_clicks
  from pre_aggregated
)


--Build a report to extract all the aggregated value per exposure group for DSP campaigns
SELECT ad_product_type,
 Advertiser,
 campaign,
 exposure_group,
 BUILT_IN_PARAMETER('TIME_WINDOW_START') AS time_window_start,
 BUILT_IN_PARAMETER('TIME_WINDOW_END') AS time_window_end,
 SUM(reach) as reach,
 sum(new_to_brand) as new_to_brand,
 SUM(DPV_dsp) as DPV_dsp,
 SUM(unique_dpv) as unique_dpv,
 SUM(purchases) as purchases,
 SUM(unique_purchases) as unique_purchases,
 SUM(phone_imp) as phone_impressions,
 SUM(tablet_imp) as tablet_impressions,
 SUM(TV_imp) as TV_impressions,
 SUM(PC_imp) as PC_impressions,
 SUM(other_imp) as other_impressions,
 (SUM(phone_imp)+SUM(tablet_imp)+SUM(TV_imp)+SUM(PC_imp)+SUM(other_imp)) as exposure_group_imp,
 SUM(phone_cost)/100000 as phone_cost,
 SUM(tablet_cost)/100000 as tablet_cost,
 SUM(TV_cost)/100000 as TV_cost,
 SUM(PC_cost)/100000 as PC_cost,
 SUM(other_cost)/100000 as other_cost,
 ((SUM(phone_cost)+SUM(tablet_cost)+SUM(TV_cost)+SUM(PC_cost)+SUM(other_cost))/100000) as exposure_group_cost,
 SUM(phone_clicks) as phone_clicks,
 SUM(tablet_clicks) as tablet_clicks,
 SUM(TV_clicks) as TV_clicks,
 SUM(PC_clicks) as PC_clicks,
 SUM(other_clicks) as other_clicks,
 ((SUM(phone_clicks)+SUM(tablet_clicks)+SUM(TV_clicks)+SUM(PC_clicks)+SUM(other_clicks))) as exposure_group_clicks,
 SUM(phone_purch) as phone_purchases,
 SUM(tablet_purch) as tablet_purchases,
 SUM(TV_purch) as TV_purchases,
 SUM(PC_purch) as PC_purchases,
 SUM(other_purch) as other_purchases,
 (SUM(phone_purch)+SUM(tablet_purch)+SUM(TV_purch)+SUM(PC_purch)+SUM(other_purch)) as exposure_group_purchases,
 SUM(phone_rev)*1.0 as phone_revenue,
 CASE WHEN SUM(tablet_rev)>0 THEN SUM(tablet_rev) ELSE 0.0 END as tablet_revenue,
 CASE WHEN SUM(TV_rev)>0 THEN SUM(TV_rev) ELSE 0.0 END as TV_revenue,
 CASE WHEN SUM(PC_rev)>0 THEN SUM(PC_rev) ELSE 0.0 END as PC_revenue,
 CASE WHEN SUM(other_rev)>0 THEN SUM(other_rev) ELSE 0.0 END as other_revenue,
 (SUM(phone_rev)+SUM(tablet_rev)+SUM(TV_rev)+SUM(PC_rev)+SUM(other_rev))*1.0 as exposure_group_revenue,

 --Calculated ROAS values using Sales and Cost information. Query can be expanded by adding additional calculated fields
 CASE WHEN SUM(phone_cost)>0 THEN SUM(phone_rev)/(SUM(phone_cost)/100000) ELSE 0.0 END as phone_roas,
 CASE WHEN SUM(tablet_cost)>0 THEN SUM(tablet_rev)/(SUM(tablet_cost)/100000) ELSE 0.0 END as tablet_roas,
 CASE WHEN SUM(TV_cost)>0 THEN SUM(TV_rev)/(SUM(TV_cost)/100000) ELSE 0.0 END as TV_roas,
 CASE WHEN SUM(PC_cost)>0 THEN SUM(PC_rev)/(SUM(PC_cost)/100000) ELSE 0.0 END as PC_roas,
 CASE WHEN SUM(other_cost)>0 THEN SUM(other_rev)/(SUM(other_cost)/100000) ELSE 0.0 END as Other_roas,
 CASE WHEN ((SUM(phone_cost)+SUM(tablet_cost) +SUM(TV_cost)+SUM(PC_cost)+SUM(other_cost))/100000)>0
 THEN (SUM(phone_rev)+SUM(tablet_rev)+SUM(TV_rev)+SUM(PC_rev)+SUM(other_rev))/
 ((SUM(phone_cost)+SUM(tablet_cost) +SUM(TV_cost)+SUM(PC_cost)+SUM(other_cost))/100000)
 ELSE 0.0 END as exposure_group_roas
 FROM aggregated
GROUP BY ad_product_type, exposure_group, campaign, Advertiser
