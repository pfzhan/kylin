--https://github.com/Kyligence/KAP/issues/10096

SELECT
 test_cal_dt.week_beg_dt
 ,test_category_groupings.meta_categ_name
 ,test_category_groupings.categ_lvl2_name
 ,test_category_groupings.categ_lvl3_name
 ,sum(test_kylin_fact.price) as GMV
 , count(*) as TRANS_CNT
 FROM test_kylin_fact
  inner JOIN edw.test_cal_dt as test_cal_dt
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt
 inner JOIN test_category_groupings
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
 inner JOIN edw.test_sites as test_sites
 ON test_kylin_fact.lstg_site_id = test_sites.site_id
 inner JOIN edw.test_seller_type_dim as test_seller_type_dim
 ON test_kylin_fact.slr_segment_cd = test_seller_type_dim.seller_type_cd
 INNER JOIN TEST_ORDER as TEST_ORDER
 ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID
inner join test_account
on TEST_KYLIN_FACT.seller_id = test_account.account_id
inner join test_country
on test_account.account_country = test_country.country
 where (test_kylin_fact.leaf_categ_id = 100 or test_kylin_fact.leaf_categ_id > 200)
    and test_kylin_fact.price > 10
    and test_kylin_fact.lstg_format_name like '%BIN%'
    and test_cal_dt.week_beg_dt between DATE '2013-05-01' and DATE '2013-08-01'
 group by test_cal_dt.week_beg_dt
 ,test_category_groupings.meta_categ_name
 ,test_category_groupings.categ_lvl2_name
 ,test_category_groupings.categ_lvl3_name