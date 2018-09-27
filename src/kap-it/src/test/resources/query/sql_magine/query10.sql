-- https://github.com/Kyligence/KAP/issues/6483

select c.LEAF_CATEG_ID, c.SITE_ID, b.SELLER_ID, b.LSTG_FORMAT_NAME from
test_category_groupings c join (

select SELLER_ID, LSTG_FORMAT_NAME,LSTG_SITE_ID, test_kylin_fact.leaf_categ_id  from test_kylin_fact
inner JOIN edw.test_cal_dt as test_cal_dt
 ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt
 inner JOIN test_category_groupings
 ON test_kylin_fact.leaf_categ_id = test_category_groupings.leaf_categ_id AND test_kylin_fact.lstg_site_id = test_category_groupings.site_id
 where test_kylin_fact.TRANS_ID >= 0

 ) b on b.leaf_categ_id = c.leaf_categ_id AND b.LSTG_SITE_ID=c.SITE_ID