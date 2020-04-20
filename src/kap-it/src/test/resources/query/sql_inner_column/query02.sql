select count(1) as num from
(
select trans_id, TEST_KYLIN_FACT.cal_dt
from TEST_KYLIN_FACT
inner JOIN TEST_ACCOUNT ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID
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
 inner JOIN test_country
ON test_account.account_country = test_country.country
where
  TEST_ACCOUNT.ACCOUNT_ID + TEST_KYLIN_FACT.ITEM_COUNT >= 10000336
  and TEST_KYLIN_FACT.ITEM_COUNT > 100
  or (
    TEST_KYLIN_FACT.ITEM_COUNT * 100 <> 100000
    and LSTG_FORMAT_NAME in ('FP-GTC', 'ABIN')
  )
)
group by cal_dt