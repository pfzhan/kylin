-- KE-12838
select sum(ITEM_COUNT) as ITEM_CNT
FROM TEST_KYLIN_FACT as TEST_KYLIN_FACT
INNER JOIN TEST_ACCOUNT as SELLER_ACCOUNT
ON TEST_KYLIN_FACT.SELLER_ID = SELLER_ACCOUNT.ACCOUNT_ID AND TEST_KYLIN_FACT.SELLER_ID IS NOT NULL
INNER JOIN TEST_ORDER as TEST_ORDER
ON TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID
INNER JOIN TEST_ACCOUNT as BUYER_ACCOUNT
ON TEST_ORDER.BUYER_ID = BUYER_ACCOUNT.ACCOUNT_ID
INNER JOIN EDW.TEST_CAL_DT as TEST_CAL_DT
ON TEST_KYLIN_FACT.CAL_DT = TEST_CAL_DT.CAL_DT
INNER JOIN TEST_CATEGORY_GROUPINGS as TEST_CATEGORY_GROUPINGS
ON TEST_KYLIN_FACT.LEAF_CATEG_ID = TEST_CATEGORY_GROUPINGS.LEAF_CATEG_ID AND TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_CATEGORY_GROUPINGS.SITE_ID
INNER JOIN EDW.TEST_SITES as TEST_SITES
ON TEST_KYLIN_FACT.LSTG_SITE_ID = TEST_SITES.SITE_ID
INNER JOIN EDW.TEST_SELLER_TYPE_DIM as TEST_SELLER_TYPE_DIM
ON TEST_KYLIN_FACT.SLR_SEGMENT_CD = TEST_SELLER_TYPE_DIM.SELLER_TYPE_CD
INNER JOIN TEST_COUNTRY as BUYER_COUNTRY
ON BUYER_ACCOUNT.ACCOUNT_COUNTRY = BUYER_COUNTRY.COUNTRY
INNER JOIN TEST_COUNTRY as SELLER_COUNTRY
ON SELLER_ACCOUNT.ACCOUNT_COUNTRY = SELLER_COUNTRY.COUNTRY