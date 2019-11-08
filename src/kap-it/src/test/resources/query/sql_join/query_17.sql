SELECT SUM(PRICE * ITEM_COUNT), sum(item_count), TEST_KYLIN_FACT.SELLER_ID
FROM
TEST_KYLIN_FACT INNER JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID
INNER JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT=TEST_CAL_DT.CAL_DT
INNER JOIN TEST_ORDER ON TEST_ACCOUNT.ACCOUNT_ID = TEST_ORDER.BUYER_ID AND TEST_CAL_DT.CAL_DT = TEST_ORDER.TEST_DATE_ENC
GROUP BY TEST_KYLIN_FACT.SELLER_ID


