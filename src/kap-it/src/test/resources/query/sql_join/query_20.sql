SELECT sum(sum_item_count), TEST_KYLIN_FACT.SELLER_ID
FROM
(
select sum(item_count) as sum_item_count, CAL_DT, ORDER_ID, SELLER_ID
from TEST_KYLIN_FACT
group by CAL_DT, ORDER_ID, SELLER_ID
)
TEST_KYLIN_FACT LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID
INNER JOIN EDW.TEST_CAL_DT ON TEST_KYLIN_FACT.CAL_DT=TEST_CAL_DT.CAL_DT
LEFT JOIN TEST_ORDER ON TEST_ACCOUNT.ACCOUNT_ID = TEST_ORDER.BUYER_ID
AND TEST_CAL_DT.CAL_DT = TEST_ORDER.TEST_DATE_ENC
AND TEST_KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID
GROUP BY TEST_KYLIN_FACT.SELLER_ID, TEST_ORDER.TEST_DATE_ENC


