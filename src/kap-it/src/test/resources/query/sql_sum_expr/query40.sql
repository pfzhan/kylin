-- KE-14446 sum case when with percentile
SELECT
FLOOR(PERCENTILE(PRICE, 0.5)),
SUM(CASE WHEN TRANS_ID > 100 THEN ITEM_COUNT ELSE 0 END)
FROM TEST_KYLIN_FACT

