-- test is false, is true, is not false, is not true

SELECT trans_id,
CASE WHEN IS_EFFECTUAL IS FALSE THEN 0 ELSE 1 END,
CASE WHEN IS_EFFECTUAL IS NOT TRUE THEN 0 ELSE 1 END
FROM TEST_KYLIN_FACT
WHERE (PRICE > 0) IS TRUE AND (ITEM_COUNT > 0) IS NOT FALSE

