

SELECT expm1(0),
       expm1(item_count)
FROM test_kylin_fact
WHERE item_count < 1000
ORDER BY item_count LIMIT 1;