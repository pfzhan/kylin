-- https://github.com/Kyligence/KAP/issues/14852
-- aggregate index test

SELECT cbrt(price),
       count(cbrt(price * item_count - 1))
FROM test_kylin_fact
GROUP BY cbrt(price)
ORDER BY 1 LIMIT 2;