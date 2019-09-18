


SELECT log2(2),
       log2(abs(price))
FROM test_kylin_fact
ORDER BY price LIMIT 1;