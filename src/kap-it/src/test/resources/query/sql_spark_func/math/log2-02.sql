

SELECT log2(2),
       log2(abs(price))
FROM test_kylin_fact
GROUP BY log2(2),
         log2(abs(price)),
         price
ORDER BY price LIMIT 1;