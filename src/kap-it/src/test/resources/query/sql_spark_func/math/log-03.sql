

SELECT log(10, 100),
       cast(log(10, abs(cast(price AS double) + 1)) AS varchar)
FROM test_kylin_fact
GROUP BY price,
         cast(log(10, abs(cast(price AS double) + 1)) AS varchar)
ORDER BY price LIMIT 2;