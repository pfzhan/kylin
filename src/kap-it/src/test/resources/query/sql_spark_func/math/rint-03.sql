

SELECT cast(rint(price) * cast('234.23' AS double) AS varchar)
FROM test_kylin_fact
GROUP BY cast(rint(price) * cast('234.23' AS double) AS varchar),
         price
ORDER BY price LIMIT 10;