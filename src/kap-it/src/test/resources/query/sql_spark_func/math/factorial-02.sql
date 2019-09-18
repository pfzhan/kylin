

SELECT item_count,
       CASE
           WHEN item_count < 1 THEN 0
           WHEN item_count<=20 THEN factorial(item_count)
           ELSE NULL
       END
FROM test_kylin_fact
GROUP BY item_count,
         CASE
             WHEN item_count < 1 THEN 0
             WHEN item_count<=20 THEN factorial(item_count)
             ELSE NULL
         END
ORDER BY item_count LIMIT 10;