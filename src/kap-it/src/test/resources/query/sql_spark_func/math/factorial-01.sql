


SELECT factorial(5),
       CASE
           WHEN item_count <= 20 THEN factorial(item_count)
           ELSE NULL
       END
FROM test_kylin_fact
ORDER BY item_count LIMIT 3;