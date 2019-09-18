 -- https://github.com/Kyligence/KAP/issues/14852
-- complex function nesting test


SELECT count(cast(bround(bround(price, cast(substring('12323', 0, 1) AS integer)) + 2.71828, 1+2) AS varchar)),
       bround(cast('123.234356' AS double), 4)
FROM test_kylin_fact
GROUP BY bround(cast('123.234356' AS double), 4);