 -- https://github.com/Kyligence/KAP/issues/14852
-- function nesting test

--something wrong with: sum(cbrt(price + cast(concat('123.456', '78') AS double)))

SELECT cbrt(price + cast(concat('123.456', '78') AS double))
FROM test_kylin_fact
ORDER BY price LIMIT 10;