-- example: both table are full load, result have one context
--       join
--      /   \
--    join   A
--   /    \
--  B      A

SELECT buyer_account.account_country AS b_country
FROM test_account buyer_account
	JOIN test_country buyer_country ON buyer_account.account_country = buyer_country.country
	JOIN test_country seller_country ON buyer_country.country = seller_country.country
	ORDER BY b_country
LIMIT 100