-- example: both table are full load, result have two context
--       join
--      /   \
--    join   A
--   /    \
--  A      B

SELECT buyer_account.account_country AS b_country
FROM test_account buyer_account
	JOIN test_country buyer_country ON buyer_account.account_country = buyer_country.country
	JOIN test_account seller_account ON buyer_account.account_country = seller_account.account_country
	ORDER BY b_country
LIMIT 100