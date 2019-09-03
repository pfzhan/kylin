-- join with agg
SELECT TEST_ACCOUNT.ACCOUNT_SELLER_LEVEL, SUM(TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL)
FROM TEST_ACCOUNT
LEFT JOIN
(
  SELECT count(DISTINCT COUNTRY) as COUNTRY_DISTINCT_CNT
  FROM TEST_COUNTRY
) TEST_COUNTRY
ON
TEST_ACCOUNT.ACCOUNT_BUYER_LEVEL + TEST_COUNTRY.COUNTRY_DISTINCT_CNT > 10
GROUP BY TEST_ACCOUNT.ACCOUNT_SELLER_LEVEL
ORDER BY 1,2
LIMIT 10000