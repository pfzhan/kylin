SELECT COUNT("T"."ACCOUNT_COUNTRY"), "ACCOUNT_SELLER_LEVEL"
FROM (
	SELECT "PRICE", "TRANS_ID", "SELLER_ID" FROM "TEST_KYLIN_FACT" ORDER BY "TRANS_ID" DESC
	)
"S" INNER JOIN (
	SELECT "ACCOUNT_COUNTRY", "ACCOUNT_ID", "ACCOUNT_SELLER_LEVEL" FROM "TEST_ACCOUNT"
	)
"T"
ON "T"."ACCOUNT_ID" = "S"."SELLER_ID"
INNER JOIN "TEST_COUNTRY" "E"
ON "T"."ACCOUNT_COUNTRY" = "E"."COUNTRY"
GROUP BY "ACCOUNT_SELLER_LEVEL"
ORDER BY ACCOUNT_SELLER_LEVEL