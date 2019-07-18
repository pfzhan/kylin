select TEST_KYLIN_FACT.LEAF_CATEG_ID, count(TEST_KYLIN_FACT.LEAF_CATEG_ID) as cnt
 from TEST_ACCOUNT
 RIGHT JOIN test_kylin_fact
 ON TEST_KYLIN_FACT.SELLER_ID = TEST_ACCOUNT.ACCOUNT_ID
 group by TEST_KYLIN_FACT.LEAF_CATEG_ID
 order by TEST_KYLIN_FACT.LEAF_CATEG_ID asc
LIMIT 20
