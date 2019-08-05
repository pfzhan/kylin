select TEST_KYLIN_FACT.LEAF_CATEG_ID, count(TEST_KYLIN_FACT.LEAF_CATEG_ID) as cnt
 from TEST_COUNTRY
 RIGHT JOIN (
   select TEST_ACCOUNT.ACCOUNT_COUNTRY, TEST_KYLIN_FACT.SELLER_ID, TEST_KYLIN_FACT.LEAF_CATEG_ID
   from TEST_ACCOUNT
   RIGHT JOIN TEST_KYLIN_FACT ON TEST_KYLIN_FACT.SELLER_ID < TEST_ACCOUNT.ACCOUNT_ID
 ) TEST_KYLIN_FACT ON TEST_KYLIN_FACT.ACCOUNT_COUNTRY = TEST_COUNTRY.COUNTRY
 group by TEST_KYLIN_FACT.LEAF_CATEG_ID
 order by TEST_KYLIN_FACT.LEAF_CATEG_ID asc
LIMIT 20
