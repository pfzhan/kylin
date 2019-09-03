--https://github.com/kyligence/kap/issues/14155#

SELECT "LSTG_FORMAT_NAME" FROM "TEST_KYLIN_FACT"
WHERE rlike(substr("LSTG_FORMAT_NAME",1), 'ABIN')