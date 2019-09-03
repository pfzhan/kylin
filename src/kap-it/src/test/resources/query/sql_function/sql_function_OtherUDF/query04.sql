--https://github.com/kyligence/kap/issues/14155#


SELECT "LSTG_FORMAT_NAME" FROM "TEST_KYLIN_FACT"
WHERE rlike("LSTG_FORMAT_NAME", '^FP |GTC$')
and rlike("LSTG_FORMAT_NAME",'FP-GT+C*')
and rlike("LSTG_FORMAT_NAME",'FP-(non )?GTC')
and rlike("LSTG_FORMAT_NAME",'FP_GTC')
and rlike("LSTG_FORMAT_NAME",'FP-[A-Z][^a-z]C')