--https://github.com/Kyligence/KAP/issues/13616

SELECT "LSTG_FORMAT_NAME" FROM "TEST_KYLIN_FACT"
WHERE REGEXP_LIKE("LSTG_FORMAT_NAME", '^FP |GTC$')
and REGEXP_LIKE("LSTG_FORMAT_NAME",'FP-GT+C*')
and REGEXP_LIKE("LSTG_FORMAT_NAME",'FP-(non )?GTC')
and REGEXP_LIKE("LSTG_FORMAT_NAME",'FP.GTC')
and REGEXP_LIKE("LSTG_FORMAT_NAME",'FP-[A-Z][^a-z]C')