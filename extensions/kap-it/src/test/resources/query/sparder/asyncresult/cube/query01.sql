SELECT "TEST_KYLIN_FACT"."CAL_DT", SUM("TEST_KYLIN_FACT"."PRICE") AS "sum_PRICE_ok" FROM "TEST_KYLIN_FACT" "TEST_KYLIN_FACT"
  INNER JOIN (
             SELECT COUNT(1) AS "XTableau_join_flag",     SUM("TEST_KYLIN_FACT"."PRICE") AS "X__alias__A",     "TEST_KYLIN_FACT"."CAL_DT" AS "none_CAL_DT_ok"   FROM "TEST_KYLIN_FACT" "TEST_KYLIN_FACT"
             GROUP BY "TEST_KYLIN_FACT"."CAL_DT"    )

    "t0" ON ("TEST_KYLIN_FACT"."CAL_DT" = "t0"."none_CAL_DT_ok") GROUP BY "TEST_KYLIN_FACT"."CAL_DT"