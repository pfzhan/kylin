SELECT "t0"."TEMP_Test__2731970693__0_" AS "TEMP_Test__2731970693__0_"
FROM (
  SELECT "CALCS"."STR1" AS "STR1",
    SUM("CALCS"."NUM1") AS "TEMP_Test__2731970693__0_",
    SUM("CALCS"."NUM1") AS "X_measure__0"
  FROM "TDVT"."CALCS" "CALCS"
  GROUP BY "CALCS"."STR1"
) "t0"
GROUP BY "t0"."TEMP_Test__2731970693__0_"