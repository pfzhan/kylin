SELECT "t1"."TEMP_Test__2210987900__0_" AS "TEMP_Test__2210987900__0_"
FROM (
  SELECT "t0"."STR1" AS "STR1",
    AVG("t0"."X_measure__0") AS "TEMP_Test__2210987900__0_",
    AVG("t0"."X_measure__0") AS "X_measure__1"
  FROM (
    SELECT "CALCS"."STR1" AS "STR1",
      "CALCS"."STR2" AS "STR2",
      SUM("CALCS"."NUM1") AS "X_measure__0"
    FROM "TDVT"."CALCS" "CALCS"
    GROUP BY "CALCS"."STR1",
      "CALCS"."STR2"
  ) "t0"
  GROUP BY "t0"."STR1"
) "t1"
GROUP BY "t1"."TEMP_Test__2210987900__0_"