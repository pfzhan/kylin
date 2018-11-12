SELECT "Calcs"."KEY" AS "KEY",
  SUM("Calcs"."NUM2") AS "sum_num2_ok"
FROM "TDVT"."CALCS" "Calcs"
GROUP BY "Calcs"."KEY"
ORDER BY SUM("Calcs"."NUM2") DESC
LIMIT 10