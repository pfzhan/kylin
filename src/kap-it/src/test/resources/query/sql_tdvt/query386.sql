SELECT (CASE WHEN ("CALCS"."NUM0" >= "CALCS"."NUM1") THEN 1 WHEN NOT ("CALCS"."NUM0" >= "CALCS"."NUM1") THEN 0 ELSE NULL END) AS "TEMP_Test__1339453971__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY (CASE WHEN ("CALCS"."NUM0" >= "CALCS"."NUM1") THEN 1 WHEN NOT ("CALCS"."NUM0" >= "CALCS"."NUM1") THEN 0 ELSE NULL END)