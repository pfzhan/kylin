SELECT "t2"."X_measure__1" AS "avg_LOD_Network_Recapture__copy__ok",
"t1"."X_measure__3" AS "avg_LOD_Practice_HCC_RAF_Score__copy_2__ok",
"t4"."X_measure__5" AS "avg_LOD_Provider_RAF_Score__copy_2__ok"
FROM (   SELECT AVG("t0"."X_measure__2") AS "X_measure__3"
FROM (     SELECT "Z_360_HEAT"."PCP_PRACTICE" AS "PCP_PRACTICE",
AVG("Z_360_HEAT"."TOTAL_RAF_SCORE") AS "X_measure__2"
FROM "POPHEALTH_ANALYTICS"."Z_360_HEAT" "Z_360_HEAT"
GROUP BY "Z_360_HEAT"."PCP_PRACTICE") "t0"
HAVING (COUNT(1) > 0) ) "t1"
CROSS JOIN (   SELECT {fn CONVERT(AVG("Z_360_HEAT"."TOTAL_RAF_SCORE"), SQL_DOUBLE)} AS "X_measure__1"
FROM "POPHEALTH_ANALYTICS"."Z_360_HEAT" "Z_360_HEAT"
HAVING (COUNT(1) > 0) ) "t2"
CROSS JOIN (   SELECT AVG("t3"."X_measure__4") AS "X_measure__5"
FROM (     SELECT "Z_360_HEAT"."PCP_NAME" AS "PCP_NAME",
AVG("Z_360_HEAT"."TOTAL_RAF_SCORE") AS "X_measure__4"
FROM "POPHEALTH_ANALYTICS"."Z_360_HEAT" "Z_360_HEAT"
GROUP BY "Z_360_HEAT"."PCP_NAME") "t3"
HAVING (COUNT(1) > 0) ) "t4";