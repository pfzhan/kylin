SELECT "Calcs"."STR2" AS "STR2"
FROM "TDVT"."CALCS" "Calcs"
WHERE ((NOT ("Calcs"."STR2" IN ('eight', 'eleven', 'fifteen', 'five'))) OR ("Calcs"."STR2" IS NULL))
GROUP BY "Calcs"."STR2"