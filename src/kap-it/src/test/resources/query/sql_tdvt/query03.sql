SELECT "Calcs"."STR2" AS "STR2"
FROM "TDVT"."CALCS" "Calcs"
WHERE ((NOT (("Calcs"."STR2" >= 'eight') AND ("Calcs"."STR2" <= 'six'))) OR ("Calcs"."STR2" IS NULL))
GROUP BY "Calcs"."STR2"