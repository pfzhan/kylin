SELECT (CASE WHEN ("CALCS"."DATE0" >= {d '1975-11-12'}) THEN 1 WHEN NOT ("CALCS"."DATE0" >= {d '1975-11-12'}) THEN 0 ELSE NULL END) AS "TEMP_Test__4028845222__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY (CASE WHEN ("CALCS"."DATE0" >= {d '1975-11-12'}) THEN 1 WHEN NOT ("CALCS"."DATE0" >= {d '1975-11-12'}) THEN 0 ELSE NULL END)