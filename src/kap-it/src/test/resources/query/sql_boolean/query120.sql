-- verify the SQL query that contain the boolean datatype

SELECT IS_EFFECTUAL,COUNT(*) coun
FROM TEST_KYLIN_FACT
GROUP BY IS_EFFECTUAL