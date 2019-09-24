-- agg push down with inner col
SELECT
        COUNT(*)
    FROM
        (  SELECT
            TEST_KYLIN_FACT.SELLER_ID,
            SUM(TEST_KYLIN_FACT.ITEM_COUNT),
            CAL_DT
        FROM
            TEST_KYLIN_FACT
        GROUP BY
            TEST_KYLIN_FACT.SELLER_ID,
            CAL_DT
            ) A
    INNER JOIN
        (
            SELECT TEST_KYLIN_FACT.SELLER_ID + 11 AS S, CAL_DT
            FROM TEST_KYLIN_FACT
            ) B  ON A.SELLER_ID = B.S AND A.CAL_DT = B.CAL_DT
