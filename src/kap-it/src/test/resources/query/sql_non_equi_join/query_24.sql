-- agg push down count(1) with inner col (should not do agg push down here)
SELECT COUNT(1)
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
    LEFT JOIN
        (
            SELECT TEST_KYLIN_FACT.SELLER_ID + 11 AS S, CAL_DT
            FROM TEST_KYLIN_FACT
            ) B
          ON A.SELLER_ID = B.S AND A.CAL_DT > B.CAL_DT