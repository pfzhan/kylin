-- agg push down count with inner col (left join)
SELECT
        COUNT(*)
    FROM
    (
            SELECT TEST_KYLIN_FACT.SELLER_ID + TEST_KYLIN_FACT.LSTG_SITE_ID AS S, CAL_DT
            FROM TEST_KYLIN_FACT
            ) B
    LEFT JOIN
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
          ON A.SELLER_ID = B.S AND A.CAL_DT = B.CAL_DT
