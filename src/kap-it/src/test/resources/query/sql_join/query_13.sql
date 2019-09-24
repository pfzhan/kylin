-- agg push down with inner col (lef join)
SELECT
        SUM(KYLIN_ITEM_COUNT)
    FROM
    (
            SELECT TEST_KYLIN_FACT.SELLER_ID + TEST_KYLIN_FACT.LSTG_SITE_ID AS S, CAL_DT, ITEM_COUNT as KYLIN_ITEM_COUNT
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
