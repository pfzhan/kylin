SELECT CAL_DT,
             CASE WHEN SELLER_ID = '057|001|02|021' THEN '编辑' WHEN SELLER_ID = '058|001|02|021' THEN '趣美颜' ELSE 'AI抠图' END AS SELLER_ID,
                                                                                                                            sum(PRICE) AS p
      FROM TEST_KYLIN_FACT
      WHERE SELLER_ID IN (1,2,3)
      GROUP BY CAL_DT,
               CASE WHEN SELLER_ID = '057|001|02|021' THEN '编辑' WHEN SELLER_ID = '058|001|02|021' THEN '趣美颜' ELSE 'AI抠图' END