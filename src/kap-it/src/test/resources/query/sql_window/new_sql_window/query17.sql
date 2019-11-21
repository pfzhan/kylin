select * from (
          SELECT
          sum(price) over(partition by seller_id order by seller_id) sm,
          row_number() over(order by seller_id) AS rn,
          seller_id
          FROM
        (
            SELECT
              seller_id, price
            FROM
              TEST_KYLIN_FACT
            ORDER BY
              seller_id
        ) AS T
        )
        where sm > 100 AND rn BETWEEN 1 AND 100
