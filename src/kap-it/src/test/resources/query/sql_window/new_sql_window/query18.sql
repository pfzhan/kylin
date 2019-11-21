select * from (
          SELECT
          price,
          count(price) over(order by price) as cnt,
          sum(price) over(order by price) as sm,
          row_number() over(order by price) AS rn,
           seller_id
          FROM
        (
            SELECT
              seller_id, sum(price + seller_id) over(order by seller_id) as price
            FROM
              TEST_KYLIN_FACT
            group by price, seller_id
            ORDER BY seller_id
        ) AS T
        )
        where cnt > 100 AND sm > 100 AND rn BETWEEN 1 AND 100