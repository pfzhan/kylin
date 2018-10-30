-- #5169
select t1.seller_id, t1.sum_price, t2.CAL_DT from (
select seller_id,CAL_DT,sum(price) as sum_price from test_kylin_fact
  group by seller_id,CAL_DT limit 200
) t1
INNER JOIN
(
  select seller_id,CAL_DT,sum(price) as sum_price from test_kylin_fact
  group by seller_id,CAL_DT
) t2
on t1.seller_id = t2.seller_id
limit 20