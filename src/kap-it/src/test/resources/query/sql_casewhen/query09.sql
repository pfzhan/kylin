select sum(case when price > 1 and item_count < 10 and seller_id > 20 then 1 else 0 end),
      sum(case when price > 1 or item_count < 5 or seller_id > 10 then price else 0 end)
from test_kylin_fact
where (case when price > 1 and item_count < 10 and seller_id > 20 then 1 else 0 end) = 1
