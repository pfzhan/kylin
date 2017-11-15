select t1.leaf_categ_id, max_price, min_price, sum_price
from
(select leaf_categ_id, sum(price) as sum_price from test_kylin_fact group by leaf_categ_id) t1
join
(select leaf_categ_id, max(price) as max_price from test_kylin_fact group by leaf_categ_id) t2
on t1.leaf_categ_id = t2.leaf_categ_id
join
(select leaf_categ_id, min(price) as min_price from test_kylin_fact group by leaf_categ_id) t3
on t1.leaf_categ_id = t3.leaf_categ_id
order by t1.leaf_categ_id