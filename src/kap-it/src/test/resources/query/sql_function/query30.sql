select rank() over (order by avg_price asc) rnk, avg_price, time1, avg_id4
from
(
select avg(ID4) avg_id4, avg(price8) avg_price, time1
from test_measure
group by time1
)