select sum(v_revenue) as revenue
from  SSB.p_lineorder
left join SSB.dates on lo_orderdate = d_datekey
where d_year = 1993
and lo_discount between 1 and 3
and lo_quantity < 25