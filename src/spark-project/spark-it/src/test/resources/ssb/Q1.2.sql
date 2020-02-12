select sum(v_revenue) as revenue
from SSB.p_lineorder
left join SSB.dates on lo_orderdate = d_datekey
where d_yearmonthnum = 199401
and lo_discount between 4 and 6
and lo_quantity between 26 and 35;