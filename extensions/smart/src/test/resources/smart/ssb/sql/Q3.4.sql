select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
from p_lineorder
left join dates on lo_orderdate = d_datekey
left join customer on lo_custkey = c_custkey
left join supplier on lo_suppkey = s_suppkey
where (c_city='UNITED KI040' or c_city='UNITED KI010') and (s_city='UNITED KI040' or s_city='UNITED KI010') and d_yearmonth = 'Mar1997'
group by c_city, s_city, d_year
order by d_year asc, lo_revenue desc;