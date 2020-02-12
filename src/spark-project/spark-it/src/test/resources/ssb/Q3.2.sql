select sum(lo_revenue) as lo_revenue, d_year, p_brand
from SSB.p_lineorder
left join SSB.dates on lo_orderdate = d_datekey
left join SSB.part on lo_partkey = p_partkey
left join SSB.supplier on lo_suppkey = s_suppkey
where p_brand = 'MFGR#1414' and s_region = 'AMERICA'
group by d_year, p_brand
order by d_year, p_brand