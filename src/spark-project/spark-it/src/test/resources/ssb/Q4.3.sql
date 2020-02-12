select d_year, s_city, p_brand, sum(lo_revenue) - sum(lo_supplycost) as profit
from SSB.p_lineorder
left join SSB.dates on lo_orderdate = d_datekey
left join SSB.customer on lo_custkey = c_custkey
left join SSB.supplier on lo_suppkey = s_suppkey
left join SSB.part on lo_partkey = p_partkey
where c_region = 'AMERICA' and s_nation = 'PERU'
and (d_year = 1997 or d_year = 1998)
and p_category = 'MFGR#12'
group by d_year, s_city, p_brand
order by d_year, s_city, p_brand;