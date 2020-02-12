select  count(distinct LO_CUSTKEY) as uv, s_name
from SSB.p_lineorder
left join SSB.supplier on lo_suppkey = s_suppkey
group by s_name