
select count(L_SHIPDATE)
from TPCH.LINEITEM
group by case when L_ORDERKEY in (1, 2) then 2 end
