select 100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	tpch.lineitem inner join tpch.part on l_partkey = p_partkey
where
	l_shipdate >= '1995-08-01'
	and l_shipdate < '1995-09-01';
