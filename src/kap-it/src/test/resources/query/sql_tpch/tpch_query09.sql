select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			year(o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			lineitem
			inner join part on l_partkey = p_partkey
			inner join supplier on l_suppkey = s_suppkey
			inner join partsupp on l_suppkey = ps_suppkey and l_partkey = ps_partkey
			inner join orders on l_orderkey = o_orderkey
			inner join nation on s_nationkey = n_nationkey
		where
			p_name like '%plum%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
