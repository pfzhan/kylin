-- this sql is same with sql_tpch/tpch_query07.sql
-- and its join info is partly equals with sql_tpch/sql_tpch_reprosal/query04.sql

select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			year(l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			tpch.lineitem
			inner join tpch.supplier on s_suppkey = l_suppkey
			inner join tpch.orders on l_orderkey = o_orderkey
			inner join tpch.customer on o_custkey = c_custkey
			inner join tpch.nation n1 on s_nationkey = n1.n_nationkey
			inner join tpch.nation n2 on c_nationkey = n2.n_nationkey
		where
			(
				(n1.n_name = 'KENYA' and n2.n_name = 'PERU')
				or (n1.n_name = 'PERU' and n2.n_name = 'KENYA')
			)
			and l_shipdate between '1995-01-01' and '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
