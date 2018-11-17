select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
    lineitem
    inner join orders on l_orderkey = o_orderkey
    inner join customer on c_custkey = o_custkey
where
	c_mktsegment = 'BUILDING'
	and o_orderdate < '1995-03-22'
	and l_shipdate > '1995-03-22'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10;
