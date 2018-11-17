select
	sn.n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
    lineitem
    inner join orders on l_orderkey = o_orderkey
    inner join customer on o_custkey = c_custkey
    inner join nation cn on c_nationkey = cn.n_nationkey
    inner join supplier on l_suppkey = s_suppkey
    inner join nation sn on s_nationkey = sn.n_nationkey
    inner join region on sn.n_regionkey = r_regionkey
where
	r_name = 'AFRICA'
	and o_orderdate >= '1993-01-01'
	and o_orderdate < '1994-01-01'
group by
	sn.n_name
order by
	revenue desc;
