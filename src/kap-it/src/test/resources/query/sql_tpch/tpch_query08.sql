select
	o_year,
	sum(peru_volumn) / sum(volume) as mkt_share
from
	(
		select
			year(o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			case
				when n2.n_name = 'PERU' then l_extendedprice * (1 - l_discount)
				else 0
			end as peru_volumn
		from
	    	lineitem
		    inner join part on l_partkey = p_partkey
		    inner join supplier on l_suppkey = s_suppkey
			inner join orders on l_orderkey = o_orderkey
			inner join customer on o_custkey = c_custkey
		    inner join nation n1 on c_nationkey = n1.n_nationkey
		    inner join nation n2 on s_nationkey = n2.n_nationkey
		    inner join region on n1.n_regionkey = r_regionkey
		where
			r_name = 'AMERICA'
			and o_orderdate between '1995-01-01' and '1996-12-31'
			and p_type = 'ECONOMY BURNISHED NICKEL'
	) as all_nations
group by
	o_year
order by
	o_year;
