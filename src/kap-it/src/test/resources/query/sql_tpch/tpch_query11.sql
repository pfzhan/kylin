with q11_part_tmp_cached as (
	select
		ps_partkey,
		sum(ps_supplycost * ps_availqty) as part_value
	from
		tpch.partsupp
		inner join tpch.supplier on ps_suppkey = s_suppkey
		inner join tpch.nation on s_nationkey = n_nationkey
	where
		n_name = 'GERMANY'
	group by ps_partkey
),
q11_sum_tmp_cached as (
	select
		sum(ps_supplycost * ps_availqty) as total_value
	from
		tpch.partsupp
		inner join tpch.supplier on ps_suppkey = s_suppkey
		inner join tpch.nation on s_nationkey = n_nationkey
	where
		n_name = 'GERMANY'
)

select
	ps_partkey, 
	part_value
from (
	select
		ps_partkey,
		part_value,
		total_value
	from
		q11_part_tmp_cached, q11_sum_tmp_cached
)
where
	part_value > total_value * 0.0001
order by
	part_value desc