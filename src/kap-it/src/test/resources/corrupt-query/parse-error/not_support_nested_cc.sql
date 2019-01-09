-- refer to https://github.com/Kyligence/KAP/issues/8179

select
	seller_id,
	sum(case
		when seller_id > 10000000 and seller_id < 10000200 then pp
		else 0
	end) / sum(pp) as pp_ratio
from
	(
		select price * (1 - price) * (1-price) as pp from test_kylin_fact
		where price > 12 and cal_dt between '2012-01-01' and '2012-02-01'
	) as test_fact
group by
	seller_id
order by
	seller_id;