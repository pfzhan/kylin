-- with subquery

with ca as(select time0 as t0, time1, datetime0 from tdvt.calcs),
     sta as( select sum(timestampdiff(minute, ca.time1, ca.t0)), ca.datetime0 from ca group by ca.datetime0 ),
     da as (select sum(timestampdiff(second, time0, time1)) as col1, datetime0, num1 from tdvt.calcs group by datetime0, num1)
select col1, datetime0 from (
    select da.col1, da.num1, sta.datetime0
    from da left join sta on da.datetime0 = sta.datetime0
    group by sta.datetime0, num1, col1
    order by sta.datetime0
)
group by col1, datetime0