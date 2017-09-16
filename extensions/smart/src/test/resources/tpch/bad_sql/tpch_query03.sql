--  Sum quantity/price/discount from lineitems, filter by date, group by returnflag and linestatus.

select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_saleprice) as sum_disc_price,
    sum(l_saleprice) + sum(l_taxprice) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
