-- SQL q65.sql
select 
    s_store_name,
    i_item_desc,
    sc.revenue,
    i_current_price,
    i_wholesale_cost,
    i_brand
from
    (select 
        ss_store_sk, avg(revenue) as ave
    from
        (select 
        ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
    from
        store_sales 
    join date_dim on ss_sold_date_sk = d_date_sk
    where d_month_seq between 1212 and 1212 + 11
    group by ss_store_sk , ss_item_sk) sa
    group by ss_store_sk) sb 
    join 
    (select 
        ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
    from
        store_sales 
    join date_dim on ss_sold_date_sk = d_date_sk
    where d_month_seq between 1212 and 1212 + 11
    group by ss_store_sk , ss_item_sk) sc
    on sb.ss_store_sk = sc.ss_store_sk
    join store on s_store_sk = sc.ss_store_sk
    join item on i_item_sk = sc.ss_item_sk
where sc.revenue <= 0.1 * sb.ave
order by s_store_name , i_item_desc
limit 100
