-- SQL q95.sql
with ws_wh as
(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from web_sales ws1
 join web_sales ws2 on ws1.ws_order_number = ws2.ws_order_number
 where ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
select 
   count(distinct ws_order_number) as order_count
  ,sum(ws_ext_ship_cost) as total_shipping_cost
  ,sum(ws_net_profit) as total_net_profit
from
   web_sales ws1
join date_dim on ws1.ws_ship_date_sk = d_date_sk
join customer_address on ws1.ws_ship_addr_sk = ca_address_sk
join web_site on ws1.ws_web_site_sk = web_site_sk
where
    d_date between '2002-5-01' and '2002-6-30'
and ca_state = 'GA'
and web_company_name = 'pri'
and ws1.ws_order_number in (select ws_order_number
                            from ws_wh)
and ws1.ws_order_number in (select wr_order_number
                            from web_returns join ws_wh
                            on wr_order_number = ws_wh.ws_order_number)
order by count(distinct ws_order_number)
limit 100