-- SQL q24.sql
with ssales as
(select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_sales_price) netpaid
from store_sales
join store_returns on ss_ticket_number = sr_ticket_number
  and ss_item_sk = sr_item_sk
join store on ss_store_sk = s_store_sk
join item on ss_item_sk = i_item_sk
join customer on ss_customer_sk = c_customer_sk
join customer_address on s_zip = ca_zip
where c_birth_country = upper(ca_country)
and s_market_id=8
group by c_last_name
        ,c_first_name
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size)
, ssnetpaid as 
(select 0.05*avg(netpaid) as baseline
from ssales)
select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
from ssales, ssnetpaid
where i_color = 'orchid'
group by c_last_name
        ,c_first_name
        ,s_store_name
        ,baseline
having sum(netpaid) > baseline