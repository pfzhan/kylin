-- SQL q46.sql
select  c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,amt,profit 
 from
   (select ss_ticket_number
          ,ss_customer_sk
          ,ca_city bought_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales
    join date_dim on store_sales.ss_sold_date_sk = date_dim.d_date_sk
    join store on store_sales.ss_store_sk = store.s_store_sk  
    join household_demographics on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    join customer_address on store_sales.ss_addr_sk = customer_address.ca_address_sk
    where (household_demographics.hd_dep_count = 4 or
         household_demographics.hd_vehicle_count= 2)
    and date_dim.d_dow in (6,0)
    and date_dim.d_year in (1998,1998+1,1998+2) 
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn
    join customer on dn.ss_customer_sk = customer.c_customer_sk
    join customer_address current_addr on customer.c_current_addr_sk = current_addr.ca_address_sk
    where current_addr.ca_city <> bought_city
  order by c_last_name
          ,c_first_name
          ,ca_city
          ,bought_city
          ,ss_ticket_number
  limit 100