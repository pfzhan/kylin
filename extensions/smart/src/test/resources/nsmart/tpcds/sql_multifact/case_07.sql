select  i_item_id, 
        avg(ss.ss_quantity) agg1,
        avg(ss.ss_list_price) agg2,
        avg(ss.ss_coupon_amt) agg3,
        avg(ss.ss_sales_price) agg4 
 from TPCDS_BIN_PARTITIONED_ORC_2.store_sales as ss 
 join TPCDS_BIN_PARTITIONED_ORC_2.customer_demographics on ss.ss_cdemo_sk = customer_demographics.cd_demo_sk
 join TPCDS_BIN_PARTITIONED_ORC_2.date_dim on ss.ss_sold_date_sk = date_dim.d_date_sk
 join TPCDS_BIN_PARTITIONED_ORC_2.item on ss.ss_item_sk = item.i_item_sk
 join TPCDS_BIN_PARTITIONED_ORC_2.promotion on ss.ss_promo_sk = promotion.p_promo_sk
 where cd_gender = 'F' and 
       cd_marital_status = 'W' and
       cd_education_status = 'Primary' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 1998 
 group by i_item_id
 order by i_item_id
 limit 100;
