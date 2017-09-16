select  i_item_id, 
        avg(case when ss.ss_sales_price > ss1.ss_sales_price then 1 else 0 end) agg4 
 from TPCDS_BIN_PARTITIONED_ORC_2.store_sales as ss 
 join TPCDS_BIN_PARTITIONED_ORC_2.store_sales ss1 on ss.SS_SOLD_TIME_SK = ss1.SS_SOLD_TIME_SK
 join TPCDS_BIN_PARTITIONED_ORC_2.item on ss.ss_item_sk = item.i_item_sk
 group by i_item_id
 order by i_item_id
 limit 100;
