--#6715

select TRANS_ID,sum(PRICE)
from KYLIN_SALES
where PART_DT='2012-01-01'
group by TRANS_ID