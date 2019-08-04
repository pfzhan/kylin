-- https://github.com/Kyligence/KAP/issues/13615

select *
from TEST_MEASURE
where ifnull(ID2,132322342)  =  132322342  --test bigint null value
and ifnull(ID3,14123)  =  14123  --test long null value
and ifnull(ID4,313)  =  313  --test int null value
and ifnull(price1,12.34)  =  12.34  --test float null value
and ifnull(price2,124.44)  =  124.44  --test double null value
and ifnull(price3,14.242343)  =  14.242343  --test decimal(19,6) null value
and ifnull(price5,2)  =  2  --test short null value
and ifnull(price6,7)  =  7  --test tinyint null value
and ifnull(price7,1)  =  1  --test smallint null value
and ifnull(name1,'FT')  =  'FT'  --test string null value
and ifnull(name2,'FT')  =  'FT'  --test varchar(254) null value
and ifnull(name3,'FT')  =  'FT'  --test char null value
and ifnull(name4,2)  =  2  --test byte null value
and ifnull(time1,date'2014-3-31')  =  date'2014-3-31'  --test date null value
--and ifnull(time2,current_timestamp)  =  current_timestamp  --test timestamp null value
and ifnull(flag,true)  =  true  --test boolean null value