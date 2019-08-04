-- https://github.com/Kyligence/KAP/issues/13615

select ifnull(ID2,132322342),  --test bigint null value
       ifnull(ID3,14123),  --test long null value
       ifnull(ID4,313),  --test int null value
       ifnull(price1,12.34),  --test float null value
       ifnull(price2,124.44),  --test double null value
       ifnull(price3,14.242343),  --test decimal(19,6) null value
       ifnull(price5,2),  --test short null value
       ifnull(price6,7),  --test tinyint null value
       ifnull(price7,1),  --test smallint null value
       ifnull(name1,'FT'),  --test string null value
       ifnull(name2,'FT'),  --test varchar(254) null value
       ifnull(name3,'FT'),  --test char null value
       ifnull(name4,2),  --test byte null value
       ifnull(time1,date'2014-3-31'),  --test date null value
      -- ifnull(time2,current_timestamp),  --test timestamp null value
       ifnull(flag,true), --test boolean null value
       isnull(ID2),  --test bigint null value
       isnull(ID3),  --test long null value
       isnull(ID4),  --test int null value
       isnull(price1),  --test float null value
       isnull(price2),  --test double null value
       isnull(price3),  --test decimal(19,6) null value
       isnull(price5),  --test short null value
       isnull(price6),  --test tinyint null value
       isnull(price7),  --test smallint null value
       isnull(name1),  --test string null value
       isnull(name2),  --test varchar(254) null value
       isnull(name3),  --test char null value
       isnull(name4),  --test byte null value
       isnull(time1),  --test date null value
       isnull(time2),  --test timestamp null value
       isnull(flag) --test boolean null value
from TEST_MEASURE

