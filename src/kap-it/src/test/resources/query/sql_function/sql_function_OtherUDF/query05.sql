--https://github.com/Kyligence/KAP/issues/14368

select IF(ID2 > 0, IF(ID2 > 90, '优秀', IF(ID2 > 70, '良好', '中等')), '非法分数'),--test bigint
       IF(ID3 > 0, IF(ID3 > 90, '优秀', IF(ID3 > 70, '良好', '中等')), '非法分数'),--test long
       IF(ID4 > 0, IF(ID4 > 90, '优秀', IF(ID4 > 70, '良好', '中等')), '非法分数'),--test int
       IF(price1 > 0, IF(price1 > 90, '优秀', IF(price1 > 70, '良好', '中等')), '非法分数'),--test float
       IF(price2 > 0, IF(price2 > 90, '优秀', IF(price2 > 70, '良好', '中等')), '非法分数'),--test double
       IF(price3 > 0, IF(price3 > 90, '优秀', IF(price3 > 70, '良好', '中等')), '非法分数'),--test decimal(19,6)
       IF(price5 > 0, IF(price5 > 90, '优秀', IF(price5 > 70, '良好', '中等')), '非法分数'),--test short
       IF(price6 > 0, IF(price6 > 90, '优秀', IF(price6 > 70, '良好', '中等')), '非法分数'),--test tinyint
       IF(price7 > 0, IF(price7 > 90, '优秀', IF(price7 > 70, '良好', '中等')), '非法分数'),--test smallint
       IF(name1 = 'FT','FT',name1), --test string
       IF(name2 = 'FT','FT',name2), --test varchar(254)
       IF(name3 = 'FT','FT',name3), --test char
       IF(name4 = 2, 2 ,name4), --test byte
       IF(time1 = date'2014-3-31', date'2014-3-31' ,time1 ), --test date
       IF(time2 = timestamp'2019-08-08 16:33:41', timestamp'2019-08-08 16:33:41' ,time2 ), --test timestamp
       IF(flag = true, true ,false) --test boolean
from TEST_MEASURE