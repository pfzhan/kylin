select concat_ws(';',collect_set(ID1+1)),
       concat_ws(';',collect_set(ID2+1)),
       concat_ws(',',collect_set(ID3+1)),
       concat_ws('_',collect_set(concat(name1,name1))),
       concat_ws(' ',collect_set(concat(name2,name2))),
       concat_ws(';',collect_set(concat(name3,name3))),
       concat_ws(';',collect_set(concat(name4,name4))),
       concat_ws('-',collect_set(price1+1)),
       concat_ws(';',collect_set(price2+1)),
       concat_ws(';',collect_set(price3+1)),
       concat_ws(';',collect_set(price5+1)),
       concat_ws(';',collect_set(price6+1)),
       concat_ws(';',collect_set(price7+1)),
       concat_ws(';',collect_set(concat(time1,1))),
       concat_ws(';',collect_set(concat(time2,1))),
       concat_ws(';',collect_set(concat(flag,1)))
from test_measure
limit 10