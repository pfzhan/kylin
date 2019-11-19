select concat_ws(';',collect_set(ID1)),
       concat_ws(';',collect_set(ID2)),
       concat_ws(',',collect_set(ID3)),
       concat_ws('_',collect_set(name1)),
       concat_ws(' ',collect_set(name2)),
       concat_ws(';',collect_set(name3)),
       concat_ws(';',collect_set(name4)),
       concat_ws('-',collect_set(price1)),
       concat_ws(';',collect_set(price2)),
       concat_ws(';',collect_set(price3)),
       concat_ws(';',collect_set(price5)),
       concat_ws(';',collect_set(price6)),
       concat_ws(';',collect_set(price7)),
       concat_ws(';',collect_set(time1)),
       concat_ws(';',collect_set(time2)),
       concat_ws(';',collect_set(flag))
from test_measure
limit 10