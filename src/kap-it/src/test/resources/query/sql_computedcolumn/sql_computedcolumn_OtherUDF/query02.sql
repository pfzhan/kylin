
       --CC contian '$' : https://github.com/Kyligence/KAP/issues/14545
select --count(distinct rlike("LSTG_FORMAT_NAME", '^FP |GTC$')),
       count(distinct rlike("LSTG_FORMAT_NAME",'FP-GT+C*')),
       count(distinct rlike("LSTG_FORMAT_NAME",'FP-(non )?GTC')),
       count(distinct rlike("LSTG_FORMAT_NAME",'FP-[A-Z][^a-z]C'))
from test_kylin_fact