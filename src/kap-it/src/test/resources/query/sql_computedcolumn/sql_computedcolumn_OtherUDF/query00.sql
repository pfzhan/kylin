

select count(REGEXP_LIKE("LSTG_FORMAT_NAME", '^FP |GTC$')),
       count(REGEXP_LIKE("LSTG_FORMAT_NAME",'FP-GT+C*')),
       count(REGEXP_LIKE("LSTG_FORMAT_NAME",'FP-(non )?GTC')),
       count(REGEXP_LIKE("LSTG_FORMAT_NAME",'FP-[A-Z][^a-z]C'))
from test_kylin_fact