--https://github.com/Kyligence/KAP/issues/13612

--test to_char(timestamp,part)
select to_char(TEST_TIME_ENC,'YEAR'),
       to_char(TEST_TIME_ENC,'Y'),
       to_char(TEST_TIME_ENC,'y'),
       to_char(TEST_TIME_ENC,'MONTH'),
       to_char(TEST_TIME_ENC,'M'),
       to_char(TEST_TIME_ENC,'DAY'),
       to_char(TEST_TIME_ENC,'D'),
       to_char(TEST_TIME_ENC,'d'),
       to_char(TEST_TIME_ENC,'HOUR'),
       to_char(TEST_TIME_ENC,'H'),
       to_char(TEST_TIME_ENC,'h'),
       to_char(TEST_TIME_ENC,'MINUTE'),
       to_char(TEST_TIME_ENC,'MINUTES'),
       to_char(TEST_TIME_ENC,'m'),
       to_char(TEST_TIME_ENC,'SECOND'),
       to_char(TEST_TIME_ENC,'SECONDS'),
       to_char(TEST_TIME_ENC,'s')
from TEST_ORDER