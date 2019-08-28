-- all params are constant, will not propose computed column

select max(timestampadd(second, 1, timestamp '1970-01-01 10:01:01')) from tdvt.calcs