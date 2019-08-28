-- one param of timestampdiff expression contains cast

select sum(timestampdiff(second, time0, cast(datetime1 as timestamp))) from tdvt.calcs;