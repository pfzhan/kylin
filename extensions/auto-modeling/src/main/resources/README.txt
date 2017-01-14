SOME Sample piece of script

File auto-cube-dryrun.sh:
    source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
    source ${dir}/check-env.sh "if-not-yet"
    ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.modeling.auto.mockup.MockupRunner "$@" \
            || exit 1
    exit 0

File auto-cube-service.sh:
    source $(cd -P -- "$(dirname -- "$0")" && pwd -P)/header.sh
    source ${dir}/check-env.sh "if-not-yet"
    ${KYLIN_HOME}/bin/kylin.sh io.kyligence.kap.modeling.auto.AutomodelingService "$@" \
            || exit 1
    exit 0


Entry cmd:
./bin/auto-cube-service.sh ssb \
    "select sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where d_year = 1993;" \
    "select sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where d_yearmonthnum = 199401;" \
    "select sum(v_revenue) as revenue from p_lineorder left join dates on lo_orderdate = d_datekey where d_weeknuminyear = 6 and d_year = 1994;" \
    "select sum(lo_revenue) as lo_revenue, d_year, p_brand from p_lineorder left join dates on lo_orderdate = d_datekey left join part on lo_partkey = p_partkey left join supplier on lo_suppkey = s_suppkey where p_category = 'MFGR#0702' and s_region = 'AMERICA' group by d_year, p_brand order by d_year, p_brand;"
