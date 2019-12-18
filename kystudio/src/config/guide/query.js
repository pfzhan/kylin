// query 页面的脚本配置
// let sqls = [
//   `select sum(lo_revenue) as revenue
//   from ssb.lineorder
//   left join ssb.dates on lo_orderdate = d_datekey
//   where d_weeknuminyear = 6 and d_year = 1994
//   and lo_discount between 5 and 7
//   and lo_quantity between 26 and 35;`,
//   `select sum(lo_revenue) as lo_revenue, d_year, p_brand
//   from ssb.lineorder
//   left join ssb.dates on lo_orderdate = d_datekey
//   left join ssb.part on lo_partkey = p_partkey
//   left join ssb.supplier on lo_suppkey = s_suppkey
//   where p_category = 'MFGR#12' and s_region = 'AMERICA'
//   group by d_year, p_brand
//   order by d_year, p_brand;`,
//   `select sum(lo_revenue) as lo_revenue, d_year, p_brand
//   from ssb.lineorder
//   left join ssb.dates on lo_orderdate = d_datekey
//   left join ssb.part on lo_partkey = p_partkey
//   left join ssb.supplier on lo_suppkey = s_suppkey
//   where p_brand between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA'
//   group by d_year, p_brand
//   order by d_year, p_brand;`,
//   `select sum(lo_revenue) as lo_revenue, d_year, p_brand
//   from ssb.lineorder
//   left join ssb.dates on lo_orderdate = d_datekey
//   left join ssb.part on lo_partkey = p_partkey
//   left join ssb.supplier on lo_suppkey = s_suppkey
//   where p_brand = 'MFGR#2239' and s_region = 'EUROPE'
//   group by d_year, p_brand
//   order by d_year, p_brand;`,
//   `select d_year, c_nation, sum(lo_revenue) - sum(lo_supplycost) as profit
//   from ssb.lineorder
//   left join ssb.dates on lo_orderdate = d_datekey
//   left join ssb.customer on lo_custkey = c_custkey
//   left join ssb.supplier on lo_suppkey = s_suppkey
//   left join ssb.part on lo_partkey = p_partkey
//   where c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
//   group by d_year, c_nation
//   order by d_year, c_nation;`,
//   `select d_year, s_nation, p_category, sum(lo_revenue) - sum(lo_supplycost) as profit
//   from ssb.lineorder
//   left join ssb.dates on lo_orderdate = d_datekey
//   left join ssb.customer on lo_custkey = c_custkey
//   left join ssb.supplier on lo_suppkey = s_suppkey
//   left join ssb.part on lo_partkey = p_partkey
//   where c_region = 'AMERICA'and s_region = 'AMERICA'
//   and (d_year = 1997 or d_year = 1998)
//   and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
//   group by d_year, s_nation, p_category
//   order by d_year, s_nation, p_category;`,
//   `select d_year, s_city, p_brand, sum(lo_revenue) - sum(lo_supplycost) as profit
//   from ssb.lineorder
//   left join ssb.dates on lo_orderdate = d_datekey
//   left join ssb.customer on lo_custkey = c_custkey
//   left join ssb.supplier on lo_suppkey = s_suppkey
//   left join ssb.part on lo_partkey = p_partkey
//   where c_region = 'AMERICA'and s_nation = 'UNITED STATES'
//   and (d_year = 1997 or d_year = 1998)
//   and p_category = 'MFGR#14'
//   group by d_year, s_city, p_brand
//   order by d_year, s_city, p_brand;`,
//   `select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
//   from ssb.lineorder
//   left join ssb.dates on lo_orderdate = d_datekey
//   left join ssb.customer on lo_custkey = c_custkey
//   left join ssb.supplier on lo_suppkey = s_suppkey
//   where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_yearmonth = 'Dec1997'
//   group by c_city, s_city, d_year
//   order by d_year asc, lo_revenue desc;`,
//   `select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
//   from ssb.lineorder
//   left join ssb.dates on lo_orderdate = d_datekey
//   left join ssb.customer on lo_custkey = c_custkey
//   left join ssb.supplier on lo_suppkey = s_suppkey
//   where (c_city='UNITED KI1' or c_city='UNITED KI5')
//   and (s_city='UNITED KI1' or s_city='UNITED KI5')
//   and d_year >= 1992 and d_year <= 1997
//   group by c_city, s_city, d_year
//   order by d_year asc, lo_revenue desc;`,
//   `select c_nation, s_nation, d_year, sum(lo_revenue) as lo_revenue
//   from ssb.lineorder
//   left join ssb.dates on lo_orderdate = d_datekey
//   left join ssb.customer on lo_custkey = c_custkey
//   left join ssb.supplier on lo_suppkey = s_suppkey
//   where c_region = 'ASIA' and s_region = 'ASIA'and d_year >= 1992 and d_year <= 1997
//   group by c_nation, s_nation, d_year
//   order by d_year asc, lo_revenue desc;`,
//   `select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
//   from ssb.lineorder
//   left join ssb.dates on lo_orderdate = d_datekey
//   left join ssb.customer on lo_custkey = c_custkey
//   left join ssb.supplier on lo_suppkey = s_suppkey
//   where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES'
//   and d_year >= 1992 and d_year <= 1997
//   group by c_city, s_city, d_year
//   order by d_year asc, lo_revenue desc;`,
//   `select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue from ssb.lineorder left join ssb.dates on lo_orderdate = d_datekey left join ssb.customer on lo_custkey = c_custkey left join ssb.supplier on lo_suppkey = s_suppkey where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_yearmonth = 'Dec1997' group by c_city, s_city, d_year order by d_year asc, lo_revenue desc;`,
//   `select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue from ssb.lineorder left join ssb.dates on lo_orderdate = d_datekey left join ssb.customer on lo_custkey = c_custkey left join ssb.supplier on lo_suppkey = s_suppkey where (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_year >= 1992 and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, lo_revenue desc;`,
//   `select c_nation, s_nation, d_year, sum(lo_revenue) as lo_revenue from ssb.lineorder left join ssb.dates on lo_orderdate = d_datekey left join ssb.customer on lo_custkey = c_custkey left join ssb.supplier on lo_suppkey = s_suppkey where c_region = 'ASIA' and s_region = 'ASIA'and d_year >= 1992 and d_year <= 1997 group by c_nation, s_nation, d_year order by d_year asc, lo_revenue desc;`,
//   `select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue from ssb.lineorder left join ssb.dates on lo_orderdate = d_datekey left join ssb.customer on lo_custkey = c_custkey left join ssb.supplier on lo_suppkey = s_suppkey where c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year >= 1992 and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, lo_revenue desc;`
// ]
// import { batchRequest } from './generate'
// import { verySlowGuideSpeed } from './config'
export function queryDrama () {
  return [
    {
      eventID: 8,
      done: false,
      router: 'Insight', // 跳转到query页面
      group: 1
    },
    // ...batchRequest(sqls),
    {
      eventID: 1,
      done: false,
      target: 'queryBox', // 飞向输入sql的框子
      search: '.guide-WorkSpaceEditor',
      tip: 'queryTipAuto'
    },
    {
      eventID: 21,
      done: false,
      target: 'queryTriggerBtn', // 在编辑器中输入需要查询的SQL
      val: {
        action: 'intoEditor'
      }
    },
    {
      eventID: 21,
      done: false,
      target: 'queryTriggerBtn', // 飞向输入sql的框子
      val: {
        action: 'inputSql',
        data: `select sum(lo_revenue) as revenue
        from ssb.lineorder
        left join ssb.dates on lo_orderdate = d_datekey
        where d_year = 1993
        and lo_discount between 1 and 3
        and lo_quantity < 25`
      }
    },
    {
      eventID: 1,
      done: false,
      target: 'workSpaceSubmit' // 飞向提交按钮
    },
    {
      eventID: 2,
      done: false,
      target: 'workSpaceSubmit' // 点击提交按钮
    }
    // {
    //   eventID: 1,
    //   done: false,
    //   target: 'queryBox', // 飞向输入sql的框子
    //   search: '.guide-WorkSpaceEditor',
    //   timer: verySlowGuideSpeed
    // },
    // {
    //   eventID: 21,
    //   done: false,
    //   target: 'queryTriggerBtn', // 飞向输入sql的框子
    //   val: {
    //     action: 'requestSql',
    //     data: `select c_city, s_city, d_year, sum(lo_revenue) as lo_revenue
    //     from ssb.lineorder
    //     left join ssb.dates on lo_orderdate = d_datekey
    //     left join ssb.customer on lo_custkey = c_custkey
    //     left join ssb.supplier on lo_suppkey = s_suppkey
    //     where (c_city='UNITED KI1' or c_city='UNITED KI5')
    //     and (s_city='UNITED KI1' or s_city='UNITED KI5')
    //     and d_year >= 1992 and d_year <= 1997
    //     group by c_city, s_city, d_year
    //     order by d_year asc, lo_revenue desc`
    //   },
    //   timer: verySlowGuideSpeed
    // },
    // {
    //   eventID: 1,
    //   done: false,
    //   search: '.guide-queryAnswerBy',
    //   timer: verySlowGuideSpeed
    // },
    // {
    //   eventID: 1,
    //   done: false,
    //   search: '.guide-queryResultBox',
    //   timer: verySlowGuideSpeed
    // }
  ]
}
