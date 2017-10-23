<template>
  <div class="paddingbox ksd-common-tab monitor" id="monitor">
    <el-tabs v-model="rootActiveName" type="card">
      <el-tab-pane :label="$t('kylinLang.common.overview')" name="overview" >
      <el-tabs v-model="activeName" class="el-tabs--default">
        <el-tab-pane :label="$t('jobs')" name="jobs" >
          <jobs></jobs>
        </el-tab-pane>
        <el-tab-pane :label="$t('slowQueries')" name="slowQueries" id="slow-query-m">
         <slow_queries></slow_queries>
        </el-tab-pane>
        <el-tab-pane :label="$t('pushDown')" name="pushDown">
         <pushdown_queries></pushdown_queries>
        </el-tab-pane>
      </el-tabs>
    </el-tab-pane>
    </el-tabs>
  </div>
</template>
<script>
import jobs from './jobs_list'
import slowQueries from './slow_queries'
import pushDownQueries from './pushdown_queries'
export default {
  data () {
    return {
      rootActiveName: 'overview',
      activeName: 'jobs'
    }
  },
  mounted () {
    // safari 头部错位的问题
    this.activeName = 'slowQueries'
    this.$nextTick(() => {
      this.activeName = 'jobs'
    })
  },
  methods: {
  },
  computed: {
  },
  components: {
    'jobs': jobs,
    'slow_queries': slowQueries,
    'pushdown_queries': pushDownQueries
  },
  locales: {
    'en': {jobs: 'Jobs', slowQueries: 'Slow Queries', 'pushDown': 'Pushdown Queries'},
    'zh-cn': {jobs: '任务', slowQueries: '慢查询', 'pushDown': 'Pushdown 查询'}
  }
}
</script>
<style lang="less">
@import '../../less/config.less';
.monitor .el-tabs__content {
  overflow:visible;
}
#monitor{
  margin-left: 30px;
  margin-right: 30px;
  .el-tabs__header{
    padding-left: 30px;
    margin-left: -30px;
    margin-right: -30px;
  }
  .el-tabs__item{
    border-color: transparent;
  }
}
#monitor *{
  border-color: @grey-color;
  font-size: 12px;
}
.el-table::after, .el-table::before{
  background: @grey-color;
}
#monitor .el-table--border th {
  border-right:1px solid @tableHBC;
}

</style>
