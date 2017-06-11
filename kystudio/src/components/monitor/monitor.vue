<template>
  <div class="paddingbox ksd-common-tab monitor" id="monitor">
    <el-tabs v-model="activeName" type="card" v-if="isAdmin">
      <el-tab-pane :label="$t('jobs')" name="jobs">
        <jobs></jobs>
      </el-tab-pane>
      <el-tab-pane :label="$t('slowQueries')" name="slowQueries">
       <slow_queries></slow_queries>
      </el-tab-pane>
    </el-tabs>
  </div>  
</template>
<script>
import jobs from './jobs_list'
import slowQueries from './slow_queries'
import { hasRole } from '../../util/business'
export default {
  data () {
    return {
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
  computed: {
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  components: {
    'jobs': jobs,
    'slow_queries': slowQueries
  },
  locales: {
    'en': {jobs: 'Jobs', slowQueries: 'Slow Queries'},
    'zh-cn': {jobs: '任务', slowQueries: '慢查询'}
  }
}
</script>
<style lang="less">
@import '../../less/config.less';
.monitor .el-tabs__content {
  overflow:visible;
}
#monitor *{
  border-color: @grey-color;
}
.el-table::after, .el-table::before{
  background: @grey-color;
}
</style>
