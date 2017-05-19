<template>
  <div class="paddingbox ksd-common-tab monitor">
    <el-tabs v-model="activeName" type="card" >
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
export default {
  data () {
    return {
      activeName: 'jobs'
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
<style>
.monitor .el-tabs__content {
  overflow:visible;
}
</style>
