<template>
  <div>
    <el-table
    :data="slowQueriesList"
    border
    :default-sort = "{prop: 'last_modified', order: 'descending'}"
    style="width:100%">
      <el-table-column
      :label="$t('server')"
      sortable 
      prop="server">
      </el-table-column>
      <el-table-column
      :label="$t('user')"
      sortable
      prop="user">
      </el-table-column>
      <el-table-column
      :label="$t('sql')"
      sortable
      width="350">
        <template scope="scope">
          {{scope.row.sql}}
        </template>
      </el-table-column>
      <el-table-column
      :label="$t('adj')"
      sortable
      prop="adj">
      </el-table-column>
      <el-table-column
      sortable
      :label="$t('running_seconds')">
        <template scope="scope">
          {{scope.row.running_seconds}} (s)
        </template>
      </el-table-column>
      <el-table-column
      sortable
      :label="$t('start_time')">
        <template scope="scope">
          {{scope.row.start_time}}
        </template>
      </el-table-column>
      <el-table-column
      sortable
      :label="$t('last_modified')">
        <template scope="scope">
          {{scope.row.last_modified}}
        </template>
      </el-table-column>
      <el-table-column
      sortable
      :label="$t('thread')"
      prop="thread">
      </el-table-column>
    </el-table>
    <pager class="ksd-center"  :totalSize="slowQueiesTotal"  v-on:handleCurrentChange='pageCurrentChange' ></pager>
  </div>
</template>

<script>
import { mapActions } from 'vuex'
import { pageCount } from '../../config'
export default {
  name: 'slowQueriesList',
  data () {
    return {
      pageSize: 4,
      currentPage: 1,
      project: localStorage.getItem('selected_project')
    }
  },
  created () {
    this.loadSlowQueries({
      page: {
        pageOffset: 0,
        projectName: this.project,
        pageSize: pageCount
      }
    })
  },
  computed: {
    slowQueriesList () {
      return this.$store.state.monitor.slowQueries
    },
    slowQueiesTotal () {
      return this.$store.state.monitor.totalSlowQueries
    }
  },
  methods: {
    ...mapActions({
      loadSlowQueries: 'LOAD_SLOW_QUERIES'
    }),
    pageCurrentChange (val) {
      this.loadSlowQueries({
        page: {
          projectName: this.project,
          pageOffset: val - 1,
          pageSize: pageCount
        }
      })
    }
  },
  locales: {
    'en': {server: 'Server', user: 'User', sql: 'Sql', adj: 'Description', running_seconds: 'Running Seconds', start_time: 'Start Time', last_modified: 'Last Modified', thread: 'Thread'},
    'zh-cn': {server: '服务器', user: '用户', sql: 'Sql', adj: '描述', running_seconds: '运行时间', start_time: '开始时间', last_modified: '最后修改时间', thread: '线程'}
  }
}
</script>
<style scope="">

</style>
