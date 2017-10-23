<template>
  <div>
    <kap-icon-button icon="external-link" class="ksd-mb-8" type="blue" size="small" @click.native="exportData">{{$t('kylinLang.query.export')}}</kap-icon-button>
     <div style="width:200px;" class="ksd-mb-10 ksd-fright">
        <el-input :placeholder="$t('kylinLang.common.search')" icon="search" v-model="serarchChar" class="show-search-btn" >
          </el-input>
        </div>
    <el-table class="ksd-el-table"
    :data="slowQueriesList"
    @selection-change="handleSelectionChange"
    border
    :default-sort = "{prop: 'last_modified', order: 'descending'}"
    style="width:100%">
        <el-table-column type="expand">
          <template scope="props">
             <el-tabs v-model="activeName" class="el-tabs--default">
                <el-tab-pane :label="$t('kylinLang.common.overview')" name="overview" >
                    <div class="cube-info-view ksd-common-table ksd-mt-10">
                      <el-row class="tableheader">
                        <el-col :span="4" class="left-part"><b>{{$t('modelName')}}</b></el-col>
                        <el-col :span="20">xxxx</el-col>
                      </el-row>
                      <el-row class="tableheader">
                        <el-col :span="4" class="left-part"><b>{{$t('cubeName')}}</b></el-col>
                        <el-col :span="20">xxx</el-col>
                      </el-row>
                      <el-row class="tableheader">
                        <el-col :span="4" class="left-part"><b>{{$t('notificationEmailList')}}</b></el-col>
                        <el-col :span="20">xxx</el-col>
                      </el-row>
                      <el-row class="tableheader">
                        <el-col :span="4" class="left-part"><b>{{$t('notificationEvents')}}</b></el-col>
                        <el-col :span="20">xxx</el-col>
                      </el-row>
                      <el-row class="tableheader">
                        <el-col :span="4" class="left-part"><b>{{$t('description')}}</b></el-col>
                        <el-col :span="20">xxx</el-col>
                      </el-row>
                    </div>
                </el-tab-pane>
            </el-tabs>
          </template>
        </el-table-column>
    <el-table-column
      type="selection"
      width="55">
    </el-table-column>
      <el-table-column
      :label="$t('server')"
      sortable
      show-overflow-tooltip
      prop="server">
      </el-table-column>
      <el-table-column
      :label="$t('user')"
      sortable
      show-overflow-tooltip
      prop="user">
      </el-table-column>
      <el-table-column
      :label="$t('sql')"
      show-overflow-tooltip
      width="150">
        <template scope="scope">
          {{scope.row.sql}}
        </template>
      </el-table-column>
      <el-table-column
      :label="$t('adj')"
      sortable
      show-overflow-tooltip
      prop="adj">
      </el-table-column>
      <el-table-column
      :label="$t('running_seconds')">
        <template scope="scope">
          {{scope.row.running_seconds}} (s)
        </template>
      </el-table-column>
      <el-table-column
      sortable
      show-overflow-tooltip
      :label="$t('start_time')">
        <template scope="scope">
          {{scope.row.start_time}}
        </template>
      </el-table-column>
      <el-table-column
      sortable
      show-overflow-tooltip
      :label="$t('last_modified')">
        <template scope="scope">
          {{scope.row.last_modified}}
        </template>
      </el-table-column>
      <el-table-column
      sortable
      show-overflow-tooltip
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
import { transToGmtTime } from '../../util/business'
export default {
  name: 'pushDownQueries',
  data () {
    return {
      pageSize: 4,
      currentPage: 1,
      project: localStorage.getItem('selected_project') || null,
      activeName: 'overview',
      multipleSelection: []
    }
  },
  created () {
    var para = {
      pageOffset: 0,
      pageSize: pageCount
    }
    if (this.project) {
      para.projectName = this.project
    }
    this.loadSlowQueries({
      page: para
    })
  },
  computed: {
    slowQueriesList () {
      this.$store.state.monitor.slowQueries.forEach((p) => {
        p.start_time = transToGmtTime(p.start_time, this)
      })
      return this.$store.state.monitor.slowQueries
    },
    slowQueiesTotal () {
      return this.$store.state.monitor.totalSlowQueries
    },
    handleSelectionChange (val) {
      this.multipleSelection = val
    },
    exportData () {
    }
  },
  methods: {
    ...mapActions({
      loadSlowQueries: 'LOAD_SLOW_QUERIES'
    }),
    pageCurrentChange (val) {
      var para = {
        pageOffset: val - 1,
        pageSize: pageCount
      }
      if (this.project) {
        para.projectName = this.project
      }
      this.loadSlowQueries({
        page: para
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
