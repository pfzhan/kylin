<template>
  <div class="pushDownQueries">
    <kap-icon-button :disabled="multipleSelection.length ===0" icon="external-link" class="ksd-mb-8" type="blue" size="small" @click.native="exportCheckedQueries">{{$t('kylinLang.query.export')}}</kap-icon-button>
   <!--     <div style="width:200px;" class="ksd-mb-10 ksd-fright">
        <el-input :placeholder="$t('kylinLang.common.search')" icon="search" v-model="serarchChar" class="show-search-btn" >
          </el-input>
        </div> -->
 
    <el-alert class="ksd-mt-10 ksd-mb-10 trans ksd-center" v-show="allSelectTipVisible"
        :closable="false"
        title=""
        type="warning">
        <p v-if="isSelectAll"> {{$t('tipTitleText', {count: pushDownQueiesTotal })}}&nbsp;<a @click="clearSelection">{{$t('clearSelection')}}</a></p>
        <p v-else> {{$t('tipTitleText', {count: multipleSelection.length })}}&nbsp;<a @click="toggleChecked(pushDownQueriesList)">{{$t('selectAllTip')}}</a></p>
      </el-alert>
    <el-table class="ksd-el-table"
    :data="pushDownQueriesList"
    ref="pushdwonTable"
    @selection-change="handleSelectionChange"
    border
    :default-sort = "{prop: 'last_modified', order: 'descending'}"
    style="width:100%">
        <el-table-column type="expand">
          <template slot-scope="scope">
             <el-tabs v-model="activeName" class="el-tabs--default">
                <el-tab-pane :label="$t('kylinLang.common.overview')" name="overview" >
                    <div class="ksd-common-table ksd-mt-10">
                      <el-row class="tableheader">
                        <el-col :span="4" class="left-part">{{$t('running_seconds')}}</el-col>
                        <el-col :span="20" class="ksd-left ksd-pl-10">{{scope.row.running_seconds}} (s)</el-col>
                      </el-row>
                      <el-row class="tableheader">
                        <el-col :span="4" class="left-part">{{$t('start_time')}}</el-col>
                        <el-col :span="20" class="ksd-left ksd-pl-10">{{scope.row.start_time}}</el-col>
                      </el-row>
                      <el-row class="tableheader">
                        <el-col :span="4" class="left-part">{{$t('user')}}</el-col>
                        <el-col :span="20" class="ksd-left ksd-pl-10">{{scope.row.user}}</el-col>
                      </el-row>
                      <el-row class="tableheader">
                        <el-col :span="4" class="left-part">{{$t('thread')}}</el-col>
                        <el-col :span="20" class="ksd-left ksd-pl-10">{{scope.row.thread}}</el-col>
                      </el-row>
                      <el-row class="tableheader ksd-h-144">
                        <el-col :span="4" class="left-part ksd-h-144">{{$t('sql')}}</el-col>
                        <el-col :span="20" class="ksd-h-144 ksd-pt-10 ksd-pl-10 ksd-pr-10 ksd-pb-10">
                         <el-input 
                            type="textarea"
                            :rows="4"
                            :readonly="true"
                            v-model="scope.row.sql">
                          </el-input>
                          <el-button size="mini" v-clipboard:copy="scope.row.sql"
      v-clipboard:success="onCopy(scope.row)" v-clipboard:error="onError" type="default" class="ksd-fleft ksd-mt-10 ksd-mb-20 copy-btn">{{$t('kylinLang.common.copy')}}</el-button>
      <transition name="fade">
        <div class="copyStatusMsg" v-show="scope.row.showCopyStatus" ><i class="el-icon-circle-check"></i> <span>{{$t('kylinLang.common.copySuccess')}}</span></div>
      </transition>
                        </el-col>
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
      :width="100"
      sortable
      show-overflow-tooltip
      prop="server">
      </el-table-column>
      <el-table-column
      :label="$t('user')"
      :width="100"
      sortable
      show-overflow-tooltip
      prop="user">
      </el-table-column>
      <el-table-column
      :label="$t('sql')"
      class-name="hideOverContent"
      prop="sql">
        <template slot-scope="scope">
           <common-tip :content="scope.row.sql" placement="top-start">
            <p>{{scope.row.sql}}</p>
          </common-tip>
        </template>
      </el-table-column>
      <el-table-column
      :label="$t('adj')"
      sortable
      :width="155"
      show-overflow-tooltip
      prop="adj">
      </el-table-column>
      <el-table-column
      :width="165"
      sortable
      prop="running_seconds"
      :label="$t('running_seconds')">
        <template slot-scope="scope">
          {{scope.row.running_seconds}} (s)
        </template>
      </el-table-column>
      <el-table-column
      sortable
      show-overflow-tooltip
      :width="150"
      prop="start_time"
      :label="$t('start_time')">
        <template slot-scope="scope">
          {{scope.row.start_time}}
        </template>
      </el-table-column>
      <el-table-column
      sortable
      :width="150"
      show-overflow-tooltip
      prop="last_modified"
      :label="$t('last_modified')">
        <template slot-scope="scope">
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
    <pager class="ksd-center"  :totalSize="pushDownQueiesTotal" v-on:handleCurrentChange='pageCurrentChange' ></pager>
    <form name="export" class="exportToolOfPushDown" action="/kylin/api/diag/export/push_down" method="post">
      <input type="hidden" name="all" v-model="isSelectAll"/>
      <input type="hidden" name="project" v-model="project"/>
      <input type="hidden" name="selectedQueries" v-model="multipleSelection" v-if="!isSelectAll"/>
    </form>
  </div>
</template>

<script>
import { mapActions } from 'vuex'
import { pageCount } from '../../config'
import { transToGmtTime } from '../../util/business'
import { objectClone } from '../../util/index'
export default {
  name: 'pushDownQueries',
  data () {
    return {
      pageSize: 4,
      currentPage: 1,
      project: localStorage.getItem('selected_project') || null,
      activeName: 'overview',
      multipleSelection: [],
      isSelectAll: false,
      serarchChar: ''
    }
  },
  created () {
    var para = {
      pageOffset: 0,
      pageSize: pageCount
    }
    if (this.project) {
      para.project = this.project
    }
    this.loadPushdownQueries({
      page: para
    })
  },
  computed: {
    pushDownQueriesList () {
      var clonePushDownList = objectClone(this.$store.state.monitor.pushdownQueries)
      clonePushDownList.forEach((p) => {
        p.start_time = transToGmtTime(p.start_time, this)
        if (p.last_modified === 0) {
          p.last_modified = ''
        } else {
          p.last_modified = transToGmtTime(p.last_modified, this)
        }
        p.showCopyStatus = false
      })
      return clonePushDownList
    },
    pushDownQueiesTotal () {
      return this.$store.state.monitor.totalPushDownQueries
    },
    allSelectTipVisible () {
      return this.multipleSelection.length === this.pushDownQueriesList.length && this.multipleSelection.length !== 0
    }
  },
  methods: {
    ...mapActions({
      loadPushdownQueries: 'LOAD_PUSHDOWN_QUERIES',
      exportPushDown: 'EXPORT_PUSHDOWN'
    }),
    handleSelectionChange (val) {
      this.multipleSelection = []
      for (var i = val.length - 1; i >= 0; i--) {
        this.multipleSelection.push(val[i].uuid)
      }
      if (this.multipleSelection.length === this.pushDownQueiesTotal) {
        this.isSelectAll = true
      } else {
        this.isSelectAll = false
      }
    },
    onCopy (s) {
      return () => {
        s.showCopyStatus = true
        setTimeout(() => {
          s.showCopyStatus = false
        }, 3000)
      }
    },
    onError () {
      this.$message(this.$t('kylinLang.common.copyfail'))
    },
    clearSelection () {
      this.$refs.pushdwonTable.clearSelection()
    },
    toggleChecked (rows) {
      rows.forEach(row => {
        this.$refs.pushdwonTable.toggleRowSelection(row, true)
      })
      this.isSelectAll = true
    },
    pageCurrentChange (val) {
      var para = {
        pageOffset: val - 1,
        pageSize: pageCount
      }
      if (this.project) {
        para.project = this.project
      }
      this.loadPushdownQueries({
        page: para
      })
    },
    exportCheckedQueries () {
      this.$nextTick(() => {
        console.log(this.$el.querySelectorAll('.exportToolOfPushDown')[0])
        this.$el.querySelectorAll('.exportToolOfPushDown')[0].submit()
      })
    }
  },
  locales: {
    'en': {server: 'Server', user: 'User', sql: 'Sql', adj: 'Description', running_seconds: 'Running Seconds', start_time: 'Start Time', last_modified: 'Last Modified', thread: 'Thread', tipTitleText: 'All {count} queries on this page are selected.', clearSelection: 'Clear selection', selectAllTip: 'Click here to select all queries from all pages.'},
    'zh-cn': {server: '服务器', user: '用户', sql: 'Sql', adj: '描述', running_seconds: '运行时间', start_time: '开始时间', last_modified: '最后修改时间', thread: '线程', tipTitleText: '所有页面的 {count} 个查询被选中。', clearSelection: '取消选中', selectAllTip: '点击此处选择所有页面的所有查询。'}
  }
}
</script>
<style scope="" lang="less">
.pushDownQueries{
  .hideOverContent{
    text-overflow:ellipsis;
    .cell{
      white-space:nowrap;
      text-overflow: ellipsis;
      p{
         white-space:nowrap;
        text-overflow: ellipsis;
        overflow: hidden;
      }
    }
  }
  .left-part{
    border-right:solid 1px #393e53;
    text-align: right;
    padding-right: 20px;
  }
  .el-alert {
    p{
      color:#E3AB29;
    }
    a{
      color:#218fea;
      text-decoration: underline;
    }
  }
  .copy-btn{
  }
  .copyStatusMsg {
    padding-top: 2px;
    float: left;
    margin-left: 4px;
    color:#fff;
    i{
      color:#28cd6b;
    }
    span{
      color:#fff;
      font-size: 12px;
    }
  }
}
</style>
