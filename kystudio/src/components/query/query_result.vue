<template>
  <div class="result_box">
    <div class="ksd-title-label-small ksd-mb-10">{{$t('extraoptionrmation')}}</div>
    <div class="resultTipsLine">
      <div class="resultTips">
        <p class="resultText">
          <span class="label">{{$t('kylinLang.query.query_id')}}: </span>
          <span class="text">{{extraoption.queryId}}</span>
          <common-tip :content="$t('linkToSpark')" v-if="extraoption.appMasterURL">
            <a target="_blank" :href="extraoption.appMasterURL"><i class="el-icon-ksd-go"></i></a>
          </common-tip>
        </p>
        <!-- <p class="resultText">
          <span class="label">{{$t('kylinLang.query.status')}}</span>
          <span>{{$t('kylinLang.common.success')}}</span>
        </p> -->
        <p class="resultText">
          <span class="label">{{$t('kylinLang.query.duration')}}: </span>
          <span class="text">{{(extraoption.duration/1000)|fixed(2)||0.00}}s</span>
        </p>
        <p class="resultText query-obj" :class="{'guide-queryAnswerBy': isWorkspace}">
          <span class="label">{{$t('kylinLang.query. answered_by')}}: </span>
          <span class="text" :title="answeredBy">{{answeredBy}}</span>
        </p>
        <el-button plain size="mini" @click="toggleDetail" class="show-more-btn">
          {{$t('kylinLang.common.seeDetail')}}
          <i class="el-icon-arrow-down" v-show="!showDetail"></i>
          <i class="el-icon-arrow-up" v-show="showDetail"></i>
        </el-button>
      </div>
      <div class="resultTips" v-show="showDetail">
        <p class="resultText">
          <span class="label">{{$t('kylinLang.query.queryNode')}}: </span>
          <span class="text">{{extraoption.server}}</span>
        </p>
        <p class="resultText" v-if="!extraoption.pushDown">
          <span class="label">{{$t('kylinLang.query.total_scan_count')}}: </span>
          <span class="text">{{extraoption.totalScanCount}}</span>
        </p>
        <p class="resultText" v-if="!extraoption.pushDown">
          <span class="label">{{$t('kylinLang.query.result_row_count')}}: </span>
          <span class="text">{{extraoption.resultRowCount}}</span>
        </p>
      </div>
    </div>
    <div :class="[{'ksd-header': !showExportCondition}]">
      <div :class="['ksd-title-label-small', 'result-title', showExportCondition ? 'ksd-mt-20' : 'result-title-float', {'guide-queryResultBox': isWorkspace}]">{{$t('queryResults')}}</div>
      <div :class="['clearfix', {'ksd-mt-15': showExportCondition}]">
        <div class="ksd-fleft">
          <el-button v-if="showExportCondition" type="primary" plain size="small" @click.native="exportData">
            {{$t('exportCSV')}}
          </el-button>
        </div>
        <div class="resultOperator ksd-fright">
          <el-input :placeholder="$t('kylinLang.common.pleaseFilter')" v-model="resultFilter" class="show-search-btn ksd-inline" size="small" prefix-icon="el-icon-search">
          </el-input>
        </div>
      </div>
    </div>
  	<div class="ksd-mt-10 grid-box narrowTable">
  		<el-table
		    :data="pagerTableData"
		    border
        ref="tableLayout"
		    style="width: 100%;">
		    <el-table-column v-for="(value, index) in tableMeta" :key="index"
		      :prop="''+index"
          :min-width="52+15*(value.label&&value.label.length || 0)"
		      :label="value.label">
          <template slot-scope="props">
            <pre class="table-cell-text">{{props.row[index]}}</pre>
          </template>
		    </el-table-column>
		  </el-table>

      <kap-pager v-on:handleCurrentChange='pageSizeChange' class="ksd-center ksd-mtb-10" ref="pager" :totalSize="modelsTotal"></kap-pager>
  	</div>
    <form name="export" class="exportTool" action="/kylin/api/query/format/csv" method="post">
      <input type="hidden" name="sql" v-model="sql"/>
      <input type="hidden" name="project" v-model="project"/>
      <input type="hidden" name="limit" v-model="limit" v-if="limit"/>
    </form>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { scToFloat, showNull } from '../../util/index'
import { hasRole, transToGmtTime } from '../../util/business'
@Component({
  props: ['extraoption', 'isWorkspace', 'queryExportData'],
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      query: 'QUERY_BUILD_TABLES'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  locales: {
    'en': {
      username: 'Username',
      role: 'Role',
      analyst: 'Analyst',
      modeler: 'Modeler',
      admin: 'Admin',
      save: 'Save',
      restore: 'Restore',
      lineChart: 'Line Chart',
      barChart: 'Bar Chart',
      pieChart: 'Pie Chart',
      traceUrl: 'Trace Url:',
      extraoptionrmation: 'Query Information',
      queryResults: 'Query Results',
      exportCSV: 'Export to CSV',
      linkToSpark: 'Jump to Spark Web UI'
    },
    'zh-cn': {
      username: '用户名',
      role: '角色',
      analyst: '分析人员',
      modeler: '建模人员',
      admin: '管理人员',
      save: '保存',
      restore: '还原',
      lineChart: '折线图',
      barChart: '柱状图',
      pieChart: '饼状图',
      traceUrl: '追踪链接：',
      extraoptionrmation: '查询信息',
      queryResults: '查询结果',
      exportCSV: '导出 CSV',
      linkToSpark: '跳转至 Spark 任务详情'
    }
  }
})
export default class queryResult extends Vue {
  resultFilter = ''
  tableData = []
  tableMeta = []
  tableMetaBackup = []
  pagerTableData = []
  graphType = 'line'
  sql = ''
  project = ''
  limit = ''
  showDetail = false
  pageSize = 10
  modelsTotal = this.extraoption.results.length
  timer = null
  pageX = 0
  pageSizeX = 6
  exportData () {
    // 区别于3x中，导出所需的参数，存在props 传进来的 queryExportData 这个对象中，不再一起放在 extraoption 中
    this.sql = this.queryExportData.sql
    this.project = this.currentSelectedProject
    this.limit = this.queryExportData.limit
    this.$nextTick(() => {
      this.$el.querySelectorAll('.exportTool')[0].submit()
    })
  }
  transDataForGrid (data) {
    var columnMeata = this.extraoption.columnMetas
    var lenOfMeta = columnMeata.length
    for (var i = 0; i < lenOfMeta; i++) {
      this.tableMeta.push(columnMeata[i])
    }
    this.tableMetaBackup = this.tableMeta
    this.tableMeta = this.tableMetaBackup.slice(0, (this.pageX + 1) * this.pageSizeX)
    this.pageSizeChange(0)
  }
  toggleDetail () {
    this.showDetail = !this.showDetail
  }
  get answeredBy () {
    if (this.extraoption.realizations && this.extraoption.realizations.length) {
      return this.extraoption.realizations.map((i) => {
        return i.modelAlias
      }).join(',')
    } else {
      return this.extraoption.engineType
    }
  }
  filterTableData () {
    if (this.resultFilter) {
      const filteredData = this.extraoption.results.filter((item) => {
        const cur = item
        const trans = scToFloat(cur)
        const finalItem = showNull(trans)
        return finalItem.toString().toLocaleUpperCase().indexOf(this.resultFilter.toLocaleUpperCase()) !== -1
      })
      this.modelsTotal = filteredData.length
      return filteredData
    } else {
      this.modelsTotal = this.extraoption.results.length
      return this.extraoption.results
    }
  }
  pageSizeChange (currentPage, pageSize) {
    if (pageSize) {
      this.pageSize = pageSize
    }
    const filteredData = this.filterTableData()
    this.tableData = filteredData.slice(currentPage * this.pageSize, (currentPage + 1) * this.pageSize)
    var len = this.tableData.length
    for (let i = 0; i < len; i++) {
      var innerLen = this.tableData[i].length
      for (var m = 0; m < innerLen; m++) {
        var cur = this.tableData[i][m]
        var trans = scToFloat(cur)
        this.tableData[i][m] = showNull(trans)
      }
    }
    this.pagerTableData = Object.assign([], this.tableData)
  }
  getMoreData () {
    if (this.$refs.tableLayout.scrollPosition === 'right') {
      this.tableMeta = this.tableMetaBackup.slice(0, (++this.pageX + 1) * this.pageSizeX)
    }
  }
  @Watch('resultFilter')
  onResultFilterChange (val) {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      this.pageSizeChange(0)
    }, 500)
  }
  @Watch('extraoption')
  onExtraoptionChange (val) {
    this.tableData = []
    this.tableMeta = []
    this.pagerTableData = []
    this.transDataForGrid()
  }
  created () {
    this.transDataForGrid()
  }
  get showExportCondition () {
    return this.$store.state.system.allowAdminExport === 'true' && this.isAdmin || this.$store.state.system.allowNotAdminExport === 'true' && !this.isAdmin
  }
  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }
  mounted () {
    this.$refs.tableLayout.bodyWrapper.addEventListener('scroll', this.getMoreData)
  }
  beforeDestory () {
    this.$refs.tableLayout.bodyWrapper.removeEventListener('scroll', this.getMoreData)
  }
}
</script>
<style  lang="less">
  @import '../../assets/styles/variables.less';
  .narrowTable{
    .el-table td, .el-table th{
      height: 30px;
    }
  }
  .ksd-header {
    margin-top: 26px;
  }
  .result-title {
    position: relative;
    top: 6px;
    &.result-title-float {
      float: left;
      top: 2px;
    }
  }
  .resultTipsLine{
    font-size: 14px;
    padding: 10px;
    background-color: @table-stripe-color;
    line-height: 20px;
    position: relative;
    .show-more-btn {
      position: absolute;
      right: 10px;
      top: 10px;
    }
    .resultTips{
      align-items: center;
      flex-wrap:wrap;
      .resultText{
        color:@color-text-primary;
        .label {
          font-weight: bold;
        }
        .text{
          color:@color-text-primary;
        }
        a {
          color: @base-color;
        }
        &.query-obj {
          .text {
            width: calc(~'100% - 85px');
            display: inline-block;
            overflow: hidden;
            text-overflow: ellipsis;
            position: absolute;
            margin-left: 5px;
          }
        }
      }
    }
  }
  .result_box{
    .el-table .cell{
       word-break: break-all!important;
    }
    .resultOperator {
      .el-input {
        width: auto;
      }
    }
  }
  .table-cell-text{
    word-wrap: break-word;
    word-break: break-all;
    white-space: pre-wrap;
    color: @text-normal-color;
    font-family: Lato,"Noto Sans S Chinese",sans-serif;
  }
</style>
