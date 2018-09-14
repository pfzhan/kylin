<template>
  <div class="result_box">
    <div class="resultTipsLine">
      <el-row :gutter="20" class="resultTips">
        <el-col :span="7" class="resultText">
          <p>
            <span class="label">{{$t('kylinLang.query.queryId')}}</span>
            <span class="text">{{queryInfo.queryId}}</span>
          </p>
        </el-col>
        <el-col :span="7" class="resultText">
          <p v-if="extraoption.data.pushDown">
            <span class="label">{{$t('kylinLang.query.duration')}}</span>
            <span class="text">{{(queryInfo.duration/1000)|fixed(2)||0.00}}s</span>
          </p>
          <p v-else>
            <span class="label">{{$t('kylinLang.query.modelName')}}</span>
            <span class="text">{{queryInfo.modelName | arrayToStr}}</span>
          </p>
        </el-col>
        <el-col :span="7" class="resultText">
          <p v-if="extraoption.data.pushDown">
            <span class="label">{{$t('kylinLang.query.realization')}}</span>
            <span class="text">{{queryInfo.realization | arrayToStr}}</span>
          </p>
          <p v-else>
            <span class="label">{{$t('kylinLang.query.scanCount')}}</span>
            <span class="text">{{queryInfo.scanCount}}</span>
          </p>
        </el-col>
        <el-col :span="3" class="ksd-right" v-if="!extraoption.data.pushDown">
          <el-button plain size="medium" @click="toggleDetail">
            {{$t('kylinLang.common.seeDetail')}} 
            <i class="el-icon-arrow-down" v-show="!showDetail"></i>
            <i class="el-icon-arrow-up" v-show="showDetail"></i>
          </el-button>
        </el-col>
      </el-row>
      <el-row :gutter="20" class="resultTips" v-show="showDetail" v-if="!extraoption.data.pushDown">
        <el-col :span="7" class="resultText">
          <p>
            <span class="label">{{$t('kylinLang.query.duration')}}</span>
            <span class="text">{{(queryInfo.duration/1000)|fixed(2)||0.00}}s</span>
          </p>
        </el-col>
        <el-col :span="7" class="resultText">
          <p>
            <span class="label">{{$t('kylinLang.query.realization')}}</span>
            <span class="text">{{queryInfo.realization | arrayToStr}}</span>
          </p>
        </el-col>
        <el-col :span="7" class="resultText">
          <p>
            <span class="label">{{$t('kylinLang.query.resultRows')}}</span>
            <span class="text">{{queryInfo.resultRows}}</span>
          </p>
        </el-col>
      </el-row>
    </div>
    <div class="clearfix">
      <div class="ksd-title-label ksd-fleft ksd-mt-30">{{$t('queryInformation')}}</div>
      <div class="resultOperator ksd-mt-20 ksd-fright">
        <el-input :placeholder="$t('kylinLang.common.pleaseFilter')" v-model="resultFilter" class="show-search-btn ksd-inline" size="small" prefix-icon="el-icon-search">
        </el-input>
        <kap-icon-button v-if="showExportCondition" size="small" icon="el-icon-ksd-download" @click.native="exportData">
          {{$t('kylinLang.query.export')}}
        </kap-icon-button>
        <kap-icon-button size="small" type="primary" icon="el-icon-ksd-table_save" @click.native="openSaveQueryDialog">{{$t('kylinLang.query.saveQuery')}}</kap-icon-button>
      </div>
    </div>
  	<div class="ksd-mt-10 grid-box narrowTable">
  		<el-table
		    :data="pagerTableData"
		    border
		    style="width: 100%;">
		    <el-table-column v-for="(value, index) in tableMeta" :key="index"
		      :prop="''+index"
          :min-width="52+15*(value.label&&value.label.length || 0)"
		      :label="value.label"
          sortable
          >
		    </el-table-column>
		  </el-table>

      <pager v-on:handleCurrentChange='pageSizeChange' class="ksd-center" ref="pager"  :totalSize="modelsTotal"  ></pager>
  	</div>
    <save_query_dialog :show="saveQueryFormVisible" :extraoption='extraoption' v-on:closeModal="closeModal"></save_query_dialog>
    <form name="export" class="exportTool" action="/kylin/api/query/format/csv" method="post">
      <input type="hidden" name="sql" v-model="sql"/>
      <input type="hidden" name="project" v-model="project"/>
      <input type="hidden" name="limit" v-model="limit" v-if="limit"/>
    </form>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import { scToFloat, showNull } from '../../util/index'
import { hasRole, transToGmtTime } from '../../util/business'
import saveQueryDialog from './save_query_dialog'
@Component({
  props: ['extraoption'],
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      query: 'QUERY_BUILD_TABLES'
    })
  },
  components: {
    'save_query_dialog': saveQueryDialog
  },
  locales: {
    'en': {username: 'Username', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin', save: 'Save', restore: 'Restore', lineChart: 'Line Chart', barChart: 'Bar Chart', pieChart: 'Pie Chart', traceUrl: 'Trace Url:', queryInformation: 'Query Information'},
    'zh-cn': {username: '用户名', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '管理人员', save: '保存', restore: '还原', lineChart: '折线图', barChart: '柱状图', pieChart: '饼状图', traceUrl: '追踪链接：', queryInformation: '查询信息'}
  }
})
export default class queryResult extends Vue {
  rules = {
    name: [
      { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'blur' }
    ]
  }
  saveQueryFormVisible = false
  resultFilter = ''
  tableData = []
  tableMeta = []
  pagerTableData = []
  dateTypes = [91, 92, 93]
  stringTypes = [-1, 1, 12]
  numberTypes = [-7, -6, -5, 3, 4, 5, 6, 7, 8]
  datePattern = /_date|_dt/i
  selectDimension = ''
  selectMetrics = ''
  graphType = 'line'
  sql = ''
  project = ''
  limit = ''
  queryInfo = {
    duration: '-',
    modelName: '-',
    queryId: '',
    realization: '-',
    scanCount: 0,
    resultRows: 0
  }
  showDetail = false

  refreshQuery () {
    this.$emit('changeView', this.extraoption.index, this.extraoption, 'circle-o-notch', 'querypanel')
  }
  exportData () {
    this.sql = this.extraoption.sql
    this.project = this.extraoption.project
    this.limit = this.extraoption.limit
    this.$nextTick(() => {
      this.$el.querySelectorAll('.exportTool')[0].submit()
    })
  }
  transDataForGrid (data) {
    var columnMeata = this.extraoption.data.columnMetas
    var lenOfMeta = columnMeata.length
    for (var i = 0; i < lenOfMeta; i++) {
      this.tableMeta.push(columnMeata[i])
    }
    this.pageSizeChange(1)
  }
  toggleDetail () {
    this.showDetail = !this.showDetail
  }
  pageSizeChange (size) {
    this.tableData = this.extraoption.data.results.slice((size - 1) * this.$refs.pager.pageSize, size * this.$refs.pager.pageSize)
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
  openSaveQueryDialog () {
    this.saveQueryFormVisible = true
  }
  closeModal () {
    this.saveQueryFormVisible = false
  }
  mounted () {
    this.queryInfo = this.extraoption.data
    this.transDataForGrid()
  }
  get showExportCondition () {
    return this.$store.state.system.allowAdminExport === 'true' && this.isAdmin || this.$store.state.system.allowNotAdminExport === 'true' && !this.isAdmin
  }
  get modelsTotal () {
    return this.extraoption.data.results.length
  }
  get showHtrace () {
    return this.$store.state.system.showHtrace
  }
  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }
  get dimensionsAndMeasures () {
    var resultDimension = []
    var resultMeasures = []
    for (var s = 0; s < this.tableMeta.length; s++) {
      var meta = this.tableMeta[s]
      if ((this.dateTypes.indexOf(meta.columnType) > -1 || this.datePattern.test(meta.name))) {
        resultDimension.push({
          name: meta.name,
          type: 'date'
        })
        continue
      }
      if (this.stringTypes.indexOf(meta.columnType) > -1) {
        resultDimension.push({
          name: meta.name,
          type: 'string'
        })
        continue
      }
      if (this.numberTypes.indexOf(meta.columnType) > -1) {
        resultMeasures.push({
          name: meta.name,
          type: 'number'
        })
        continue
      }
    }
    this.selectDimension = resultDimension[0] && resultDimension[0].name || ''
    this.selectMetrics = resultMeasures[0] && resultMeasures[0].name || ''
    return {
      d: resultDimension,
      m: resultMeasures
    }
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
  .resultTipsLine{
    font-size: 14px;
    padding: 20px;
    background-color: @table-stripe-color;
    line-height: 1.8;
    .resultTips{
      align-items: center;
      flex-wrap:wrap;
      .resultText{
        padding-left:14px;
        padding-right:14px;
        &:first-child{
          padding-left:0px;
        }
        &:last-child{
           padding-right:0px;
        }
        color:@color-text-primary;
        .label {
          font-weight: bold;
        }
        .text{
          color:@color-text-primary;
        }
        .blue{
          color:#20a0ff;
        }
      }
      .projectText{
        border-right:1px solid #9095ab;
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
        margin-right: 10px;
      }
    }
  }
  .graphBox{
    .el-form-item{
      margin-bottom:10px;
      .el-form-item__label{
        font-size:12px;
        padding-top:8px;
        padding-bottom:8px;
      }
      .el-form-item__content{
        line-height:30px;
        .el-input{
          font-size: 12px;
        }
        .el-input__inner{
          height: 30px;
          line-height: 30px;
        }
      }
    }
  }
</style>
