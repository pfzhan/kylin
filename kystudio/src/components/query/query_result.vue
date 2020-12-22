<template>
  <div class="result_box">
    <div class="ksd-title-label-small ksd-mb-10">{{$t('extraoptionrmation')}}</div>
    <el-alert
      :title="$t('noModelRangeTips')"
      type="warning"
      class="ksd-mb-10"
      v-if="isShowNotModelRangeTips"
      show-icon>
    </el-alert>
    <div class="resultTipsLine">
      <div class="resultTips">
        <p class="resultText">
          <span class="label">{{$t('kylinLang.query.query_id')}}: </span>
          <span class="text">{{extraoption.queryId}}</span>
          <common-tip :content="$t('linkToSpark')" v-if="extraoption.appMasterURL && insightActions.includes('viewAppMasterURL')">
            <a target="_blank" :href="extraoption.appMasterURL"><i class="el-icon-ksd-go"></i></a>
          </common-tip>
        </p>
        <!-- <p class="resultText">
          <span class="label">{{$t('kylinLang.query.status')}}</span>
          <span>{{$t('kylinLang.common.success')}}</span>
        </p> -->
        <p class="resultText">
          <span class="label">{{$t('kylinLang.query.duration')}}: </span>
          <span class="text">
            <el-popover
              placement="right"
              :width="$lang === 'en' ? 340 : 320"
              popper-class="duration-popover"
              trigger="hover">
              <el-row v-for="(step, index) in querySteps" :key="step.name">
                <el-col :span="14">
                  <span class="step-name" :class="{'font-medium': index === 0, 'sub-step': step.group === 'PREPARATION'}" v-show="step.group !== 'PREPARATION' || (step.group === 'PREPARATION' && isShowDetail)">{{$t(step.name)}}</span>
                  <i class="el-icon-ksd-more_01" :class="{'up': isShowDetail}" v-if="index === 1" @click.stop="isShowDetail = !isShowDetail"></i>
                </el-col>
                <el-col :span="4">
                  <span class="step-duration ksd-fright" v-show="step.group !== 'PREPARATION'" :class="{'font-medium': index === 0}">{{step.duration / 1000 | fixed(2)}}s</span>
                </el-col>
                <el-col :span="6">
                  <el-progress v-show="step.group !== 'PREPARATION' && index !== 0" :stroke-width="6" :percentage="step.duration/querySteps[0].duration*100" color="#A6D6F6" :show-text="false"></el-progress>
                </el-col>
              </el-row>
              <span slot="reference" class="duration">{{(extraoption.duration/1000)|fixed(2)||0.00}}s</span>
            </el-popover>
          </span>
        </p>
        <p class="resultText query-obj" :class="{'guide-queryAnswerBy': isWorkspace}">
          <span class="label">{{$t('kylinLang.query.answered_by')}}: </span>
          <span class="text" :title="answeredBy">{{answeredBy}}</span>
        </p>
        <p class="resultText query-obj" v-if="layoutIds">
          <span class="label">{{$t('kylinLang.query.index_id')}}: </span>
          <span class="text" :title="layoutIds">{{layoutIds}}</span>
        </p>
        <p class="resultText query-obj" v-if="snapshots">
          <span class="label">{{$t('kylinLang.query.snapshot')}}: </span>
          <span class="text" :title="snapshots">{{snapshots}}</span>
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
          <span class="text">{{extraoption.totalScanRows | filterNumbers}}</span>
        </p>
        <p class="resultText" v-if="!extraoption.pushDown">
          <span class="label">{{$t('kylinLang.query.result_row_count')}}: </span>
          <span class="text">{{extraoption.resultRowCount | filterNumbers}}</span>
        </p>
      </div>
    </div>
    <el-button-group class="result-layout-btns ksd-mt-15">
      <el-button :class="{active: item.value === activeResultType}" size="mini" plain v-for="(item, index) in insightBtnGroups" :key="index" @click="changeDataType(item)">{{item.text}}</el-button>
    </el-button-group>
    <template v-if="activeResultType === 'data'">
      <div :class="[{'ksd-header': !showExportCondition}]" v-if="!isStop">
        <div :class="['ksd-title-label-small', 'result-title', showExportCondition ? 'ksd-mt-20' : 'result-title-float', {'guide-queryResultBox': isWorkspace}]">{{$t('queryResults')}}</div>
        <div :class="['clearfix', {'ksd-mt-15': showExportCondition}]">
          <div class="ksd-fleft">
            <el-button v-if="showExportCondition" :loading="hasClickExportBtn" type="primary" plain size="small" @click.native="exportData">
              {{$t('exportCSV')}}
            </el-button>
          </div>
          <div class="resultOperator ksd-fright">
            <el-input :placeholder="$t('kylinLang.common.pleaseFilter')" v-model="resultFilter" class="show-search-btn ksd-inline" size="small" prefix-icon="el-icon-search">
            </el-input>
          </div>
        </div>
      </div>
      <div class="ksd-mt-10 grid-box narrowTable" v-if="!isStop">
        <el-table
          :data="pagerTableData"
          border
          v-scroll-shadow
          ref="tableLayout"
          style="width: 100%;">
          <el-table-column v-for="(value, index) in tableMeta" :key="index"
            :prop="''+index"
            :min-width="52+15*(value.label&&value.label.length || 0)"
            show-overflow-tooltip
            :label="value.label">
            <template slot-scope="props">
              <span class="table-cell-text">{{props.row[index]}}</span>
            </template>
          </el-table-column>
        </el-table>

        <kap-pager v-on:handleCurrentChange='pageSizeChange' :curPage="currentPage+1" class="ksd-center ksd-mtb-10" ref="pager" :refTag="pageRefTags.queryResultPager" :totalSize="modelsTotal"></kap-pager>
      </div>
      <form name="export" class="exportTool" action="/kylin/api/query/format/csv" method="post">
        <input type="hidden" name="sql" v-model="sql"/>
        <input type="hidden" name="project" v-model="project"/>
        <input type="hidden" name="limit" v-model="limit" v-if="limit"/>
      </form>
    </template>
    <!-- 可视化 tab -->
    <template v-if="activeResultType === 'visualization'">
      <div class="chart-headers">
        <el-row class="ksd-mt-10" :gutter="5">
          <el-col :span="4" class="title">{{$t('chartType')}}</el-col>
          <el-col :span="10" class="title">{{$t('chartDimension')}}</el-col>
          <el-col :span="10" class="title">{{$t('chartMeasure')}}</el-col>
        </el-row>
        <el-row :gutter="5">
          <el-col :span="4" class="content">
            <el-select v-model="charts.type" @change="changeChartType">
              <el-option v-for="item in chartTypeOptions" :label="item.text" :key="item.value" :value="item.value"></el-option>
            </el-select>
          </el-col>
          <el-col :span="10" class="content">
            <el-select v-model="charts.dimension" @change="changeChartDimension">
              <el-option v-for="item in chartDimensionList.map(it => ({text: it, value: it}))" :label="item.text" :value="item.value" :key="item.value"></el-option>
            </el-select>
          </el-col>
          <el-col :span="10" class="content">
            <el-select v-model="charts.measure" @change="changeChartMeasure">
              <el-option v-for="item in chartMeasureList.map(v => ({text: v, value: v}))" :label="item.text" :value="item.value" :key="item.value"></el-option>
            </el-select>
          </el-col>
        </el-row>
      </div>
      <div class="chart-contains ksd-mt-10">
        <el-tooltip :content="$t('overSizeTips')" effect="dark" placement="top" v-if="displayOverSize">
          <i class="el-icon-ksd-info ksd-fs-15 tips"></i>
        </el-tooltip>
        <div id="charts" v-if="charts.dimension && charts.measure"></div>
        <p class="no-fill-data" v-else>{{$t('noDimensionOrMeasureData')}}</p>
      </div>
    </template>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { scToFloat, showNull } from '../../util/index'
import { hasRole, transToGmtTime } from '../../util/business'
import { pageRefTags } from 'config'
import { getOptions, compareDataSize } from './handler'
import moment from 'moment'
import echarts from 'echarts'
@Component({
  props: ['extraoption', 'isWorkspace', 'queryExportData', 'isStop'],
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      query: 'QUERY_BUILD_TABLES',
      postToExportCSV: 'EXPORT_CSV'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'insightActions'
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
      lineChart: 'Line chart',
      barChart: 'Bar chart',
      pieChart: 'Pie chart',
      traceUrl: 'Trace Url:',
      extraoptionrmation: 'Query Information',
      queryResults: 'Query Results',
      exportCSV: 'Export to CSV',
      linkToSpark: 'Jump to Spark Web UI',
      noModelRangeTips: 'The query is out of the data range for serving queries. Please add segment accordingly.',
      dataBtn: 'Data',
      visualizationBtn: 'Visualization',
      chartType: 'Chart Type',
      chartDimension: 'Dimensions',
      chartMeasure: 'Measures',
      noDimensionOrMeasureData: 'Visualization unavailable for current dataset.',
      overSizeTips: 'The dataset exceeds the maximum limit of the chart. A sampling of 1,000 rows is occurred.',
      overTenYearTips: 'Your data is too large, data sampling with 36,500 rows has been occurred.',
      totalDuration: 'Total Duration',
      PREPARATION: 'Preparation',
      SQL_TRANSFORMATION: 'SQL transformation',
      SQL_PARSE_AND_OPTIMIZE: 'SQL parser optimization',
      MODEL_MATCHING: 'Model matching',
      PREPARE_AND_SUBMIT_JOB: 'Creating and Submitting Spark job',
      WAIT_FOR_EXECUTION: 'Waiting for resources',
      EXECUTION: 'Executing',
      FETCH_RESULT: 'Receiving result',
      SQL_PUSHDOWN_TRANSFORMATION: 'SQL pushdown transformation',
      CONSTANT_QUERY: 'Constant query',
      HIT_CACHE: 'Cache hit'
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
      pieChart: '饼图',
      traceUrl: '追踪链接：',
      extraoptionrmation: '查询信息',
      queryResults: '查询结果',
      exportCSV: '导出 CSV',
      linkToSpark: '跳转至 Spark 任务详情',
      noModelRangeTips: '当前查询不在模型服务的数据范围内。请添加相应 Segment。',
      dataBtn: '数据',
      visualizationBtn: '可视化',
      chartType: '图表类型',
      chartDimension: '维度',
      chartMeasure: '度量',
      noDimensionOrMeasureData: '当前数据不支持可视化。',
      overSizeTips: '数据过大，已执行 1,000  条的数据抽样。',
      overTenYearTips: '您的数据过大，我们已执行 36,500  条的数据抽样。',
      totalDuration: '总耗时',
      PREPARATION: '查询准备',
      SQL_TRANSFORMATION: 'SQL 转换',
      SQL_PARSE_AND_OPTIMIZE: 'SQL 解析与优化',
      MODEL_MATCHING: '模型匹配',
      PREPARE_AND_SUBMIT_JOB: '创建并提交 Spark 任务',
      WAIT_FOR_EXECUTION: '等待资源',
      EXECUTION: '执行',
      FETCH_RESULT: '返回结果',
      SQL_PUSHDOWN_TRANSFORMATION: '下压 SQL 转换',
      CONSTANT_QUERY: '常数查询',
      HIT_CACHE: '击中缓存'
    }
  },
  filters: {
    filterNumbers (num) {
      if (num >= 0) return num
    }
  }
})
export default class queryResult extends Vue {
  pageRefTags = pageRefTags
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
  pageSize = +localStorage.getItem(this.pageRefTags.queryResultPager) || 10
  currentPage = 0
  modelsTotal = this.extraoption.results.length
  timer = null
  pageX = 0
  pageSizeX = 30
  hasClickExportBtn = false
  isShowDetail = false
  activeResultType = 'data'
  charts = {
    type: 'lineChart',
    dimension: '',
    measure: ''
  }
  dateTypes = [91, 92, 93]
  stringTypes = [-1, 1, 12]
  numberTypes = [-7, -6, -5, 3, 4, 5, 6, 7, 8]
  chartLayout = null
  // 增加可视化按钮
  get insightBtnGroups () {
    return [
      {text: this.$t('dataBtn'), value: 'data'},
      {text: this.$t('visualizationBtn'), value: 'visualization'}
    ]
  }
  get chartTypeOptions () {
    return [
      {text: this.$t('lineChart'), value: 'lineChart'},
      {text: this.$t('barChart'), value: 'barChart'},
      {text: this.$t('pieChart'), value: 'pieChart'}
    ]
  }
  // 动态获取维度
  get chartDimensionList () {
    const dimensionList = []
    if (this.charts.type === 'lineChart') {
      this.tableMetaBackup.forEach(item => {
        if (this.dateTypes.includes(item.columnType)) {
          dimensionList.push(item.label)
        }
      })
    } else {
      this.tableMetaBackup.forEach(item => {
        // if (this.dateTypes.includes(item.columnType) || this.stringTypes.includes(item.columnType)) {
        //   dimensionList.push(item.label)
        // }
        dimensionList.push(item.label)
      })
    }
    this.charts.dimension = dimensionList[0] || ''
    return dimensionList
  }
  // 动态获取度量
  get chartMeasureList () {
    const measureList = []
    this.tableMetaBackup.forEach(item => {
      if (this.numberTypes.includes(item.columnType)) {
        measureList.push(item.label)
      }
    })
    this.charts.measure = measureList[measureList.length - 1] || ''
    return measureList
  }

  // 切换数据展示效果
  changeDataType (item) {
    if (item.value === this.activeResultType) return
    this.activeResultType = item.value
    if (item.value === 'visualization') {
      this.$nextTick(() => {
        this.initChartOptions()
      })
    }
  }
  // 初始化 echarts 配置
  initChartOptions () {
    this.chartLayout = echarts.init(document.getElementById('charts'))
    this.chartLayout.setOption(getOptions(this))
  }
  changeChartType (v) {
    this.charts.type = v
    this.resetEcharts()
  }
  // 更改维度
  changeChartDimension () {
    this.resetEcharts()
  }
  // 更改度量
  changeChartMeasure () {
    this.resetEcharts()
  }
  resetEcharts () {
    this.chartLayout && this.chartLayout.dispose()
    this.$nextTick(() => {
      this.initChartOptions()
    })
  }
  // 更改 charts 大小
  resetChartsPosition () {
    this.chartLayout && this.chartLayout.resize()
  }
  // 是否显示数量过多 icon 提示
  get displayOverSize () {
    const result = compareDataSize(this)
    if (this.charts.type === 'pieChart') {
      return result.xData.length > 1000
    }
  }
  exportData () {
    // 区别于3x中，导出所需的参数，存在props 传进来的 queryExportData 这个对象中，不再一起放在 extraoption 中
    this.sql = this.queryExportData.sql
    this.project = this.currentSelectedProject
    this.limit = this.queryExportData.limit
    this.$nextTick(() => {
      if (this.$store.state.config.platform === 'iframe') {
        try {
          this.hasClickExportBtn = true
          let params = {
            sql: this.sql,
            project: this.project
          }
          if (this.limit) {
            params.limit = this.limit
          }
          this.postToExportCSV(params).then((res) => {
            let file = res && res.headers && res.headers.map && res.headers.map['content-disposition'] && res.headers.map['content-disposition'][0] || ''
            let fileNameStrArr = file.split(';')
            let fileNameArr = fileNameStrArr[1] ? fileNameStrArr[1].split('=') : []
            if (res && res.status === 200 && res.body) {
              // 动态从header 里取文件名，如果没取到，就前端自己配
              let fileName = fileNameArr[1].trim().replace(/"/g, '') || (moment().format('YYYYMMDDHHmmssSSS') + '.result.csv')
              let data = res.body
              const blob = new Blob([data], {type: 'text/csv;charset=utf-8'})
              if (window.navigator.msSaveOrOpenBlob) {
                navigator.msSaveBlob(blob, fileName)
              } else {
                var link = document.createElement('a')
                link.href = window.URL.createObjectURL(blob)
                link.download = fileName
                link.click()
                window.URL.revokeObjectURL(link.href)
              }
            }
            this.hasClickExportBtn = false
          }, () => {
            this.hasClickExportBtn = false
          })
        } catch (e) {
          this.hasClickExportBtn = false
        }
      } else {
        this.hasClickExportBtn = false
        this.$el.querySelectorAll('.exportTool').length && this.$el.querySelectorAll('.exportTool')[0].submit()
      }
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
      }).join(', ')
    } else {
      return this.extraoption.engineType
    }
  }
  get layoutIds () {
    if (this.extraoption.realizations && this.extraoption.realizations.length) {
      let filterIds = []
      for (let i of this.extraoption.realizations) {
        if (i.layoutId !== -1 && i.layoutId !== null) {
          filterIds.push(i.layoutId)
        }
      }
      return filterIds.join(', ')
    } else {
      return ''
    }
  }
  get snapshots () {
    if (this.extraoption.realizations && this.extraoption.realizations.length) {
      let filterSnapshot = []
      for (let i of this.extraoption.realizations) {
        if (i.snapshots && i.snapshots.length) {
          filterSnapshot = [...filterSnapshot, ...i.snapshots]
        }
      }
      filterSnapshot = [...new Set(filterSnapshot)]
      return filterSnapshot.join(', ')
    } else {
      return ''
    }
  }
  get isShowNotModelRangeTips () {
    let isAnyNull = false
    if (this.extraoption.realizations && this.extraoption.realizations.length) {
      for (let i in this.extraoption.realizations) {
        if (this.extraoption.realizations[i].layoutId === null) {
          isAnyNull = true
          break
        }
      }
    }
    return isAnyNull
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
    this.currentPage = currentPage
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
  getStepData (steps) {
    if (steps.length) {
      let renderSteps = [
        {name: 'totalDuration', duration: 0},
        {name: 'PREPARATION', duration: 0}
      ]
      steps.forEach((s) => {
        renderSteps[0].duration = renderSteps[0].duration + s.duration
        if (s.group === 'PREPARATION') {
          renderSteps[1].duration = renderSteps[1].duration + s.duration
          renderSteps.push(s)
        } else {
          renderSteps.push(s)
        }
      })
      return renderSteps
    } else {
      return []
    }
  }
  get querySteps () {
    return this.getStepData(this.extraoption['traces'])
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
    if (!this.$refs.tableLayout || !this.$refs.tableLayout.bodyWrapper) return
    this.$refs.tableLayout.bodyWrapper.addEventListener('scroll', this.getMoreData)
    window.addEventListener('resize', this.resetChartsPosition)
  }

  beforeDestory () {
    this.$refs.tableLayout.bodyWrapper.removeEventListener('scroll', this.getMoreData)
    window.removeEventListener('resize', this.resetChartsPosition)
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
          .duration {
            color: @base-color;
            cursor: pointer;
          }
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
            white-space: nowrap;
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
    .result-layout-btns {
      .el-button.active {
        color: #5c5c5c;
        background: #f4f4f4;
        border: 1px solid #cccccc;
        box-shadow: inset 1px 1px 2px 0 #ddd;
      }
    }
    .chart-headers {
      .title {
        font-weight: bold;
      }
      .content {
        .el-select {
          width: 100%;
          margin-top: 5px;
        }
      }
    }
    .chart-contains {
      width: 100%;
      height: 400px;
      position: relative;
      #charts {
        width: 100%;
        height: 100%;
      }
      .no-fill-data {
        position: absolute;
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%);
      }
      .tips {
        position: absolute;
        right: 0;
        z-index: 10;
        color: @color-warning;
      }
    }
  }
  .table-cell-text{
    // word-wrap: break-word;
    // word-break: break-all;
    white-space: pre;
    color: @text-normal-color;
    font-family: Lato,"Noto Sans S Chinese",sans-serif;
  }
</style>
