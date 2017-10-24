<template>
  <div class="result_box">
    <div class="resultTipsLine">
      <div class="resultTips">
        <div class="resultText"><p>{{$t('kylinLang.query.status')}}<span style="color:green" class="text"> success</span></p></div>
        <div class="resultText"><p>{{$t('kylinLang.query.startTime')}}<span class="text"> {{queryInfo.starttime|timeFormatHasTimeZone}}</span></p></div>
        <div class="resultText"><p>{{$t('kylinLang.query.duration')}}<span class="text"> {{(queryInfo.duration/1000)|fixed(2)||0.00}}s</span></p></div>
        <div class="resultText projectText"><p>{{$t('kylinLang.query.project')}}<span class="blue"> {{queryInfo.project}}</span></p></div>
        <div v-if="!extraoption.data.pushDown" class="resultText"><p>{{$t('kylinLang.query.queryEngine')}}<span class="blue">{{queryInfo.cube.replace(/\[name=/g, ' [')}}</span></p></div>
        <div v-if="extraoption.data.pushDown" class="resultText"><p>{{$t('kylinLang.query.queryEngine')}}<span class="blue">Push down</span></p></div>
      </div>
      <div class="resultOperator">
        <div class="grid-content bg-purple" style="text-align:right" >
          <kap-icon-button icon="refresh" size="small" @click.native="refreshQuery" style="display:inline-block"></kap-icon-button>
          <kap-icon-button icon="save" size="small" @click.native="openSaveQueryDialog" style="display:inline-block">{{$t('kylinLang.query.saveQuery')}}</kap-icon-button>
        </div>
      </div>
    </div>

  	<div class="ksd-mt-14">
      <kap-icon-button v-show="viewModel" icon="area-chart" size="small" style="border-width:1px" @click.native="changeViewModel">{{$t('kylinLang.query.visualization')}}</kap-icon-button>
      <kap-icon-button v-show="!viewModel" icon="table" size="small" @click.native="changeViewModel">{{$t('kylinLang.query.grid')}}</kap-icon-button>
      <kap-icon-button icon="external-link" size="small" @click.native="exportData">{{$t('kylinLang.query.export')}}</kap-icon-button>
      <!-- <el-button><icon name="external-link"></icon> Export</el-button> -->
    </div>
  	<div class="ksd-mt-20 grid-box narrowTable" v-show="viewModel">
  		<el-table
		    :data="pagerTableData"
		    border
		    style="width: 100%;">
		    <el-table-column v-for="(value, index) in tableMeta" :key="index"
		      :prop="''+index"
          :width="52+15*(value.label&&value.label.length || 0)"
		      :label="value.label"
          sortable
          >
		    </el-table-column>
		  </el-table>

      <pager v-on:handleCurrentChange='pageSizeChange' class="ksd-center" ref="pager"  :totalSize="modelsTotal"  ></pager>
  	</div>
    <div class="ksd-mt-10 graphBox" v-show="!viewModel">
       <el-form :inline="true"  class="demo-form-inline">
        <el-form-item :label="$t('kylinLang.query.graphType')">
          <el-select  placeholder="Graph Type" v-model="graphType" @change="changeGraphInfo">
            <el-option label="Line Chart" value="line"></el-option>
            <el-option label="Bar Chart" value="bar"></el-option>
            <el-option label="Pie Chart" value="pie"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.query.dimension')">
          <el-select  placeholder="Dimensions" v-model="selectDimension" @change="changeGraphInfo">
            <el-option :label="dime.name" :value="dime.name" v-for="dime in dimensionsAndMeasures.d" :key="dime.name"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.query.metrics')">
          <el-select  placeholder="Metrics" v-model="selectMetrics" @change="changeGraphInfo">
            <el-option :label="mea.name" :value="mea.name" v-for="(mea, index) in dimensionsAndMeasures.m" :key="index"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item>
          <!-- <el-button type="primary" @click="onSubmit">查询</el-button> -->
        </el-form-item>
      </el-form>
      <div class="echart_box" style="width:800px;height:800px;"></div>
    </div>
   <el-dialog :title="$t('kylinLang.common.save')" v-model="saveQueryFormVisible">
    <el-form :model="saveQueryMeta"  ref="saveQueryForm" :rules="rules" label-width="100px">
      <el-form-item :label="$t('kylinLang.query.querySql')" prop="sql">
       <editor v-model="saveQueryMeta.sql" lang="sql" theme="chrome" width="100%" height="200" useWrapMode="true"></editor>
      </el-form-item>
      <el-form-item :label="$t('kylinLang.query.name')" prop="name">
        <el-input v-model="saveQueryMeta.name" auto-complete="off"></el-input>
      </el-form-item>
      <el-form-item :label="$t('kylinLang.query.desc')" prop="description">
        <el-input v-model="saveQueryMeta.description"></el-input>
      </el-form-item>
    </el-form>
     <div slot="footer" class="dialog-footer">
    <el-button @click="saveQueryFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
    <el-button type="primary" @click="saveQuery">{{$t('kylinLang.common.ok')}}</el-button>
  </div>
    </el-dialog>
    <form name="export" class="exportTool" action="/kylin/api/query/format/csv" method="post">
      <input type="hidden" name="sql" v-model="sql"/>
      <input type="hidden" name="project" v-model="project"/>
      <input type="hidden" name="limit" v-model="limit" v-if="limit"/>
    </form>
  </div>
</template>
<script>
import echarts from 'echarts'
import $ from 'jquery'
import { mapActions } from 'vuex'
import { indexOfObjWithSomeKey, scToFloat, showNull } from '../../util/index'
import { handleError } from '../../util/business'
export default {
  name: 'queryResult',
  props: ['extraoption'],
  data () {
    return {
      rules: {
        name: [
          { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'blur' }
        ]
      },
      saveQueryFormVisible: false,
      // pageSize: 10,
      // currentPage: 1,
      viewModel: true,
      tableData: [],
      tableMeta: [],
      pagerTableData: [],
      dateTypes: [91, 92, 93],
      stringTypes: [-1, 1, 12],
      numberTypes: [-7, -6, -5, 3, 4, 5, 6, 7, 8],
      datePattern: /_date|_dt/i,
      selectDimension: '',
      selectMetrics: '',
      graphType: 'line',
      sql: '',
      project: '',
      limit: '',
      queryInfo: {
        duration: '-',
        project: this.extraoption.project,
        cube: '-',
        starttime: Date.now()
      },
      saveQueryMeta: {
        name: '',
        description: '',
        project: this.extraoption.project,
        sql: this.extraoption.sql
      }
    }
  },
  methods: {
    ...mapActions({
      query: 'QUERY_BUILD_TABLES',
      saveQueryToServer: 'SAVE_QUERY'
    }),
    refreshQuery () {
      this.$emit('changeView', this.extraoption.index, this.extraoption, 'circle-o-notch', 'querypanel')
     // this.addTab('query', 'querypanel', queryObj)
    },
    exportData () {
      this.sql = this.extraoption.sql
      this.project = this.extraoption.project
      this.limit = this.extraoption.limit
      this.$nextTick(() => {
        this.$el.querySelectorAll('.exportTool')[0].submit()
      })
    },
    changeViewModel () {
      this.viewModel = !this.viewModel
    },
    transDataForGrid (data) {
      var columnMeata = this.extraoption.data.columnMetas
      var lenOfMeta = columnMeata.length
      for (var i = 0; i < lenOfMeta; i++) {
        this.tableMeta.push(columnMeata[i])
      }
      // this.tableData = data.results
      // var len = this.tableData.length
      // for (let i = 0; i < len; i++) {
      //   var innerLen = this.tableData[i].length
      //   for (var m = 0; m < innerLen; m++) {
      //     var cur = this.tableData[i][m]
      //     var trans = scToFloat(cur)
      //     if (trans) {
      //       this.tableData[i][m] = showNull(trans)
      //     }
      //   }
      // }
      this.pageSizeChange(1)
    },
    transDataForGraph (dimension, measure) {
      var dIndex = indexOfObjWithSomeKey(this.tableMeta, 'name', dimension)
      var mIndex = indexOfObjWithSomeKey(this.tableMeta, 'name', measure)
      var resultObj = {}
      for (var i = 0; i < this.tableData.length; i++) {
        resultObj[this.tableData[i][dIndex]] = parseFloat(resultObj[this.tableData[i][dIndex]] || 0) + parseFloat(this.tableData[i][mIndex])
      }
      return resultObj
    },
    renderGraph (dimension, measure, transData, graphType) {
      var xAxis = []
      var yAxis = []
      var myChart = echarts.init($(this.$el).find('.echart_box')[0])
      for (var i in transData) {
        xAxis.push(i)
        yAxis.push(transData[i])
      }
      if (graphType === 'bar') {
        myChart.setOption({
          title: { text: '' },
          tooltip: {},
          xAxis: {
            data: xAxis
          },
          yAxis: {},
          series: [{
            name: '',
            type: 'bar',
            data: yAxis
          }]
        })
      } else if (graphType === 'line') {
        myChart.setOption({
          legend: {
            data: ['']
          },
          toolbox: {
            show: true,
            feature: {
              mark: {show: true},
              dataView: {show: false, readOnly: false},
              magicType: {
                show: true,
                type: ['line', 'bar'],
                title: {
                  line: this.$t('lineChart'),
                  bar: this.$t('barChart')
                }
              },
              restore: {
                show: true,
                title: this.$t('restore')},
              saveAsImage: {
                backgroundColor: '#292b38',
                show: true,
                title: 'Picture',
                lang: this.$t('save')
              }
            }
          },
          calculable: true,
          tooltip: {
            trigger: 'axis',
            formatter: ''
          },
          xAxis: [
            {
              type: 'value',
              axisLabel: {
                // formatter: '{value} °C'
                textStyle: {
                  color: '#fff'
                }
              }
            }
          ],
          yAxis: [
            {
              type: 'category',
              axisLine: {onZero: false},
              axisLabel: {
                formatter: '{value} ',
                textStyle: {
                  color: '#fff'
                }
              },
              boundaryGap: false,
              data: xAxis
            }
          ],
          series: [
            {
              name: '',
              type: 'line',
              smooth: true,
              itemStyle: {
                normal: {
                  lineStyle: {
                    shadowColor: 'rgba(0,0,0,0.4)'
                  }
                }
              },
              data: yAxis
            }
          ]
        })
      } else if (graphType === 'pie') {
        var yAxisForPie = []
        for (var ya in yAxis) {
          yAxisForPie.push({
            name: ya,
            value: yAxis[ya]
          })
        }
        myChart.setOption({
          title: {
            text: '',
            subtext: '',
            x: 'center'
          },
          tooltip: {
            trigger: 'item',
            formatter: '{a} <br/>{b} : {c} ({d}%)'
          },
          legend: {
            orient: 'vertical',
            x: 'left',
            data: xAxis
          },
          toolbox: {
            show: true,
            feature: {
              mark: {show: true},
              dataView: {show: false, readOnly: false},
              magicType: {
                show: true,
                type: ['pie', 'funnel'],
                option: {
                  funnel: {
                    x: '25%',
                    width: '50%',
                    funnelAlign: 'left',
                    max: 1548
                  }
                },
                title: {
                  line: this.$t('lineChart'),
                  bar: this.$t('barChart')
                }
              },
              restore: {
                show: true,
                title: this.$t('restore')
              },
              saveAsImage: {
                backgroundColor: '#292b38',
                show: true,
                title: 'Picture',
                lang: this.$t('save')
              }
            }
          },
          calculable: true,
          series: [
            {
              name: '',
              type: 'pie',
              radius: '55%',
              center: ['50%', '60%'],
              data: yAxisForPie
            }
          ]
        })
      }
    },
    pageSizeChange (size) {
      // this.$refs.pager.currentPage = size
      this.tableData = this.extraoption.data.results.slice((size - 1) * this.$refs.pager.pageSize, size * this.$refs.pager.pageSize)
      var len = this.tableData.length
      for (let i = 0; i < len; i++) {
        var innerLen = this.tableData[i].length
        for (var m = 0; m < innerLen; m++) {
          var cur = this.tableData[i][m]
          var trans = scToFloat(cur)
          if (trans) {
            this.tableData[i][m] = showNull(trans)
          }
        }
      }
      this.pagerTableData = Object.assign([], this.tableData)
    },
    openSaveQueryDialog () {
      this.saveQueryFormVisible = true
      this.saveQueryMeta.name = ''
      this.saveQueryMeta.description = ''
    },
    saveQuery () {
      this.$refs['saveQueryForm'].validate((valid) => {
        if (valid) {
          this.saveQueryToServer(this.saveQueryMeta).then((response) => {
            this.$message(this.$t('kylinLang.common.saveSuccess'))
            this.saveQueryFormVisible = false
            this.$emit('reloadSavedProject', 0)
          }, (res) => {
            handleError(res)
            this.saveQueryFormVisible = false
          })
        }
      })
    },
    changeGraphInfo () {
      var renderData = this.transDataForGraph(this.selectDimension, this.selectMetrics)
      this.renderGraph(this.selectDimension, this.selectMetrics, renderData, this.graphType)
    }
  },
  mounted () {
    // var _this = this
    // this.queryInfo.starttime = Date.now()
    // this.query(this.extraoption.sql).then((response) => {
    //   var queryResult = response.data
    //   _this.queryInfo.duration = queryResult.duration
    //   _this.queryInfo.cube = queryResult.cube
    //   _this.transDataForGrid(queryResult)
    // })
    this.queryInfo.duration = this.extraoption.data.duration
    this.queryInfo.cube = this.extraoption.data.cube
    this.transDataForGrid()
  },
  // watch: {
  //   'selectDimension': function () {
  //     var renderData = this.transDataForGraph(this.selectDimension, this.selectMetrics)
  //     this.renderGraph(this.selectDimension, this.selectMetrics, renderData, this.graphType)
  //   },
  //   'selectMetrics': function () {
  //     var renderData = this.transDataForGraph(this.selectDimension, this.selectMetrics)
  //     this.renderGraph(this.selectDimension, this.selectMetrics, renderData, this.graphType)
  //   },
  //   'graphType': function () {
  //     var renderData = this.transDataForGraph(this.selectDimension, this.selectMetrics)
  //     this.renderGraph(this.selectDimension, this.selectMetrics, renderData, this.graphType)
  //   }
  // },
  computed: {
    modelsTotal () {
      return this.extraoption.data.results.length
    },
    dimensionsAndMeasures () {
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
  },
  locales: {
    'en': {username: 'Username', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin', save: 'Save', restore: 'Restore', lineChart: 'Line Chart', barChart: 'Bar Chart', pieChart: 'Pie Chart'},
    'zh-cn': {username: '用户名', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '管理人员', save: '保存', restore: '还原', lineChart: '折线图', barChart: '柱状图', pieChart: '饼状图'}
  }
}
</script>
<style  lang="less">
  .narrowTable{
    .el-table td, .el-table th{
      height: 30px;
    }
  }
  .resultTipsLine{
    display: flex;
    .resultTips{
      flex:1;
      display: flex;
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
        color:#9095ab;
        .text{
          color:#d0d2db;
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
  .resultTips{
     font-size: 12px;
     p{
       line-height: 15px;
     }
  }
  .result_box{
    .el-table .cell{
       word-break: break-all!important;
       // white-space: normal!important;
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
  // .grid-box{
  //   border-width: 1px!important;
  // }
</style>
