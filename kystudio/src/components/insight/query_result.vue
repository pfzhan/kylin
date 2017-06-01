<template>
  <div>
    <el-row  class="resultTips" >
      <el-col :span="3"><div class="grid-content bg-purple"><p>Status: <span style="color:green"> success</span></p></div></el-col>
      <el-col :span="5"><div class="grid-content bg-purple"><p>Start Time: <span> {{queryInfo.starttime|gmtTime}}</span></p></div></el-col>
      <el-col :span="3"><div class="grid-content bg-purple"><p>Duration: <span> {{(queryInfo.duration/1000)|fixed(2)}}s</span></p></div></el-col>
      <el-col :span="4"><div class="grid-content bg-purple"><p>Project: <span> {{queryInfo.project}}</span></p></div></el-col>
      <el-col :span="5"><div class="grid-content bg-purple"><p>Cube: <span> {{queryInfo.cube}}</span></p></div></el-col>
      <el-col :span="4"><div class="grid-content bg-purple" style="text-align:right" >
      <kap-icon-button   icon="save" type="primary" @click.native="openSaveQueryDialog">Save Query</kap-icon-button>
      </div></el-col>
    </el-row>
  	<div>
<kap-icon-button  v-show="viewModel" icon="area-chart" type="parimary" @click.native="changeViewModel">Visualization</kap-icon-button>
<kap-icon-button v-show="!viewModel" icon="table" type="parimary" @click.native="changeViewModel">Grid</kap-icon-button>
<kap-icon-button icon="external-link" type="parimary" @click.native="exportData">Export</kap-icon-button>
   <!-- <el-button><icon name="external-link"></icon> Export</el-button> -->
   </div>
  	<div class="ksd-mt-20" v-show="viewModel">
  		<el-table
		    :data="pagerTableData"
		    border
		    style="width: 100%">
		    <el-table-column v-for="(value, index) in tableMeta" :key="index"
		      :prop="''+index"
		      :label="value.label">
		    </el-table-column>
		  </el-table>

      <pager v-on:handleCurrentChange='pageSizeChange' class="ksd-center" ref="pager"  :totalSize="modelsTotal"  ></pager>
  	</div>
    <div class="ksd-mt-20" v-show="!viewModel">
       <el-form :inline="true"  class="demo-form-inline">
        <el-form-item label="Graph Type">
          <el-select  placeholder="Graph Type" v-model="graphType">
            <el-option label="Line Chart" value="line"></el-option>
            <el-option label="Bar Chart" value="bar"></el-option>
            <el-option label="Pie Chart" value="pie"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Dimensions">
          <el-select  placeholder="Dimensions" v-model="selectDimension">
            <el-option :label="dime.name" :value="dime.name" v-for="dime in dimensionsAndMeasures.d" :key="dime.name"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Metrics">
          <el-select  placeholder="Metrics" v-model="selectMetrics">
            <el-option :label="mea.name" :value="mea.name" v-for="(mea, index) in dimensionsAndMeasures.m" :key="index"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item>
          <!-- <el-button type="primary" @click="onSubmit">查询</el-button> -->
        </el-form-item>
      </el-form>
      <div class="echart_box" style="width:800px;height:800px;"></div>
    </div>
   <el-dialog title="保存" v-model="saveQueryFormVisible">
    <el-form :model="saveQueryMeta"  ref="saveQueryForm" label-width="100px">
      <el-form-item label="Query SQL" prop="sql">
       <editor v-model="saveQueryMeta.sql" lang="sql" theme="chrome" width="100%" height="200" useWrapMode="true"></editor>
      </el-form-item>
      <el-form-item label="Name:" prop="name">
        <el-input v-model="saveQueryMeta.name" auto-complete="off"></el-input>
      </el-form-item>
      <el-form-item label="Description:" prop="description">
        <el-input v-model="saveQueryMeta.description"></el-input>
      </el-form-item>
    </el-form>
     <div slot="footer" class="dialog-footer">
    <el-button @click="saveQueryFormVisible = false">取 消</el-button>
    <el-button type="primary" @click="saveQuery">确 定</el-button>
  </div>
    </el-dialog>
  </div>
</template>
<script>
import echarts from 'echarts'
import $ from 'jquery'
import { mapActions } from 'vuex'
import pager from '../common/pager'
import { indexOfObjWithSomeKey, scToFloat, showNull } from '../../util/index'
import { handleError } from '../../util/business'
export default {
  name: 'queryResult',
  props: ['extraoption'],
  data () {
    return {
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
      queryInfo: {
        duration: '-',
        project: this.extraoption.project,
        cube: '-',
        starttime: ''
      },
      saveQueryMeta: {
        name: '',
        description: '',
        project: this.extraoption.project,
        sql: this.extraoption.sql
      }

    }
  },
  components: {
    pager
  },
  methods: {
    ...mapActions({
      query: 'QUERY_BUILD_TABLES',
      saveQueryToServer: 'SAVE_QUERY'
    }),
    exportData () {
      location.href = '/kylin/api/query/format/csv?sql=select%20*%20from%20EDW.TEST_SITES&project=default'
    },
    changeViewModel () {
      this.viewModel = !this.viewModel
    },
    transDataForGrid (data) {
      var columnMeata = data.columnMetas
      for (var i = 0; i < columnMeata.length; i++) {
        this.tableMeta.push(columnMeata[i])
      }
      this.tableData = data.results
      for (let i = 0; i < this.tableData.length; i++) {
        for (var m = 0; m < this.tableData[i].length; m++) {
          var cur = this.tableData[i][m]
          var trans = scToFloat(cur)
          if (trans) {
            this.tableData[i][m] = showNull(trans)
          }
        }
      }
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
              dataView: {show: true, readOnly: false},
              magicType: {show: true, type: ['line', 'bar']},
              restore: {show: true},
              saveAsImage: {show: true}
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
              }
            }
          ],
          yAxis: [
            {
              type: 'category',
              axisLine: {onZero: false},
              axisLabel: {
                formatter: '{value} '
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
              dataView: {show: true, readOnly: false},
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
                }
              },
              restore: {show: true},
              saveAsImage: {show: true}
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
      this.pagerTableData = Object.assign([], this.tableData.slice((size - 1) * this.$refs.pager.pageSize, size * this.$refs.pager.pageSize))
    },
    openSaveQueryDialog () {
      this.saveQueryFormVisible = true
      this.saveQueryMeta.name = ''
      this.saveQueryMeta.description = ''
    },
    saveQuery () {
      this.saveQueryToServer(this.saveQueryMeta).then((response) => {
        this.$message('query 保存成功！')
        this.saveQueryFormVisible = false
        this.$emit('reloadSavedProject', 0)
      }, (res) => {
        handleError(res)
        this.saveQueryFormVisible = false
      })
    }
  },
  mounted () {
    // var _this = this
    this.queryInfo.starttime = Date.now()
    // this.query(this.extraoption.sql).then((response) => {
    //   var queryResult = response.data
    //   _this.queryInfo.duration = queryResult.duration
    //   _this.queryInfo.cube = queryResult.cube
    //   _this.transDataForGrid(queryResult)
    // })
    this.queryInfo.duration = this.extraoption.data.duration
    this.queryInfo.cube = this.extraoption.data.cube
    this.transDataForGrid(this.extraoption.data)
  },
  watch: {
    'selectDimension': function () {
      var renderData = this.transDataForGraph(this.selectDimension, this.selectMetrics)
      this.renderGraph(this.selectDimension, this.selectMetrics, renderData, this.graphType)
    },
    'selectMetrics': function () {
      var renderData = this.transDataForGraph(this.selectDimension, this.selectMetrics)
      this.renderGraph(this.selectDimension, this.selectMetrics, renderData, this.graphType)
    },
    'graphType': function () {
      var renderData = this.transDataForGraph(this.selectDimension, this.selectMetrics)
      this.renderGraph(this.selectDimension, this.selectMetrics, renderData, this.graphType)
    }
  },
  computed: {
    modelsTotal () {
      return this.tableData.length
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
    'en': {username: 'Username', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin'},
    'zh-cn': {username: '用户名', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '管理人员'}
  }
}
</script>
<style  lang="less">
  .resultTips{
     font-size: 12px;
     border-bottom:solid 1px #ccc;
     margin-bottom: 10px;
     p{
       line-height: 45px;
     }
  }
</style>
