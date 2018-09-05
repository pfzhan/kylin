<template>
  <div class="result_box">
    <div class="resultTipsLine">
      <div class="resultTips">
        <div class="resultText">
          <p>
            <i class="el-icon-ksd-status"></i>
            <span>{{$t('kylinLang.query.status')}}</span>
            <span class="text">success</span>
          </p>
        </div>
        <div class="resultText">
          <p>
            <i class="el-icon-ksd-calendar"></i>
            <span>{{$t('kylinLang.query.startTime')}}</span>
            <span class="text">{{transToGmtTime(queryInfo.starttime)}}</span>
          </p>
        </div>
        <div class="resultText">
          <p>
            <i class="el-icon-ksd-elapsed-time"></i>
            <span>{{$t('kylinLang.query.duration')}}</span>
            <span class="text">{{(queryInfo.duration/1000)|fixed(2)||0.00}}s</span>
          </p>
        </div>
        <div class="resultText">
          <p>
            <i class="el-icon-ksd-project"></i>
            <span>{{$t('kylinLang.query.project')}} {{queryInfo.project}}</span>
          </p>
        </div>
        <div v-if="!extraoption.data.pushDown" class="resultText">
          <p>
            <i class="el-icon-ksd-search-engine"></i>
          {{$t('kylinLang.query.queryEngine')}} {{queryInfo.cube.replace(/\[name=/g, ' [')}}</p></div>
        <div v-if="extraoption.data.pushDown" class="resultText">
          <p><i class="el-icon-ksd-search-engine"></i>{{$t('kylinLang.query.queryEngine')}}<span class="blue">Push down</span></p></div>
        <div v-show="showHtrace && queryInfo.traceUrl" class="resultText">
          <i class="el-icon-ksd-search-engine"></i>{{$t('traceUrl')}}<a :href="queryInfo.traceUrl" target="_blank">{{queryInfo.traceUrl}}</a></div>
      </div>
    </div>
    <div class="clearfix">
      <div class="resultOperator ksd-mt-14 ksd-fright ksd-inline">
        <div class="grid-content bg-purple" style="text-align:right" >
          <kap-icon-button size="small" type="primary" plain="plain" @click.native="openSaveQueryDialog" style="display:inline-block">{{$t('kylinLang.query.saveQuery')}}</kap-icon-button>
          <kap-icon-button size="small" @click.native="refreshQuery" style="display:inline-block">{{$t('kylinLang.query.refreshQuery')}}</kap-icon-button>
        </div>
      </div>

	<div class="ksd-mt-14 ksd-fleft ksd-inline">
        <kap-icon-button icon="el-icon-ksd-export" v-if="showExportCondition" size="small" @click.native="exportData">{{$t('kylinLang.query.export')}}</kap-icon-button>
      </div>
    </div>
  	<div class="ksd-mt-20 grid-box narrowTable" v-show="viewModel">
  		<el-table
		    :data="pagerTableData"
		    border
		    style="width: 100%;">
		    <el-table-column  v-for="(value, index) in tableMeta" :key="value.label"
		      :prop="''+index"
          :min-width="52+15*(value.label&&value.label.length || 0)"
		      :label="value.label"
          sortable
          :sort-method="(a, b) => sortResultList(a, b, index, value.columnTypeName)"
          >
		    </el-table-column>
		  </el-table>

      <pager v-on:handleCurrentChange='pageSizeChange' class="ksd-center" ref="pager"  :totalSize="modelsTotal"  ></pager>
  	</div>
    <save_query_dialog :show="saveQueryFormVisible" :extraoption='extraoption' v-on:closeModal="closeModal" v-on:reloadSavedProject="reloadSavedProject"></save_query_dialog>
    <form name="export" class="exportTool" action="/kylin/api/query/format/csv" method="post">
      <input type="hidden" name="sql" v-model="sql"/>
      <input type="hidden" name="project" v-model="project"/>
      <input type="hidden" name="limit" v-model="limit" v-if="limit"/>
    </form>
  </div>
</template>
<script>
import { mapActions } from 'vuex'
import { scToFloat, showNull } from '../../util/index'
import { hasRole, transToGmtTime } from '../../util/business'
import { IntegerTypeForQueryResult } from 'config/index'
import saveQueryDialog from 'components/insight/save_query_dialog'
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
      isInterger: this.checkIntgerType(IntegerTypeForQueryResult),
      limit: '',
      queryInfo: {
        duration: '-',
        project: this.extraoption.project,
        cube: '-',
        starttime: Date.now()
      }
    }
  },
  components: {
    'save_query_dialog': saveQueryDialog
  },
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      query: 'QUERY_BUILD_TABLES',
      saveQueryToServer: 'SAVE_QUERY'
    }),
    sortResultList (a, b, prop, type) {
      // 数值类型的排序判断
      if (this.isInterger(type.toUpperCase())) {
        return a[prop] - b[prop]
      } else {
        return a[prop] > b[prop] ? 1 : -1
      }
    },
    checkIntgerType (intergerConfig) {
      var intergerList = intergerConfig
      var checkoutConfig = {}
      return (columnType) => {
        if (checkoutConfig.hasOwnProperty(columnType)) {
          return checkoutConfig[columnType]
        } else {
          var isInterger = intergerList.indexOf(columnType.toUpperCase()) >= 0
          return (checkoutConfig[columnType] = isInterger)
        }
      }
    },
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
    reloadSavedProject () {
      this.$emit('reloadSavedProject', 0)
    },
    transDataForGrid (data) {
      var columnMeata = this.extraoption.data.columnMetas
      var lenOfMeta = columnMeata.length
      for (var i = 0; i < lenOfMeta; i++) {
        this.tableMeta.push(columnMeata[i])
      }
      this.pageSizeChange(1)
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
          this.tableData[i][m] = showNull(trans)
        }
      }
      this.pagerTableData = Object.assign([], this.tableData)
    },
    openSaveQueryDialog () {
      this.saveQueryFormVisible = true
    },
    closeModal () {
      this.saveQueryFormVisible = false
    }
  },
  mounted () {
    this.queryInfo.duration = this.extraoption.data.duration
    this.queryInfo.cube = this.extraoption.data.cube
    this.queryInfo.traceUrl = this.extraoption.data.traceUrl
    this.transDataForGrid()
  },
  computed: {
    showExportCondition () {
      return this.$store.state.system.allowAdminExport === 'true' && this.isAdmin || this.$store.state.system.allowNotAdminExport === 'true' && !this.isAdmin
    },
    modelsTotal () {
      return this.extraoption.data.results.length
    },
    showHtrace () {
      return this.$store.state.system.showHtrace
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
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
    'en': {username: 'Username', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin', save: 'Save', restore: 'Restore', lineChart: 'Line Chart', barChart: 'Bar Chart', pieChart: 'Pie Chart', traceUrl: 'Trace Url:'},
    'zh-cn': {username: '用户名', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '管理人员', save: '保存', restore: '还原', lineChart: '折线图', barChart: '柱状图', pieChart: '饼状图', traceUrl: '追踪链接：'}
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
  // .resultTipsLine{
  //   display: flex;
  //   .resultTips{
  //     flex:1;
  //     display: flex;
  //     align-items: center;
  //     flex-wrap:wrap;
  //     .resultText{
  //       padding-left:14px;
  //       padding-right:14px;
  //       &:first-child{
  //         padding-left:0px;
  //       }
  //       &:last-child{
  //          padding-right:0px;
  //       }
  //       color:@color-text-primary;
  //       .text{
  //         color:@color-text-primary;
  //       }
  //       .blue{
  //         color:#20a0ff;
  //       }
  //     }
  //     .projectText{
  //       border-right:1px solid #9095ab;
  //     }
  //   }
  // }
  // .resultTips{
  //    font-size: 12px;
  //    p{
  //      line-height: 15px;
  //    }
  // }
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
