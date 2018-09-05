<template>
  <div class="model_panel">
    <div class="panel_dragbar" ref="modelEditBar"></div>
  	<div class="model_edit_tool" ref="modelEditTool">
    <i :class="menuStatus==='show'?'el-icon-caret-bottom':'el-icon-caret-top'" @click="slideSubMenu()" class="display_bar"></i>
  		<el-tabs v-model="menuActive" type="card"  @tab-click="subMenuTabClick"  ref="modelToolMenu" >
		    <el-tab-pane :label="$t('kylinLang.common.overview')" name="first">
              <el-tabs class="el-tabs--default model-extrainfo-tab" v-model="subMenuActive">
                  <el-tab-pane :label="$t('modelInfo')" name="first">
                      <table cellspacing="0" cellpadding="0" class="ksd-table ksd-mt-20">
                        <tr>
                          <th>{{$t('modelName')}} <common-tip :content="$t('kylinLang.model.modelNameTips')" ><i class="el-icon-question"></i></common-tip></th>
                          <td><el-input style="width:240px;" size="medium" v-model="currentModelInfo.modelName" :disabled="actionMode==='view'|| !!compeleteModelId"></el-input></td>
                        </tr>
                        <tr>
                          <th>{{$t('discribe')}}</th>
                          <td>
                              <el-input style="width:240px;"
                              type="textarea" size="medium" class="ksd-mtb-4"
                              :rows="1" :disabled="actionMode==='view'"
                              :placeholder="$t('inputModelDescription')"
                              v-model="currentModelInfo.modelDiscribe">
                            </el-input>
                          </td>
                        </tr>
                         <tr v-if="">
                          <th>{{$t('health')}}</th>
                          <td>
                          <i v-if="modelHealth.status!=='RUNNING' && modelHealth.status!=='ERROR' && (modelHealth.progress===0 || modelHealth.progress===100)" :class="modelHealth.icon" :style="{color:modelHealth.color}"></i>
                           <el-progress  :width="15" type="circle" :stroke-width="2" :show-text="false" v-if="modelHealth.status==='RUNNING'" :percentage="modelHealth.progress||0" style="width:20px;vertical-align: sub;"></el-progress>
                           <el-progress  :width="15" type="circle" :stroke-width="2" :show-text="false" v-if="modelHealth.status==='ERROR'" status="exception" :percentage="modelHealth.progress||0" style="width:20px;vertical-align: sub;"></el-progress>
                           <span style="color:rgb(32, 160, 255)" v-if="modelHealth.status==='RUNNING'">{{modelHealth.progress||0}}%</span>
                           <div style="color:#ccc;line-height:20px;margin-top:-8px;display:inline-block;" class=" ksd-ml-10" v-html="modelHealth.msg"></div>
                          </td>
                        </tr>
                         <tr>
                          <th>{{$t('owner')}}</th>
                          <td>
                             {{currentModelInfo.owner}}
                          </td>
                        </tr>
                      </table>
                  </el-tab-pane>
                  <el-tab-pane :label="$t('setting')" name="second" style="padding:0;">
                    <div style="overflow-y:auto;height:100%;width:100%;">
                   <partition-column :comHeight="260" :modelInfo="modelInfo" :actionMode="actionMode"  :columnsForTime="timeColumns" :columnsForDate="dateColumns" :editLock="editLock"  :tableList="tableList" :partitionSelect="partitionSelect" :checkModel="checkModel" :hasStreamingTable="hasStreamingTable" :showModelCheck="false" leftBoxWidth="60%" rightBoxWidth="90%"></partition-column>
                   </div>
                  </el-tab-pane>
                  <!-- Data model -->
                   <el-tab-pane :label="$t('datamodel')" name="third">
                      <h4 v-if="factTables && factTables.length" class="ksd-mt-20 ksd-mb-15">{{$t('kylinLang.common.fact')}}</h4>
                      <el-table :data="factTables"  v-if="factTables && factTables.length" class="ksd-mb-20 ksd-mt-6" border style="width: 100%" :show-header="false">
                        <el-table-column
                          width="180">
                           <template slot-scope="scope">
                              {{$t('kylinLang.common.tableName')}}
                            </template>
                        </el-table-column>
                        <el-table-column
                          label="tableName">
                           <template slot-scope="scope">
                              {{scope.row.tableInfo.name}}
                            </template>
                        </el-table-column>
                      </el-table>
                      <h4  v-if="limitLookupTables && limitLookupTables.length" class="ksd-mt-20 ksd-mb-15">{{$t('kylinLang.common.lookup')}}</h4>
                      <el-table v-show="limitLookupTables && limitLookupTables.length" class="ksd-mt-6 formTable" :data="limitLookupTables"  border style="width: 100%">
                          <el-table-column
                            label="ID"
                            width="180">
                            <template slot-scope="scope">
                              {{scope.$index+1}}
                            </template>
                          </el-table-column>
                          <el-table-column
                            :label="$t('kylinLang.common.alias')"
                            width="180">
                             <template slot-scope="scope">
                              {{scope.row.tableInfo.alias}}
                            </template>
                          </el-table-column>
                          <el-table-column
                            :label="$t('kylinLang.common.tableName')">
                            <template slot-scope="scope">
                              {{scope.row.tableInfo.name}}
                            </template>
                          </el-table-column>
                          <el-table-column
                            :renderHeader="renderColumn">
                             <template slot-scope="scope">
                              <el-checkbox v-model="scope.row.isSnapshot" @change="changeSnapshotStatus(scope.row.tableInfo.guid)"></el-checkbox>
                            </template>
                          </el-table-column>
                        </el-table>
                  </el-tab-pane>
                  <el-tab-pane :label="$t('dimension')" name="fourth">
                    <div v-for="(key, value) in dimensions" :key="value" v-show="dimensions[value].length">
                      <h4 class="ksd-mt-20 ksd-mb-10" style="font-size:12px;" >{{value}}</h4>
                      <div class="dimensionBox">
                        <el-tag class="ksd-ml-10 ksd-mb-6" :closable="true" @close="setColumnDisable(i.guid, i.name, i.isComputed)" type="primary" v-for="i in dimensions[value]" :key="i.name">{{i.name}}</el-tag>&nbsp;&nbsp;
                      </div>
                    </div>
                  </el-tab-pane>
                  <el-tab-pane :label="$t('measure')" name="fifth">
                    <div v-for="(key, value) in measures" :key="value" v-show="measures[value].length">
                      <h4 class="ksd-mt-20 ksd-mb-10" style="font-size:12px;">{{value}}</h4>
                       <div class="dimensionBox">
                      <el-tag class="ksd-ml-10 ksd-mb-6" :closable="true" @close="setColumnDisable(i.guid, i.name, i.isComputed)" v-for="i in measures[value]" type="primary" :key="i.name">{{i.name}}</el-tag>&nbsp;&nbsp;
                      </div>
                    </div>
                  </el-tab-pane>
                  <el-tab-pane :label="$t('sql')" name="sixth">
                    <div class="ksd-ml-10 ksd-mr-10">
                      <kap_editor v-show="sqlPatterns.length > 0"  ref="sqlPatterns" class="ksd-mt-20 ksd-mb-10" height="220" width="100%" lang="sql" theme="chrome" v-model="sqlPatterns">
                      </kap_editor>
                      <p v-show="!(sqlPatterns&&sqlPatterns.length)" class="ksd-center ksd-mt-20">{{$t('NoSQLInfo')}}</p>
                    </div>
                  </el-tab-pane>
              </el-tabs>
          </el-tab-pane>
          <el-tab-pane :label="$t('tableStatistics')" name="second">

              <el-tabs class="el-tabs--default model-extrainfo-tab"  v-model="sampleDataMenu">
                <el-tab-pane name="first" :label="$t('kylinLang.model.metaData')">
                   <h4 class="ksd-mt-20 ksd-mb-16">{{selectTable.tablename ? (selectTable.database + '.' + selectTable.tablename) : ''}}</h4>
                     <el-table
                      :data="statistics.slice(1)"
                      tooltip-effect="dark"
                      border
                      style="width: 100%" class="staticsTableStyle ksd-mt-10">
                       <el-table-column show-overflow-tooltip v-for="(val,index) in statistics[0]" :key="index"
                        :fixed="index === 0"
                        :width="36+15*(statistics[0][index]&&statistics[0][index].length || 10)"
                        :label="statistics[0][index]">
                         <template slot-scope="scope">
                            {{index === 0? $t('kylinLang.dataSource.'+scope.row[0]): '' + scope.row[index]}}
                          </template>
                      </el-table-column>
                    </el-table>
                </el-tab-pane>
                <el-tab-pane name="second" :label="$t('kylinLang.model.checkData')">
                   <h4 class="ksd-mt-20 ksd-mb-16">{{selectTable.database + '.' + selectTable.tablename }}</h4>
                    <el-table
                      :data="modelStatics.slice(1)"
                      tooltip-effect="dark"
                      border
                      style="width: 100%" class="staticsTableStyle ksd-mt-10">
                      <el-table-column v-for="(val,index) in modelStatics[0]" :key="index"
                        :fixed="index === 0"
                        show-overflow-tooltip
                        :prop="''+index"
                        :width="36+15*(modelStatics[0][index]&&modelStatics[0][index].length || 4)"
                        :label="modelStatics[0][index]">
                      </el-table-column>
                    </el-table>
                </el-tab-pane>
              </el-tabs>
          </el-tab-pane>
  		</el-tabs>
  	</div>
  </div>
</template>
<script>
import { mapActions, mapGetters } from 'vuex'
import $ from 'jquery'
import { changeDataAxis } from '../../util/index'
import { modelHealthStatus } from '../../config'
import { handleSuccess } from '../../util/business'
import partitionColumn from 'components/model/model_partition.vue'
// import commonTip from 'components/common/common_tip'
export default {
  name: 'modelPanel',
  data () {
    return {
      menuStatus: 'show',
      dateFormat: [{label: 'yyyy-MM-dd', value: 'yyyy-MM-dd'}, {label: 'yyyyMMdd', value: 'yyyyMMdd'}, {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'}],
      timeFormat: [{label: 'HH:mm:ss', value: 'HH:mm:ss'}, {label: 'HH:mm', value: 'HH:mm'}, {label: 'HH', value: 'HH'}],
      checkPartition: this.partitionSelect,
      modelStatics: [],
      modelStaticsCache: [],
      resultDimensionArr: {},
      resultMeasureArr: {},
      sqlPatterns: this.sqlString,
      project: localStorage.getItem('selected_project'),
      tableData: [],
      columnsD: this.columnsForDate,
      columnsT: this.columnsForTime,
      needSetTime: true,
      hasSeparate: false,
      statistics: [],
      ST: null,
      dragging: false,
      subMenuActive: this.activeNameSub,
      sampleDataMenu: 'first',
      offsetHeight: 0,
      menuActive: this.activeName
    }
  },
  components: {
    'partition-column': partitionColumn
  },
  props: ['modelInfo', 'compeleteModelId', 'actionMode', 'editLock', 'columnsForTime', 'columnsForDate', 'activeName', 'activeNameSub', 'tableList', 'selectTable', 'partitionSelect', 'sqlString', 'checkModel', 'hasStreamingTable'],
  methods: {
    ...mapActions({
      getAutoModelSql: 'GET_AUTOMODEL_SQL',
      loadTableExt: 'LOAD_DATASOURCE_EXT'
    }),
    changeSnapshotStatus (guid) {
      this.$emit('changeIsSnapshot', guid)
    },
    renderColumn (h) {
      return (<span><span>{this.$t('snapshorttip')}</span>&nbsp;<common-tip content={this.$t('snapshortdesc')} >
        <i class="el-icon-question question-icon"></i>
      </common-tip></span>)
    },
    setColumnDisable (guid, columnName, isComputed) {
      this.$emit('changeColumnType', guid, columnName, 'M', isComputed)
    },
    slideSubMenu (currentMenuStatus) {
      var editTool = this.$refs.modelEditTool
      var dragbar = this.$el.querySelector('.panel_dragbar')
      var content = this.$el.querySelectorAll('.el-tabs__content')
      if (currentMenuStatus) {
        this.menuStatus = currentMenuStatus
      }
      if (this.menuStatus === 'hide') {
        this.menuStatus = 'show'
        editTool.style.bottom = '0px'
        dragbar.style.bottom = this.offsetHeight + 360 + 'px'
        content[0].style.height = this.offsetHeight + 338 + 'px'
        content[1].style.height = this.offsetHeight + 260 + 'px'
        content[2].style.height = this.offsetHeight + 260 + 'px'
        editTool.style.height = this.offsetHeight + 360 + 'px'
      } else {
        this.menuStatus = 'hide'
        editTool.style.bottom = '-318px'
        dragbar.style.bottom = '42px'
        content[0].style.height = '338px'
        content[1].style.height = '260px'
        content[2].style.height = '260px'
        editTool.style.height = '360px'
      }
    },
    changeDateColumn (val) {
      if (val === this.checkPartition.time_column && !this.modelInfo.uuid) {
        this.$set(this.checkPartition, 'time_column', null)
        this.$set(this.checkPartition, 'time_format', null)
      }
      this.needSetTime = true
      for (var i in this.columnsForDate) {
        if (i === this.checkPartition.date_table) {
          for (var s = 0; s < this.columnsForDate[i].length; s++) {
            if (this.columnsForDate[i][s].name === this.checkPartition.date_column) {
              if (!this.columnsForDate[i][s].isFormat) {
                this.needSetTime = false
                this.$set(this.checkPartition, 'partition_date_format', 'yyyyMMdd')
                this.$set(this.checkPartition, 'time_format', null)
                this.$set(this.checkPartition, 'time_column', null)
                this.hasSeparate = false
              }
            }
          }
        }
      }
    },
    changeSeparate (val) {
      if (!val && !this.modelInfo.uuid) {
        this.$set(this.checkPartition, 'time_column', '')
        this.$set(this.checkPartition, 'time_format', '')
      }
    },
    subMenuTabClick () {
      this.slideSubMenu('hide')
    },
    loadTableStatics (database, tableName) {
      this.statistics = []
      this.modelStatics = []
      if (!database || !tableName) {
        return
      }
      this.loadTableExt({tableName: database + '.' + tableName, project: this.project}).then((res) => {
        handleSuccess(res, (data) => {
          if (!data) {
            return
          }
          var arr = []
          var lenOffeature = data && data.columns_stats && data.columns_stats.length || 0
          if (lenOffeature) {
            arr = [[''], ['cardinality'], ['maxLengthVal'], ['maximum'], ['minLengthVal'], ['minimal'], ['nullCount']]
            for (let i = 0; i < lenOffeature; i++) {
              arr[0].push(data.columns_stats[i].column_name)
              arr[1].push(data.columns_stats[i].cardinality)
              arr[2].push(data.columns_stats[i].max_length_value)
              arr[3].push(data.columns_stats[i].max_value)
              arr[4].push(data.columns_stats[i].min_length_value)
              arr[5].push(data.columns_stats[i].min_value)
              arr[6].push(data.columns_stats[i].null_count)
            }
            this.statistics = arr
          }
          var tableData = this.$store.state.datasource.dataSource[this.modelInfo.project || localStorage.getItem('selected_project')]
          var tableInfo = []
          for (var k = 0; k < (tableData && tableData.length || 0); k++) {
            if (tableData[k].database === database && tableData[k].name === tableName) {
              tableInfo = tableData[k]
              break
            }
          }
          var sampleData = changeDataAxis(data.sample_rows, true)
          var sampleDataLen = sampleData && sampleData.length || 0
          if (sampleDataLen) {
            var basicColumn = [['']]
            for (var i = 0; i < sampleDataLen; i++) {
              for (var m = 0; m < sampleData[i].length - 1; m++) {
                basicColumn[0].push(tableInfo.columns[m].name)
              }
              break
            }
            this.modelStatics = basicColumn.concat(sampleData)
          }
        })
      })
    },
    tableStaticsBaseData () {
      var tableData = this.tableList
      for (var k = 0; k < tableData.length; k++) {
        if (tableData[k].database === this.selectTable.database && tableData[k].name === this.selectTable.tablename) {
          this.tableData = tableData[k]
          break
        }
      }
      this.loadTableStatics(this.selectTable.database, this.selectTable.tablename, this.selectTable.columnname)
    }
  },
  watch: {
    'selectTable.tablename' () {
      this.tableStaticsBaseData()
    },
    'partitionSelect.partition_time_column' (val) {
      if (val) {
        this.hasSeparate = true
      } else {
        this.hasSeparate = false
      }
    },
    'sqlString' (val) {
      this.sqlPatterns = val
    },
    activeName (val) {
      this.menuActive = val
    }
  },
  computed: {
    ...mapGetters([
      'selectedProjectDatasource'
    ]),
    editMode () {
      return this.editLock
    },
    modelHealth () {
      var obj = {}
      this.$store.state.model.modelsDianoseList.forEach((data) => {
        if (data.modelName === this.modelInfo.modelName) {
          Object.assign(obj, {
            progress: data.progress === 0 ? 0 : parseInt(data.progress),
            msg: (data.messages && data.messages.length ? data.messages.map((x) => {
              return x.replace(/\r\n/g, '\n')
            }) : [modelHealthStatus[data.heathStatus].message]).join('\n'),
            icon: modelHealthStatus[data.heathStatus].icon,
            status: data.heathStatus,
            color: modelHealthStatus[data.heathStatus].color
          })
        }
      })
      return obj
    },
    currentModelInfo () {
      return this.modelInfo
    },
    dateColumns () {
      return this.columnsForDate || []
    },
    timeColumns () {
      return this.columnsForTime || []
    },
    dateColumnsByTable () {
      for (var i in this.columnsForDate) {
        if (i === this.checkPartition.date_table) {
          return this.columnsForDate[i]
        }
      }
      return []
    },
    timeColumnsByTable () {
      for (var i in this.columnsForTime) {
        if (i === this.checkPartition.time_table) {
          return this.columnsForTime[i].filter((column) => {
            if (i !== this.checkPartition.date_table || column.name !== this.checkPartition.date_column) {
              return column
            }
          })
        }
      }
      return []
    },
    dimensions () {
      this.resultDimensionArr = {}
      for (var k = 0, len = this.tableList && this.tableList.length || 0; k < len; k++) {
        this.resultDimensionArr[this.tableList[k].alias] = this.resultDimensionArr[this.tableList[k].alias] || []
        var clen = this.tableList[k] && this.tableList[k].columns && this.tableList[k].columns.length || 0
        for (var m = 0; m < clen; m++) {
          if (this.tableList[k].columns[m].btype === 'D') {
            // this.resultDimensionArr[this.tableList[k].alias] = this.resultDimensionArr[this.tableList[k].name] || []
            this.resultDimensionArr[this.tableList[k].alias].push({name: this.tableList[k].columns[m].name, guid: this.tableList[k].guid, isComputed: this.tableList[k].columns[m].isComputed})
          }
        }
      }
      return this.resultDimensionArr
    },
    factTables () {
      for (var k = 0, len = this.tableList && this.tableList.length || 0; k < len; k++) {
        if (this.tableList[k].kind === 'ROOTFACT') {
          return [{keyName: 'Tablename', tableInfo: this.tableList[k]}]
        }
      }
      return []
    },
    limitLookupTables () {
      var limitLookupResult = []
      for (var k = 0, len = this.tableList && this.tableList.length || 0; k < len; k++) {
        if (this.tableList[k].kind !== 'ROOTFACT') {
          limitLookupResult.push({isSnapshot: this.tableList[k].kind === 'LOOKUP', tableInfo: this.tableList[k]})
        }
      }
      return limitLookupResult
    },
    measures () {
      this.resultMeasureArr = {}
      for (var k = 0, len = this.tableList && this.tableList.length || 0; k < len; k++) {
        this.resultMeasureArr[this.tableList[k].alias] = this.resultMeasureArr[this.tableList[k].alias] || []
        var mlen = this.tableList[k].columns && this.tableList[k].columns.length || 0
        for (var m = 0; m < mlen; m++) {
          if (this.tableList[k].columns[m].btype === 'M') {
            // this.resultMeasureArr[this.tableList[k].alias] = this.resultMeasureArr[this.tableList[k].name] || []
            this.resultMeasureArr[this.tableList[k].alias].push({name: this.tableList[k].columns[m].name, guid: this.tableList[k].guid, isComputed: this.tableList[k].columns[m].isComputed})
          }
        }
      }
      return this.resultMeasureArr
    }
  },
  mounted () {
    var editor = this.$refs.sqlPatterns && this.$refs.sqlPatterns.$refs.kapEditor.editor || ''
    editor.setReadOnly(true)
    var editTool = this.$el.querySelector('.model_edit_tool')
    var dragbar = this.$el.querySelector('.panel_dragbar')
    var content = this.$el.querySelectorAll('.el-tabs__content')
    dragbar.onmousedown = (e) => {
      e.preventDefault()
      this.dragging = true
      var oldTop = 0
      var fullScreen = $(window)
      // handle mouse movement
      $(document).mousemove((e) => {
        if (e.pageY - oldTop > 4 || oldTop - e.pageY > 4) {
          oldTop = e.pageY
          this.offsetHeight = fullScreen.height() - e.pageY - 360
          // Set wrapper height
          if (fullScreen.height() - e.pageY > 42) {
            this.menuStatus = 'show'
            dragbar.style.bottom = this.offsetHeight + 360 + 'px'
            if (this.offsetHeight > 0) {
              editTool.style.bottom = '0px'
              editTool.style.height = this.offsetHeight + 360 + 'px'
              content[0].style.height = 338 + this.offsetHeight + 'px'
              content[1].style.height = 260 + this.offsetHeight + 'px'
              content[2].style.height = 260 + this.offsetHeight + 'px'
            } else {
              editTool.style.bottom = this.offsetHeight + 'px'
              editTool.style.height = '360px'
              content[0].style.height = '338px'
              content[1].style.height = '260px'
              content[2].style.height = '260px'
            }
          } else {
            this.menuStatus = 'hide'
            this.offsetHeight = 0
          }
        }
      })
    }
    $(document).mouseup((e) => {
      if (this.dragging) {
        $(document).unbind('mousemove')
        this.dragging = false
      }
    })
  },
  created () {
    this.$on('menu-toggle', (currentMenuStatus) => {
      this.slideSubMenu(currentMenuStatus)
    })
    setTimeout((argument) => {
      this.slideSubMenu('show')
    }, 1000)
    this.tableStaticsBaseData()
  },
  destroyed () {
    clearTimeout(this.ST)
    $(document).unbind('mouseup')
    $(document).unbind('mousemove')
  },
  locales: {
    'en': {modelName: 'Model Name', discribe: 'Model Description', owner: 'Owner', inputModelDescription: 'Please input model description.', modelInfo: 'Model Info', partition: 'Partition', setting: 'Setting', filter: 'Filter', filterCondition: 'Filter Condition', tableStatistics: 'Table Statistics', dimension: 'Dimension', measure: 'Measure', filterPlaceHolder: 'Please input filter condition', health: 'Model health', NoSQLInfo: 'No SQL patterns.', sql: 'SQL Patterns', datamodel: 'Model', snapshorttip: 'Snapshot', snapshortdesc: '1.If lookup table >300Mb, then it cannot be a snapshot, and can support query only when joining its fact table;<br/>2.You can overwrite the limit of lookup table size(300Mb in default) on kylin.properties;'},
    'zh-cn': {modelName: '模型名称', discribe: '模型描述', owner: 'Owner', inputModelDescription: '请输入模型的描述。', modelInfo: '模型信息', 'partition': '分区', setting: '设置', filter: '过滤器', filterCondition: '过滤条件', tableStatistics: '源表信息', dimension: '维度', measure: '度量', filterPlaceHolder: '请输入过滤条件', health: '模型健康', NoSQLInfo: '没有"SQL查询记录"的相关信息。', sql: 'SQL Patterns', datamodel: '模型', snapshorttip: '以snapshot形式存储', snapshortdesc: '1.当维度表大于300Mb时，无法以snapshot存储，不支持独立查询；<br/>2.维度表大小的限制（出厂默认为300Mb）可以在kylin.properties中重写；'}
  }
}
</script>
<style lang="less">
@import '../../assets/styles/variables.less';
  .model_panel {
    h4{
      color:@text-title-color;
    }
    .question-icon{
      color:@text-secondary-color;
    }
    .panel_dragbar {
      width: 100%;
      height: 4px;
      position: fixed;
      cursor: row-resize;
      opacity: 1;
      z-index: 2000;
      bottom: 42px;
      background-color:@modeleditpanel-title-bg-color;
    }
    .model_edit_tool {
      background-color: @modeleditpanel-title-bg-color;
      &>.el-tabs--card{
        &>.el-tabs__header{
          margin-bottom: 0;
          .el-tabs__nav{
            margin-top: 10px;
            &>.el-tabs__item{
              background-color: @modeleditpanel-title-bg-color;
             &.is-active{
              background-color: @fff;
              color: @text-title-color;
             }
            }
          }
        }
        &>.el-tabs__content{
          background-color: @fff;
          height: 100%;
          // padding-top: 26px;
          &>div{
            height:100%;
          }
        }
      }

      .model-extrainfo-tab{
        display: flex;
        flex-direction: column;
        &>.el-tabs__header{
          margin-bottom: 0;
        }
        &>.el-tabs__content{
          &>.el-tab-pane{
            overflow-y:auto;
            height: 100%;
            padding: 0 30px;
            .partitionBox{
              overflow-y:visible;
              height: 100%;
            }
            .el-tag.el-tag--primary:hover {
              background: none;
            }
          }
        }
      }
    	z-index:2000;
      position:fixed;
      // background-color: #fff;
      bottom:0;
      left:138px;
      right: 0px;

      &.smallScreen {
        left:56px;
      }
      .display_bar{
        position: absolute;
        top:10px;
        right: 10px;
        cursor: pointer;
        z-index: 1
      }
      .dimensionBox {
        // background-color: #2b2d3b;
        border: solid 1px @line-border-color;
        padding-top: 6px;
      }

      .el-tab-pane .el-form{
        height: 260px;
        // overflow-y: auto;
      }
    }
  }
</style>
