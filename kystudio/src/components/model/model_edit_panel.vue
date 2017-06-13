<template>
	<div class="model_edit_tool">
  <icon :name="menuStatus==='show'?'sort-down':'sort-up'" @click.native="slideSubMenu()" class="display_bar"></icon>
		<el-tabs v-model="menuActive" type="border-card"  @tab-click="subMenuTabClick" @click.native="">
		    <el-tab-pane label="Overview" name="first">
            <el-tabs v-model="subMenuActive" >
                <el-tab-pane :label="$t('modelInfo')" name="first">
                    <table  cellspacing="0" cellpadding="0">
                      <tr>
                        <th>{{$t('modelName')}}</th>
                        <td><el-input v-model="currentModelInfo.modelName" :disabled="actionMode==='view'|| !!compeleteModelId"></el-input></td>
                      </tr>
                      <tr>
                        <th>{{$t('discribe')}}</th>
                        <td>
                            <el-input
                            type="textarea"
                            :rows="2" :disabled="actionMode==='view'"
                            :placeholder="$t('inputModelDescription')"
                            v-model="currentModelInfo.modelDiscribe">
                          </el-input>
                        </td>
                      </tr>
                       <tr v-show="currentModelInfo.owner">
                        <th>{{$t('owner')}}</th>
                        <td>
                           {{currentModelInfo.owner}}
                        </td>
                      </tr>
                    </table>
                </el-tab-pane>
                <el-tab-pane :label="$t('setting')" name="second">
                 <partition-column style="width:800px" :modelInfo="modelInfo" :actionMode="actionMode"  :columnsForTime="timeColumns" :columnsForDate="dateColumns" :tableList="tableList" :partitionSelect="partitionSelect" ></partition-column>
                 <!--  <el-form   label-width="240px">
                    <el-form-item label="Partition Date Column">
                      <el-select v-model="checkPartition.date_table" placeholder="请选择" :disabled="editMode || actionMode==='view'">
                        <el-option
                          v-for="(key,value) in dateColumns"
                          :label="value"
                          :value="value">
                        </el-option>
                      </el-select>
                      <el-select v-model="checkPartition.date_column" @change="changeDateColumn" placeholder="请选择" :disabled="editMode  || actionMode==='view'">
                        <el-option
                          v-for="item in dateColumnsByTable"
                          :label="item.name"
                          :value="item.name">
                        </el-option>
                      </el-select>
                    </el-form-item>
                    <el-form-item label="Date Format">
                      <el-select v-model="checkPartition.partition_date_format" placeholder="请选择" :disabled="!needSetTime || editMode  || actionMode==='view'">
                        <el-option
                          v-for="item in dateFormat"
                          :label="item.label"
                          :value="item.label">
                        </el-option>
                      </el-select>
                    </el-form-item>
                    <el-form-item label="separate time of the day column " v-show="needSetTime">
                    <el-switch v-model="hasSeparate" on-text="" @change="changeSeparate" off-text="" :disabled="editMode  || actionMode==='view'"></el-switch>
                    </el-form-item>
                    <el-form-item label="Partition Time Column" v-show="hasSeparate">
                      <el-select v-model="checkPartition.time_table" placeholder="请选择" :disabled="editMode  || actionMode==='view'">
                        <el-option
                          v-for="(key,value) in timeColumns"
                          :label="value"
                          :value="value">
                        </el-option>
                      </el-select>
                      <el-select v-model="checkPartition.time_column" placeholder="请选择" v-show="hasSeparate" :disabled="editMode  || actionMode==='view'">
                        <el-option
                          v-for="item in timeColumnsByTable"
                          :label="item.name"
                          :value="item.name">
                        </el-option>
                      </el-select>
                    </el-form-item>
                     <el-form-item label="Time Format" v-show="hasSeparate">
                      <el-select v-model="checkPartition.partition_time_format" placeholder="请选择" :disabled="editMode  || actionMode==='view'">
                        <el-option
                          v-for="item in timeFormat"
                          :label="item.label"
                          :value="item.label">
                        </el-option>
                      </el-select>
                    </el-form-item>
                  </el-form> -->
                </el-tab-pane>
                <el-tab-pane :label="$t('filter')" name="five">
                  <el-form label-width="240px">
                    <el-form-item :label="$t('filterCondition')">
                       <el-input :disabled="actionMode==='view'"
                        type="textarea"
                        :autosize="{ minRows: 2, maxRows: 4}"
                        :placeholder="$t('filterPlaceHolder')"
                        v-model="currentModelInfo.filterStr">
                      </el-input>
                    </el-form-item>
                  </el-form>
                </el-tab-pane>
                <el-tab-pane :label="$t('dimension')" name="third">
                  <div v-for="(key, value) in dimensions" :key="key">
                    <el-badge :value="dimensions[value]&&dimensions[value].length" class="item ksd-mt-10" style="background-color:green">
                    <el-button size="small">{{value}}</el-button>
                    </el-badge>
                    <br/>
                    <el-tag class="ksd-ml-10 ksd-mt-6" v-for="i in dimensions[value]" :key="i">{{i}}</el-tag>&nbsp;&nbsp;
                  </div>
                </el-tab-pane>
                <el-tab-pane :label="$t('measure')" name="fourth">
                  <div v-for="(key, value) in measures" :key="key">
                    <el-badge :value="measures[value]&&measures[value].length" class="item ksd-mt-10" >
                    <el-button size="small">{{value}}</el-button>
                    </el-badge>
                    <br/>
                    <el-tag class="ksd-ml-10 ksd-mt-6" v-for="i in measures[value]" :key="i">{{i}}</el-tag>
                  </div>
                </el-tab-pane>

            </el-tabs>
        </el-tab-pane>
		    <!-- <el-tab-pane label="Model Statistics" name="second">Model Statistics</el-tab-pane> -->
        <el-tab-pane :label="$t('tableStatistics')" name="second">
             <el-table
              :data="statistics.slice(1)"
              style="width: 100%">
               <el-table-column v-for="(val,index) in statistics[0]" :key="index"
                :prop="''+index"
                width="220"
                :label="statistics[0][index]">
              </el-table-column>
              <!-- <el-table-column
                prop="column_name"
                label="列名"
                width="180">
              </el-table-column>
              <el-table-column
                prop="cardinality"
                label="基数"
                width="180">
              </el-table-column>
              <el-table-column
                prop="max_length_value"
                label="最大长度值">
              </el-table-column>
               <el-table-column
                prop="max_value"
                label="最大值">
              </el-table-column>
               <el-table-column
                prop="min_length_value"
                label="最小长度值">
              </el-table-column>
               <el-table-column
                prop="min_value"
                label="最小值">
              </el-table-column>
               <el-table-column
                prop="null_count"
                label="空值个数">
              </el-table-column> -->
            </el-table>
        <el-table
          :data="modelStatics.slice(1)"
          border
          style="width: 100%">
          <el-table-column v-for="(val,index) in modelStatics[0]" :key="index"
            :prop="''+index"
            width="220"
            :label="modelStatics[0][index]">
          </el-table-column>
        </el-table>
        </el-tab-pane>
		</el-tabs>

	</div>
</template>
<script>
import { mapActions } from 'vuex'
import { changeDataAxis } from '../../util/index'
import { handleSuccess } from '../../util/business'
import partitionColumn from 'components/model/model_partition.vue'
export default {
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
      project: localStorage.getItem('selected_project'),
      tableData: [],
      columnsD: this.columnsForDate,
      columnsT: this.columnsForTime,
      needSetTime: true,
      hasSeparate: false,
      statistics: []
    }
  },
  components: {
    'partition-column': partitionColumn
  },
  props: ['modelInfo', 'compeleteModelId', 'actionMode', 'columnsForTime', 'columnsForDate', 'activeName', 'activeNameSub', 'tableList', 'selectTable', 'partitionSelect'],
  methods: {
    ...mapActions({
      loadTableExt: 'LOAD_DATASOURCE_EXT'
    }),
    slideSubMenu (currentMenuStatus) {
      if (currentMenuStatus) {
        this.menuStatus = currentMenuStatus
      }
      if (this.menuStatus === 'hide') {
        this.menuStatus = 'show'
        this.$el.style.bottom = '0'
      } else {
        this.menuStatus = 'hide'
        this.$el.style.bottom = '-318px'
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
      this.loadTableExt(database + '.' + tableName).then((res) => {
        handleSuccess(res, (data) => {
          var lenOffeature = data.columns_stats && data.columns_stats.length || 0
          var arr = [['列名'], ['基数'], ['最大长度值'], ['最大值'], ['最小长度值'], ['最小值'], ['空值个数']]
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
          var sampleData = changeDataAxis(data.sample_rows, true)
          console.log(sampleData, 8899)
          var basicColumn = [['列名']]
          for (var i = 0, len = sampleData && sampleData.length || 0; i < len; i++) {
            for (var m = 0; m < sampleData[i].length - 1; m++) {
              basicColumn[0].push(data.columns_stats[m].column_name)
            }
            break
          }
          this.modelStatics = basicColumn.concat(sampleData)
          console.log(this.modelStatics, 99900)
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
    'selectTable.columnname' () {
      this.tableStaticsBaseData()
    },
    'partitionSelect.partition_time_column' (val) {
      if (val) {
        this.hasSeparate = true
      } else {
        this.hasSeparate = false
      }
    }
  },
  computed: {
    editMode () {
      return this.editLock
    },
    // editModeDisabled () {
    //   return this.editLock
    // },
    menuActive () {
      return this.activeName
    },
    subMenuActive () {
      return this.activeNameSub
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
    // hasseparate () {
    //   if (this.checkPartition.time_column !== null) {
    //     console.log(990)
    //     this.hasSeparate = true
    //     return true
    //   }
    //   console.log(9190)
    //   this.hasSeparate = false
    //   return false
    // },
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
        this.resultDimensionArr[this.tableList[k].name] = this.resultDimensionArr[this.tableList[k].name] || []
        for (var m = 0; m < this.tableList[k].columns.length; m++) {
          if (this.tableList[k].columns[m].btype === 'D') {
            this.resultDimensionArr[this.tableList[k].name] = this.resultDimensionArr[this.tableList[k].name] || []
            this.resultDimensionArr[this.tableList[k].name].push(this.tableList[k].columns[m].name)
          }
        }
      }
      return this.resultDimensionArr
    },
    measures () {
      this.resultMeasureArr = {}
      for (var k = 0, len = this.tableList && this.tableList.length || 0; k < len; k++) {
        this.resultMeasureArr[this.tableList[k].name] = this.resultMeasureArr[this.tableList[k].name] || []
        for (var m = 0; m < this.tableList[k].columns.length; m++) {
          if (this.tableList[k].columns[m].btype === 'M') {
            this.resultMeasureArr[this.tableList[k].name] = this.resultMeasureArr[this.tableList[k].name] || []
            this.resultMeasureArr[this.tableList[k].name].push(this.tableList[k].columns[m].name)
          }
        }
      }
      return this.resultMeasureArr
    }

  },
  created () {
    var _this = this
    this.$on('menu-toggle', function (currentMenuStatus) {
      _this.slideSubMenu(currentMenuStatus)
    })
    setTimeout(function (argument) {
      _this.slideSubMenu('show')
    }, 1000)
  },
  mounted () {
  },
  locales: {
    'en': {modelName: 'Model Name', discribe: 'Model Description', owner: 'Owner', inputModelDescription: 'Please input model description', modelInfo: 'Model Info', partition: 'Partition', setting: 'Setting', filter: 'Filter', filterCondition: 'Filter Condition', tableStatistics: 'Table Statistics', dimension: 'Dimension', measure: 'Measure', filterPlaceHolder: 'Please input filter condition'},
    'zh-cn': {modelName: '模型名称', discribe: '模型描述', owner: 'Owner', inputModelDescription: '请输入模型的描述', modelInfo: '模型信息', 'partition': '分区', setting: '设置', filter: '过滤器', filterCondition: '过滤条件', tableStatistics: '采样数据', dimension: '维度', measure: '度量', filterPlaceHolder: '请输入过滤条件'}
  }
}
</script>
<style lang="less">
@import '../../less/config.less';
  .model_edit_tool {
    height: 360px;
  	z-index:2000;
    position:fixed;
    background-color: #fff;
    bottom:0;
    left:200px;
    right: 0px;
    &.smallScreen {
      left:100px;
    }
    .el-tab-pane{
      height: 290px;
      overflow: auto;
    }
    .display_bar{
      position: absolute;
      top:10px;
      right: 10px;
      cursor: pointer;
      z-index: 1
    }
    .el-badge__content {
      background-color: @base-color;
    }
    table{
      width: 100%;
      border-right:1px solid #e0e6ed;
      border-bottom:1px solid #e0e6ed;
      th{
        background: #f1f3f7;
        border-left:1px solid #e0e6ed;
        border-top:1px solid #e0e6ed;
        min-height:44px;
        width: 220px;
        font-weight: normal;
        font-size: 14px;
      }
      td{
        border-left:1px solid #e0e6ed;
        border-top:1px solid #e0e6ed;
        input{
          width: 400px;
          margin: 4px 4px;
        }
        textarea{
          width: 400px;
          margin: 4px 4px;
        }
      }
    }
    .el-tabs--border-card{
      box-shadow: none;
      border-bottom: none;
    }
    .el-tab-pane .el-form{
      height: 260px;
      overflow-y: auto;
    }

  }
</style>
