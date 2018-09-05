<template>
	<div  class="partition-box">
    <div style="display: flex;">
      <div style="padding:20px;" :style="{width: leftBoxWidth}">
        <el-form v-model="checkPartition" label-width="185px" :rules="rule" ref="partition" :label-position="labelPosition">
          <el-form-item :label="$t('partitionDateColumn')">
            <span slot="label">{{$t('partitionDateColumn')}}
            <common-tip :content="$t('kylinLang.model.partitionDateTip')" ><i class="el-icon-question"></i></common-tip>
            </span>
            <el-row :gutter="10" :class="{resetMargin: labelPosition === 'right'}">
            <el-col :span="12">
               <el-select style="width:100%" size="medium" v-model="checkPartition.date_table" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode || actionMode==='view'" @visible-change="ensurePartition">
                <el-option :label="$t('kylinLang.common.pleaseSelect')" value=""></el-option>
                <el-option
                  v-for="(key,value) in dateColumns"
                  :key="key+''"
                  :label="value"
                  :value="value">
                </el-option>
              </el-select>
            </el-col>
            <el-col :span="12">
              <el-select style="width:100%" size="medium" v-model="checkPartition.date_column" @change="changeDateColumn" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode  || actionMode==='view'">
                <el-option
                  v-for="item in dateColumnsByTable"
                  :key="item.name"
                  :label="item.name"
                  :value="item.name">
                </el-option>
              </el-select>
            </el-col>
            </el-row>
          </el-form-item>
          <el-form-item :label="$t('dateFormat')" prop="partition_date_format">
            <el-row :gutter="10" :class="{resetMargin: labelPosition === 'right'}">
              <el-col :span="12">
                <el-select size="medium" v-model="checkPartition.partition_date_format" style="width: 100%" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode  || actionMode==='view'">
                  <el-option
                    v-for="item in timeFormat"
                    :key="item.value"
                    :label="item.label"
                    :value="item.value">
                  </el-option>
                </el-select>
              </el-col>
            </el-row>
          </el-form-item>
          <el-form-item :label="$t('hasSeparateLabel')" v-show="needSetTime" class="ksd-mt-20">
            <span slot="label">
              {{$t('hasSeparateLabel')}}
              <common-tip :content="$t('kylinLang.model.partitionSplitTip')" ><i class="el-icon-question"></i></common-tip>
              <el-switch v-model="hasSeparate" active-text="OFF" @change="changeSepatate" inactive-text="ON" :disabled="editMode  || actionMode==='view'" v-if="labelPosition === 'top'"></el-switch>
            </span>
              <el-switch v-model="hasSeparate" active-text="OFF" @change="changeSepatate" inactive-text="ON" :disabled="editMode  || actionMode==='view'" v-if="labelPosition === 'right'"></el-switch>
          </el-form-item>
          <el-form-item :label="$t('kylinLang.model.primaryPartitionColumn')" v-show="hasSeparate">
           <span slot="label">{{$t('kylinLang.model.primaryPartitionColumn')}}
            <common-tip :content="$t('kylinLang.model.mutilPartitionTip')" ><i class="el-icon-question"></i></common-tip></span>
            <el-row>
              <el-col :span="11">
                <el-select size="medium" style="width:100%" v-model="checkPartition.mutilLevel_table" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode  || actionMode==='view'">
                  <el-option
                    v-for="(key,value) in timeColumns"
                    :key="key+''"
                    :label="value"
                    :value="value">
                  </el-option>
                </el-select>
                </el-col>
                <el-col :span="1">&nbsp;</el-col>
                <el-col :span="12">
                  <el-select size="medium" style="width:100%" v-model="checkPartition.mutilLevel_column" :placeholder="$t('kylinLang.common.pleaseSelect')" v-show="hasSeparate" :disabled="editMode  || actionMode==='view'">
                    <el-option
                      v-for="item in timeColumnsByTable"
                      :key="item.name"
                      :label="item.name"
                      :value="item.name">
                    </el-option>
                </el-select>
                </el-col>
            </el-row>
          </el-form-item>
          <el-form-item :label="$t('filterCondition')" class="ksd-mt-20">
             <el-input
              type="textarea" :disabled="actionMode==='view'"
              :autosize="{ minRows: 2, maxRows: 4}"
              :placeholder="$t('filterPlaceHolder')"
              v-model="modelInfo.filterStr">
            </el-input>
          </el-form-item>
        </el-form>
        <div class="el-row" v-if="showModelCheck">
          <div class="el-col el-col-24 ksd-pb-10">
            <span v-show="!hasStreamingTable">{{$t('kylinLang.model.checkModel')}}</span>
            <common-tip v-show="!hasStreamingTable" :content="$t('kylinLang.model.modelCheck')" >
              <i class="el-icon-question"></i>
            </common-tip>
            <el-switch v-show="!hasStreamingTable" v-model="checkModel.openModelCheck" active-text=""  inactive-text="" @change="changeCheckModel">
            </el-switch>
          </div>
          <div class="el-col" :class="[layout.right ? 'el-col-24' : 'el-col-8']">
            <el-checkbox v-show="!hasStreamingTable && checkModel.openModelCheck" v-model="checkModel.modelStatus">{{$t('kylinLang.model.modelStatusCheck')}}</el-checkbox>
          </div>
          <div class="el-col" :class="[layout.right ? 'el-col-24' : 'el-col-8']">
            <el-checkbox v-show="!hasStreamingTable && checkModel.openModelCheck" v-model="checkModel.factTable">{{$t('kylinLang.model.factTableSampling')}}</el-checkbox>
          </div>
          <div class="el-col" :class="[layout.right ? 'el-col-24' : 'el-col-8']">
            <el-checkbox v-show="!hasStreamingTable && checkModel.openModelCheck" v-model="checkModel.lookupTable">{{$t('kylinLang.model.lookupTableSampling')}}</el-checkbox>
          </div>
        </div>
      </div>
      <div v-show="!layout.right&&checkPartition.date_column" class="close-sample-btn" @click="changeDateColumn"><i class="el-icon-d-arrow-left"></i></div>
      <div v-show="layout.right" class="partition-right-sample">
       <p class="ksd-ml-30 ksd-mt-20">{{$t('kylinLang.model.checkData')}}</p>
        <div class="close-sample-btn" @click="closeSample"><i class="el-icon-d-arrow-right"></i></div>
       <div class="sampe-table" :style="{width: rightBoxWidth}">
          <el-table class="ksd-mt-10"
            :data="modelStatics"
             style="width: 100%"
            border>
            <el-table-column
              prop="0"
              width="40"
              label="ID">
            </el-table-column>
            <el-table-column
              prop="1"
              :label="checkPartition.date_column">
            </el-table-column>
          </el-table>
        </div>
      </div>
    </div>
	</div>
</template>
<script>
import { mapActions, mapGetters } from 'vuex'
import { changeDataAxis } from '../../util/index'
import { handleSuccess, handleError } from '../../util/business'
import { timeDataType } from '../../config/index'
export default {
  props: ['modelInfo', 'actionMode', 'editLock', 'columnsForTime', 'columnsForDate', 'partitionSelect', 'comHeight', 'checkModel', 'hasStreamingTable', 'showModelCheck', 'labelPos', 'leftBoxWidth', 'rightBoxWidth'],
  data () {
    return {
      menuStatus: 'show',
      dateFormat: [
        {label: 'yyyy-MM-dd', value: 'yyyy-MM-dd'},
        {label: 'yyyyMMdd', value: 'yyyyMMdd'},
        {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'},
        {label: 'yyyy-MM-dd HH:mm:ss.SSS', value: 'yyyy-MM-dd HH:mm:ss.SSS'}
      ],
      integerFormat: [
        {label: 'yyyy-MM-dd', value: 'yyyy-MM-dd'},
        {label: 'yyyyMMdd', value: 'yyyyMMdd'},
        {label: 'yyyy-MM-dd HH:mm:ss', value: 'yyyy-MM-dd HH:mm:ss'},
        {label: 'yyyy-MM-dd HH:mm:ss.SSS', value: 'yyyy-MM-dd HH:mm:ss.SSS'},
        {label: '', value: ''}
      ],
      regArr: [new RegExp(/^\d{4}([^\d])\d{2}\1\d{2}$/), new RegExp(/^\d{8}$/), new RegExp(/^\d{4}([^\d])\d{2}\1\d{2}\s+?\d{2}([:])\d{2}\2\d{2}$/), new RegExp(/^\d{4}([^\d])\d{2}\1\d{2}\s+?\d{2}([:])\d{2}\2\d{2}\.\d{3}$/)],
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
      statistics: [],
      layout: {
        left: 24,
        right: 0
      },
      rule: {
        partition_date_format: [
          {validator: this.dateFormatCheck, trigger: 'change'}
        ]
      },
      labelPosition: this.labelPos || 'right'
    }
  },
  methods: {
    ...mapActions({
      loadColumnSampleData: 'GET_COLUMN_SAMPLEDATA',
      validColumnFormat: 'VALID_PARTITION_COLUMN',
      loadTableExt: 'LOAD_DATASOURCE_EXT'
    }),
    formValid (filed) {
      this.$refs.partition.validateField(filed)
      // this.$refs.partition.validate()
    },
    getFullTableNameInfo (alias) {
      for (var i in this.columnsForDate) {
        if (i === alias) {
          var columnObj = this.columnsForDate[i]
          if (columnObj && columnObj.length) {
            return [columnObj[0].database, columnObj[0].table]
          }
        }
      }
      return []
    },
    getColumnInfo (column) {
      for (var i in this.dateColumnsByTable) {
        if (this.dateColumnsByTable[i].name === column) {
          return this.dateColumnsByTable[i]
        }
      }
      return {}
    },
    ensurePartition (visible) {
      if (Object.keys(this.columnsForDate) <= 0 && visible) {
        var para = {
          showClose: true,
          message: this.$t('ensurePartition'),
          type: 'warning',
          duration: 0
        }
        this.$message(para)
      }
    },
    dateFormatCheck (rule, value, callback) {
      if (!this.checkPartition.partition_date_format || this.modelStatics.length === 0) {
        callback()
        return
      }
      var project = this.modelInfo.project || this.project
      if (!this.checkPartition.date_column && !this.checkPartition.date_table) {
        return
      }
      var databaseAndTableName = this.getFullTableNameInfo(this.checkPartition.date_table)
      this.validColumnFormat({project: project, table: databaseAndTableName.join('.'), column: this.checkPartition.date_column, format: this.checkPartition.partition_date_format}).then((res) => {
        handleSuccess(res, (data) => {
          if (!data && data !== null) {
            callback(new Error(this.$t('validFail')))
          } else {
            callback()
          }
        })
      }, (res) => {
        handleError(res, (a, b, c, d) => {
          callback(new Error(d))
        })
      })
    },
    querySearchForDate (queryString, cb) {
      var result = queryString ? this.dateFormat.filter((formatStr) => {
        return formatStr.value.indexOf(queryString) === 0
      }) : this.dateFormat
      cb(result)
    },
    initTimeForm () {
      let partitionColumn = this.getColumnInfo(this.checkPartition.date_column)
      if (partitionColumn && partitionColumn.column && partitionColumn.column.datatype && timeDataType.indexOf(partitionColumn.column.datatype) >= 0 && this.checkPartition.partition_date_format === '') {
        if (this.modelStatics.length > 0 && this.modelStatics[0].length > 1) {
          let sampleData = this.modelStatics[0][1]
          if (this.regArr[0].test(sampleData)) {
            this.checkPartition.partition_date_format = 'yyyy-MM-dd'
            return
          }
          if (this.regArr[1].test(sampleData)) {
            this.checkPartition.partition_date_format = 'yyyyMMdd'
            return
          }
          if (this.regArr[2].test(sampleData)) {
            this.checkPartition.partition_date_format = 'yyyy-MM-dd HH:mm:ss'
            return
          }
          if (this.regArr[3].test(sampleData)) {
            this.checkPartition.partition_date_format === 'yyyy-MM-dd HH:mm:ss.SSS'
            return
          }
        }
        this.checkPartition.partition_date_format === 'yyyy-MM-dd'
      }
    },
    changeDateColumn (val) {
      if (val === this.checkPartition.time_column && !this.modelInfo.uuid) {
        this.$set(this.checkPartition, 'mutilLevel_column', '')
        this.$set(this.checkPartition, 'mutilLevel_format', '')
      }
      this.needSetTime = true
      var databaseAndTableName = this.getFullTableNameInfo(this.checkPartition.date_table)
      if (databaseAndTableName.length) {
        this.loadTableStatics(databaseAndTableName[0], databaseAndTableName[1], this.checkPartition.date_column)
      }
      this.formValid('partition_date_format')
    },
    changeCheckModel (val) {
      if (val) {
        this.checkModel.modelStatus = true
        this.checkModel.factTable = true
        this.checkModel.lookupTable = true
      } else {
        this.checkModel.modelStatus = false
        this.checkModel.factTable = false
        this.checkModel.lookupTable = false
      }
    },
    closeSample () {
      this.layout.left = 24
      this.layout.right = 0
      this.modelStatics = []
    },
    loadTableStatics (database, tableName, columnName) {
      var project = this.modelInfo.project || this.project
      this.loadColumnSampleData({ project: project, table: database + '.' + tableName, column: columnName }).then((res) => {
        handleSuccess(res, (data) => {
          this.layout.left = 14
          this.layout.right = 10
          var sampleData = changeDataAxis([data], true)
          this.modelStatics = sampleData
          this.initTimeForm()
        })
      }, () => {
        this.modelStatics = []
        this.layout.left = 14
        this.layout.right = 10
        this.modelStatics = []
      })
    },
    changeSepatate (val) {
      this.$set(this.checkPartition, 'mutilLevel_column', '')
      this.$set(this.checkPartition, 'mutilLevel_table', '')
    }
  },
  computed: {
    ...mapGetters([
      'selectedProjectDatasource'
    ]),
    editMode () {
      return this.editLock
    },
    currentModelInfo () {
      return this.modelInfo
    },
    timeFormat () {
      let partitionColumn = this.getColumnInfo(this.checkPartition.date_column)
      if (!partitionColumn || !partitionColumn.column || this.checkPartition.date_table === '' || this.checkPartition.date_column === '') {
        return []
      } else {
        if (timeDataType.indexOf(partitionColumn.column.datatype) === -1) {
          return this.integerFormat
        } else {
          return this.dateFormat
        }
      }
    },
    dateColumns () {
      for (let k in this.columnsForDate) {
        if (k) {
          // this.columnsForDate[''] = []
          return this.columnsForDate
        }
      }
      this.$set(this.checkPartition, 'date_table', '')
      this.$set(this.checkPartition, 'date_column', '')
      this.$set(this.checkPartition, 'partition_date_format', '')
      this.$set(this.checkPartition, 'mutilLevel_table', '')
      this.$set(this.checkPartition, 'mutilLevel_column', '')
      this.closeSample()
    },
    timeColumns () {
      return this.columnsForTime || []
    },
    dateColumnsByTable () {
      for (var i in this.columnsForDate) {
        if (this.checkPartition.date_table === '') {
          this.checkPartition.date_column = ''
        }
        if (i === this.checkPartition.date_table) {
          return this.columnsForDate[i]
        }
      }
      return []
    },
    timeColumnsByTable () {
      if (this.checkPartition.mutilLevel_table === '') {
        this.checkPartition.mutilLevel_column = ''
      }
      for (var i in this.columnsForTime) {
        if (i === this.checkPartition.mutilLevel_table) {
          return this.columnsForTime[i].filter((column) => {
            if (i !== this.checkPartition.date_table || column.name !== this.checkPartition.date_column) {
              return column
            }
          })
        }
      }
      return []
    }
  },
  mounted () {
    this.hasSeparate = !!(this.checkPartition && this.checkPartition.mutilLevel_column)
  },
  locales: {
    'en': {partitionDateColumn: ' Time Partition Column', dateFormat: 'Time Format', hasSeparateLabel: 'More Partition', timeFormat: 'Time Format', filterCondition: 'Filter Condition', filterPlaceHolder: 'The filter condition, no clause "WHERE" needed, eg: Date>YYYY-MM-DD.', noSample: 'Executing data format check depends on table sampling result.', validFail: 'The date format is invalid for this column. Please try another partition column or date format.', ensurePartition: 'Please ensure there is a fact table and time partition column should belong to it.'},
    'zh-cn': {partitionDateColumn: '时间分区列', dateFormat: '时间格式', hasSeparateLabel: '更多分区列', timeFormat: '时间格式', filterCondition: '过滤条件', filterPlaceHolder: '请输入过滤条件，不需要写“WHERE”，例如：Date>YYYY-MM-DD。', noSample: '当表具有采样结果时，才能对其分区列的格式进行检查。', validFail: '分区列格式不正确，请重新选择分区格式或更换分区列。', ensurePartition: '请在模型中设置一张事实表，并从该事实表上选取时间类型的分区列。'}
  }
}
</script>
<style lang="less">
@import '../../assets/styles/variables.less';
.partition-box {
  height: 100%;
  position: relative;
  .el-form-item{
    margin-bottom: 12px;
  }
  .el-form {
    .el-form-item__error {
      position: relative;
    }
  }
  .resetMargin {
    margin: 0 !important;
  }
  .close-sample-btn{
    position: absolute;
    width: 12px;
    height: 42px;
    right: 0;
    top:30%;
    background: @modeledit-bg-color;
    border: 1px solid @line-border-color;
    border-radius: 4px 0 0 4px;
    border-right: none;
    line-height: 42px;
    font-size: 12px;
    cursor: pointer;
    text-align: center;
    color:#455a64;
  }
  .partition-right-sample {
    position: relative;
    border-left: solid 1px @line-border-color;
    .close-sample-btn{
      left:0;
      border-radius: 0 4px 4px 0;
      border-left: none;
      border-right: 1px solid @line-border-color;
    }
    .sampe-table{
      padding-right: 10px;
      padding-left: 30px; 
      padding-bottom: 10px;
      width:100%
    }
  }
}
</style>
