<template>
	<div  class="partitionBox">
    <el-row>
      <el-col :span="layout.left" style="padding:10px;">
        <el-form v-model="checkPartition"  label-position="top" :rules="rule" ref="partition">
          <el-form-item :label="$t('partitionDateColumn')">
            <span slot="label">{{$t('partitionDateColumn')}}
            <common-tip :content="$t('kylinLang.model.partitionDateTip')" ><icon name="question-circle-o"></icon></common-tip>
            </span>
            <el-row>
            <el-col :span="11">
               <el-select style="width:100%" v-model="checkPartition.date_table" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode || actionMode==='view'">
                <el-option
                  v-for="(key,value) in dateColumns"
                  :key="key+''"
                  :label="value"
                  :value="value">
                </el-option>
              </el-select>
            </el-col>
            <el-col :span="1">&nbsp;</el-col>
            <el-col :span="12">
               <el-select style="width:100%" v-model="checkPartition.date_column" @change="changeDateColumn" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode  || actionMode==='view'">
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
            <el-row>
            <!-- <el-select v-model="checkPartition.partition_date_format" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="!needSetTime || editMode  || actionMode==='view'">
              <el-option
                v-for="item in dateFormat"
                :key="item.label"
                :label="item.label"
                :value="item.label">
              </el-option>
            </el-select> -->
               <el-autocomplete :disabled="editMode  || actionMode==='view'" style="width:100%" @select="formValid('partition_date_format')"
                      class="inline-input"
                      v-model="checkPartition.partition_date_format"
                      :fetch-suggestions="querySearchForDate"
                      :placeholder="$t('kylinLang.common.pleaseSelect')"
                ></el-autocomplete>
             </el-row>
          </el-form-item>
          <el-form-item :label="$t('hasSeparateLabel')" v-show="needSetTime" class="ksd-mt-20">
           <span slot="label">
              {{$t('hasSeparateLabel')}}
              <common-tip :content="$t('kylinLang.model.partitionSplitTip')" ><icon name="question-circle-o"></icon></common-tip>
              <el-switch v-model="hasSeparate" on-text="" @change="changeSepatate" off-text="" :disabled="editMode  || actionMode==='view'"></el-switch>
            </span>
          </el-form-item>
          <el-form-item :label="$t('partitionTimeColumn')" v-show="hasSeparate">
           <span slot="label">{{$t('partitionTimeColumn')}}
            <common-tip :content="$t('kylinLang.model.partitionTimeTip')" ><icon name="question-circle-o"></icon></common-tip></span>
            <el-row>
              <el-col :span="11">
                <el-select style="width:100%" v-model="checkPartition.time_table" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode  || actionMode==='view'">
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
                  <el-select style="width:100%" v-model="checkPartition.time_column" :placeholder="$t('kylinLang.common.pleaseSelect')" @change="changeTimeColumn" v-show="hasSeparate" :disabled="editMode  || actionMode==='view'">
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
           <el-form-item :label="$t('timeFormat')" v-show="hasSeparate" prop="partition_time_format">
          <!--   <el-select v-model="checkPartition.partition_time_format" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode  || actionMode==='view'">
              <el-option
                v-for="item in timeFormat"
                :key="item.label"
                :label="item.label"
                :value="item.label">
              </el-option>
            </el-select> -->
            <el-row>
              <el-autocomplete style="width:100%" :disabled="editMode  || actionMode==='view'"  @select="formValid('partition_time_format')"
                      class="inline-input"
                      v-model="checkPartition.partition_time_format"
                      :fetch-suggestions="querySearchForTime"
                      :placeholder="$t('kylinLang.common.pleaseSelect')"
              ></el-autocomplete>
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
      </el-col>
      <el-col :span="layout.right" v-if="layout.right" style="padding:20px 20px 20px 0 ; margin-top: -30px;">
       <div class="col-line"></div>
       <p class="ksd-ml-20">{{$t('kylinLang.model.checkData')}} <span @click="closeSample" class="el-icon el-icon-close" style="font-size: 12px;cursor: pointer; float: right;color:grey"></span></p>
       <el-alert class="ksd-mt-20 ksd-ml-20 trans" v-if="modelStatics && modelStatics.length <= 1"
        :title="$t('noSample')"
        show-icon
        :closable="false"
        type="warning">
      </el-alert>
       <el-table class="ksd-ml-20" v-if="modelStatics && modelStatics.length>1"
          :data="modelStatics.slice(1)"
          border>
          <el-table-column v-for="(val,index) in modelStatics[0]" :key="index"
            :prop="''+index"
            :width="index === 0 ? 60 : ''"
            :label="modelStatics[0][index]">
          </el-table-column>
        </el-table>
      </el-col>
    </el-row>
	</div>
</template>
<script>
import { mapActions } from 'vuex'
import { changeDataAxis } from '../../util/index'
import { handleSuccess, handleError } from '../../util/business'
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
      statistics: [],
      layout: {
        left: 24,
        right: 0
      },
      rule: {
        partition_date_format: [
          {validator: this.dateFormatCheck, trigger: 'blur'}
        ],
        partition_time_format: [
          {validator: this.timeFormatCheck, trigger: 'blur'}
        ]
      }
    }
  },
  props: ['modelInfo', 'actionMode', 'editLock', 'columnsForTime', 'columnsForDate', 'partitionSelect', 'comHeight'],
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
    dateFormatCheck (rule, value, callback) {
      if (!this.checkPartition.partition_date_format || this.modelStatics.length === 0) {
        callback()
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
    timeFormatCheck (rule, value, callback) {
      if (!this.checkPartition.partition_time_format || this.modelStatics.length === 0) {
        callback()
      }
      var project = this.modelInfo.project || this.project
      var databaseAndTableName = this.getFullTableNameInfo(this.checkPartition.time_table)
      this.validColumnFormat({project: project, table: databaseAndTableName.join('.'), column: this.checkPartition.time_column, format: this.checkPartition.partition_time_format}).then((res) => {
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
    querySearchForTime (queryString, cb) {
      var result = queryString ? this.timeFormat.filter((formatStr) => {
        return formatStr.value.indexOf(queryString) === 0
      }) : this.timeFormat
      cb(result)
    },
    changeDateColumn (val) {
      if (val === this.checkPartition.time_column && !this.modelInfo.uuid) {
        this.$set(this.checkPartition, 'time_column', null)
        this.$set(this.checkPartition, 'time_format', null)
      }
      this.needSetTime = true
      this.checkLockFormat()
      var databaseAndTableName = this.getFullTableNameInfo(this.checkPartition.date_table)
      if (databaseAndTableName.length) {
        this.loadTableStatics(databaseAndTableName[0], databaseAndTableName[1], this.checkPartition.date_column)
      }
      this.formValid('partition_date_format')
    },
    changeTimeColumn () {
      var databaseAndTableName = this.getFullTableNameInfo(this.checkPartition.time_table)
      if (databaseAndTableName.length) {
        this.loadTableStatics(databaseAndTableName[0], databaseAndTableName[1], this.checkPartition.time_column)
      }
      this.formValid('partition_time_format')
    },
    checkLockFormat () {
      // for (var i in this.columnsForDate) {
      //   if (i === this.checkPartition.date_table) {
      //     for (var s = 0; s < this.columnsForDate[i].length; s++) {
      //       if (this.columnsForDate[i][s].name === this.checkPartition.date_column) {
      //         if (!this.columnsForDate[i][s].isFormat) {
      //           this.needSetTime = false
      //           this.$set(this.checkPartition, 'partition_date_format', 'yyyyMMdd')
      //           this.$set(this.checkPartition, 'time_format', null)
      //           this.$set(this.checkPartition, 'time_column', null)
      //           this.hasSeparate = false
      //         }
      //       }
      //     }
      //   }
      // }
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
          var basicColumn = [['ID', columnName]]
          var sampleData = changeDataAxis([data], true)
          this.modelStatics = basicColumn.concat(sampleData)
        })
      }, () => {
        this.layout.left = 14
        this.layout.right = 10
        this.modelStatics = []
      })
    },
    changeSepatate (val) {
      if (!val && !this.modelInfo.uuid) {
        this.$set(this.checkPartition, 'time_column', '')
        this.$set(this.checkPartition, 'time_format', '')
      }
    }
  },
  computed: {
    editMode () {
      return this.editLock
    },
    currentModelInfo () {
      return this.modelInfo
    },
    dateColumns () {
      for (let k in this.columnsForDate) {
        if (k) {
          this.columnsForDate[''] = []
          return this.columnsForDate || []
        }
      }
      this.$set(this.checkPartition, 'date_table', '')
      this.$set(this.checkPartition, 'date_column', '')
      this.$set(this.checkPartition, 'partition_date_format', '')
      this.$set(this.checkPartition, 'time_table', null)
      this.$set(this.checkPartition, 'time_column', null)
      this.$set(this.checkPartition, 'partition_time_format', null)
      this.closeSample()
    },
    timeColumns () {
      this.columnsForTime[''] = []
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
      if (this.checkPartition.time_table === '') {
        this.checkPartition.time_column = ''
      }
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
    }
  },
  created () {
  },
  mounted () {
    this.hasSeparate = !!(this.checkPartition && this.checkPartition.time_column)
    this.checkLockFormat()
  },
  locales: {
    'en': {partitionDateColumn: 'Partition Date Column', dateFormat: 'Date Format', hasSeparateLabel: 'More partition column', partitionTimeColumn: 'Partition Time Column', timeFormat: 'Time Format', filterCondition: 'Filter Condition', filterPlaceHolder: 'The filter condition, no clause "WHERE" needed, eg: Date>YYYY-MM-DD.', noSample: 'Executing data format check depends on table sampling result.', validFail: 'The date format is invalid for this column. Please try another partition column or date format.'},
    'zh-cn': {partitionDateColumn: '分区列（日期类型）', dateFormat: '日期格式', hasSeparateLabel: '更多分区列', partitionTimeColumn: '分区列（时间类型）', timeFormat: '时间格式', filterCondition: '过滤条件', filterPlaceHolder: '请输入过滤条件，不需要写“WHERE”，例如：Date>YYYY-MM-DD。', noSample: '当表具有采样结果时，才能对其分区列的格式进行检查。', validFail: '分区列格式不正确，请重新选择分区格式或更换分区列。'}
  }
}
</script>
<style lang="less">
.partitionBox {
  // overflow-y: auto;
  .el-form-item{
    margin-bottom: 12px;
  }
  .el-form {
    // height: 260px;
    // overflow: hidden;
  }
  .col-line{
    width: 1px;
    height: 100%;
    background-color: rgb(41, 43, 56);
    position: absolute;
    bottom:-51px;
    top:-82px;
    margin-top: 52px;
  }
  .el-input{
    padding: 0;
  }
}
</style>
