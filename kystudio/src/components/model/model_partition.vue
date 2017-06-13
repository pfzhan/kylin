<template>
	<div style="overflow:hidden" class="partitionBox">
      <el-form  label-width="240px">
        <el-form-item :label="$t('partitionDateColumn')">
          <el-col :span="11">
                 <el-select v-model="checkPartition.date_table" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode || actionMode==='view'">
                  <el-option
                    v-for="(key,value) in dateColumns"
                    :key="key"
                    :label="value"
                    :value="value">
                  </el-option>
                </el-select>
          </el-col>
          <el-col :span="2"></el-col>
          <el-col class="line" :span="11">
             <el-select v-model="checkPartition.date_column" @change="changeDateColumn" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode  || actionMode==='view'">
                  <el-option
                    v-for="item in dateColumnsByTable"
                    :key="item.name"
                    :label="item.name"
                    :value="item.name">
                  </el-option>
                </el-select>
          </el-col>
        </el-form-item>
        <el-form-item :label="$t('dateFormat')">
          <el-select v-model="checkPartition.partition_date_format" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="!needSetTime || editMode  || actionMode==='view'">
            <el-option
              v-for="item in dateFormat"
              :key="item.label"
              :label="item.label"
              :value="item.label">
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('hasSeparateLabel')" v-show="needSetTime">
        <el-switch v-model="hasSeparate" on-text="" @change="changeSepatate" off-text="" :disabled="editMode  || actionMode==='view'"></el-switch>
        </el-form-item>
        <el-form-item :label="$t('partitionTimeColumn')" v-show="hasSeparate">
        <el-col :span="11">
          <el-select v-model="checkPartition.time_table" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode  || actionMode==='view'">
            <el-option
              v-for="(key,value) in timeColumns"
              :key="key"
              :label="value"
              :value="value">
            </el-option>
          </el-select>
          </el-col>
          <el-col :span="2"></el-col>
          <el-col :span="11">
            <el-select v-model="checkPartition.time_column" :placeholder="$t('kylinLang.common.pleaseSelect')" v-show="hasSeparate" :disabled="editMode  || actionMode==='view'">
              <el-option
                v-for="item in timeColumnsByTable"
                :key="item.name"
                :label="item.name"
                :value="item.name">
              </el-option>
          </el-select>
          </el-col>
        </el-form-item>
         <el-form-item :label="$t('timeFormat')" v-show="hasSeparate">
          <el-select v-model="checkPartition.partition_time_format" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="editMode  || actionMode==='view'">
            <el-option
              v-for="item in timeFormat"
              :key="item.label"
              :label="item.label"
              :value="item.label">
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>
	</div>
</template>
<script>
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
  props: ['modelInfo', 'actionMode', 'editLock', 'columnsForTime', 'columnsForDate', 'partitionSelect'],
  methods: {
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
      this.columnsForDate[''] = []
      return this.columnsForDate || []
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
  },
  locales: {
    'en': {partitionDateColumn: 'Partition Date Column', dateFormat: 'Date Format', hasSeparateLabel: 'Has a separate "time of the day" column?', partitionTimeColumn: 'Partition Time Column', timeFormat: 'Time Format'},
    'zh-cn': {partitionDateColumn: '分区列（日期类型）', dateFormat: '日期格式', hasSeparateLabel: '您使用单独的列来表示某天内的时间吗？', partitionTimeColumn: '分区列（时间类型）', timeFormat: '时间格式'}
  }
}
</script>
<style lang="less" scoped>
.partitionBox {
  .el-form {
    height: auto;
    overflow: hidden;
  }
}
</style>
