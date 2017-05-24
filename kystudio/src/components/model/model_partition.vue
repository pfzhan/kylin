<template>
	<div style="overflow:hidden" class="partitionBox">
      <el-form  label-width="240px">
        <el-form-item label="Partition Date Column">
          <el-col :span="11">
                 <el-select v-model="checkPartition.date_table" placeholder="请选择" :disabled="editMode || actionMode==='view'">
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
             <el-select v-model="checkPartition.date_column" @change="changeDateColumn" placeholder="请选择" :disabled="editMode  || actionMode==='view'">
                  <el-option
                    v-for="item in dateColumnsByTable"
                    :key="item.name"
                    :label="item.name"
                    :value="item.name">
                  </el-option>
                </el-select>
          </el-col>
        </el-form-item>
        <el-form-item label="Date Format">
          <el-select v-model="checkPartition.partition_date_format" placeholder="请选择" :disabled="!needSetTime || editMode  || actionMode==='view'">
            <el-option
              v-for="item in dateFormat"
              :key="item.label"
              :label="item.label"
              :value="item.label">
            </el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="separate time of the day column " v-show="needSetTime">
        <el-switch v-model="hasSepatate" on-text="" @change="changeSepatate" off-text="" :disabled="editMode  || actionMode==='view'"></el-switch>
        </el-form-item>
        <el-form-item label="Partition Time Column" v-show="hasSepatate">
        <el-col :span="11">
          <el-select v-model="checkPartition.time_table" placeholder="请选择" :disabled="editMode  || actionMode==='view'">
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
            <el-select v-model="checkPartition.time_column" placeholder="请选择" v-show="hasSepatate" :disabled="editMode  || actionMode==='view'">
              <el-option
                v-for="item in timeColumnsByTable"
                :key="item.name"
                :label="item.name"
                :value="item.name">
              </el-option>
          </el-select>
          </el-col>
        </el-form-item>
         <el-form-item label="Time Format" v-show="hasSepatate">
          <el-select v-model="checkPartition.partition_time_format" placeholder="请选择" :disabled="editMode  || actionMode==='view'">
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
      hasSepatate: false,
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
                this.hasSepatate = false
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
  watch: {
    'partitionSelect.partition_time_column' (val) {
      if (val) {
        this.hasSepatate = true
      } else {
        this.hasSepatate = false
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
    }
  },
  created () {
  },
  mounted () {
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
