<template>
  <el-form id="build-cube" :model="timeZone" label-position="right" label-width="280px" :rules="rules" ref="buildCubeForm">
    <el-form-item :label="$t('partitionDateColumn')" >
      <el-tag>{{cubeDesc.partitionDateColumn}}</el-tag>
    </el-form-item>
    <el-form-item :label="$t('startDate')" prop="startDate">
      <el-date-picker
        v-model="timeZone.startDate"
        type="datetime">
      </el-date-picker>
    </el-form-item>   
    <el-form-item :label="$t('endDate')" prop="endDate">
      <el-date-picker
        v-model="timeZone.endDate"
        type="datetime">
      </el-date-picker>    
    </el-form-item>          
  </el-form>
</template>
<script>
import { transToUtcTimeFormat, transToUTCMs } from '../../../util/business'
export default {
  name: 'build_cube',
  props: ['cubeDesc'],
  data () {
    return {
      timeZone: {
        startDate: transToUtcTimeFormat((this.cubeDesc.segments[this.cubeDesc.segments.length - 1]) ? this.cubeDesc.segments[this.cubeDesc.segments.length - 1].date_range_end : this.cubeDesc.partitionDateStart),
        endDate: null
      },
      rules: {
        startDate: [
          { validator: this.validateStartDate, trigger: 'blur' }
        ],
        endDate: [
          { validator: this.validateEndDate, trigger: 'blur' }
        ]
      }
    }
  },
  methods: {
    validateStartDate: function (rule, value, callback) {
      let endTime = (new Date(this.timeZone.endDate)).getTime()
      let startTime = (new Date(value)).getTime()
      if (startTime === null) {
        callback(new Error(this.$t('selectDate')))
      } else if (endTime < startTime) {
        callback(new Error(this.$t('timeCompare')))
      } else {
        callback()
      }
    },
    validateEndDate: function (rule, value, callback) {
      let endTime = (new Date(value)).getTime()
      let startTime = (new Date(this.timeZone.startDate)).getTime()
      if (endTime === null) {
        callback(new Error(this.$t('selectDate')))
      } else if (endTime < startTime) {
        callback(new Error(this.$t('timeCompare')))
      } else {
        callback()
      }
    }
  },
  created () {
    let _this = this
    this.$on('buildCubeFormValid', (t) => {
      _this.$refs['buildCubeForm'].validate((valid) => {
        if (valid) {
          _this.$emit('validSuccess', {start: transToUTCMs(this.timeZone.startDate), end: transToUTCMs(this.timeZone.endDate)})
        }
      })
    })
  },

  watch: {
    cubeDesc (cubeDesc) {
      this.timeZone.startDate = transToUtcTimeFormat(0) // new Date((this.cubeDesc.segments[this.cubeDesc.segments.length - 1]) ? this.cubeDesc.segments[this.cubeDesc.segments.length - 1].last_build_time : this.cubeDesc.partitionDateStart)
      this.timeZone.endDate = null
    }
  },

  locales: {
    'en': {partitionDateColumn: 'PARTITION DATE COLUMN', startDate: 'Start Date (Include)', endDate: 'End Date (Exclude)', selectDate: 'Please select the date.', timeCompare: 'End time should be later than the start time.'},
    'zh-cn': {partitionDateColumn: '分区日期列', startDate: '起始日期 (包含)', endDate: '结束日期 (不包含)', selectDate: '请选择时间', timeCompare: '结束日期应晚于起始时间'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  #build-cube{
    margin-top: 22px;
    .el-form-item__label{
      float: left!important;
    }
    .el-form-item{
      height: 50px;
      marigin-bottom: 0!important;
      margin-top: -22px;
    }
    .el-date-editor{
      height: 36px;
      padding: 0;
    }
  }
</style>
