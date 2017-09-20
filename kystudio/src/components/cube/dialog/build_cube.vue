<template>
  <el-form id="build-cube" :model="timeZone" label-position="right" label-width="280px" :rules="rules" ref="buildCubeForm">
    <el-form-item :label="$t('partitionDateColumn')" >
      <el-tag>{{cubeDesc.partitionDateColumn}}</el-tag>
    </el-form-item>
    <el-form-item :label="$t('startDate')" prop="startDate">
      <el-date-picker :clearable="false" ref="startDateInput"
                      v-model="timeZone.startDate"
        type="datetime">
      </el-date-picker>
    </el-form-item>
    <el-form-item :label="$t('endDate')" prop="endDate">
      <el-date-picker :clearable="false" ref="endDateInput"
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
        startDate: transToUtcTimeFormat(this.cubeDesc.segments[this.cubeDesc.segments.length - 1] ? this.cubeDesc.segments[this.cubeDesc.segments.length - 1].date_range_end : this.cubeDesc.partitionDateStart),
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
      let realValue = this.$refs['startDateInput'].$el.querySelectorAll('.el-input__inner')[0].value
      realValue = realValue.replace(/(^\s*)|(\s*$)/g, '')
      if (realValue) {
        let reg = /^[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])(\s+[0-9]\d:[0-9]\d:[0-9]\d)?$/
        let regExp = new RegExp(reg)
        let isLegalDate = regExp.test(realValue)

        if (isLegalDate) {
          let regDate = /^[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])$/
          let regExpDate = new RegExp(regDate)
          let isLegalDateFormat = regExpDate.test(realValue)
          if (isLegalDateFormat) {
            realValue = realValue + ' 00:00:00'
            this.timeZone.startDate = new Date()
            this.timeZone.startDate = new Date(realValue)
            this.$nextTick(() => {
              this.$refs['startDateInput'].$el.querySelectorAll('.el-input__inner')[0].value = realValue
            })
          }
        } else {
          callback(new Error(this.$t('legalDate')))
        }
      }
      let endTime = (new Date(this.timeZone.endDate)).getTime()
      let startTime = (new Date(this.timeZone.startDate)).getTime()
      if (startTime === null || startTime === undefined || startTime === '' || isNaN(startTime)) {
        callback(new Error(this.$t('selectDate')))
      } else if (endTime <= startTime) {
        // callback(new Error(this.$t('timeCompare')))
      } else {
        callback()
      }
    },
    validateEndDate: function (rule, value, callback) {
      let realValue = this.$refs['endDateInput'].$el.querySelectorAll('.el-input__inner')[0].value
      realValue = realValue.replace(/(^\s*)|(\s*$)/g, '')
      if (realValue) {
        let reg = /^[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])(\s+[0-9]\d:[0-9]\d:[0-9]\d)?$/
        let regExp = new RegExp(reg)
        let isLegalDate = regExp.test(realValue)

        if (isLegalDate) {
          let regDate = /^[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])$/
          let regExpDate = new RegExp(regDate)
          let isLegalDateFormat = regExpDate.test(realValue)
          if (isLegalDateFormat) {
            realValue = realValue + ' 00:00:00'
            this.timeZone.endDate = new Date()
            this.timeZone.endDate = new Date(realValue)
            this.$nextTick(() => {
              this.$refs['endDateInput'].$el.querySelectorAll('.el-input__inner')[0].value = realValue
            })
          }
        } else {
          callback(new Error(this.$t('legalDate')))
        }
      }

      let endTime = (new Date(this.timeZone.endDate)).getTime()
      let startTime = (new Date(this.timeZone.startDate)).getTime()
      if (endTime === null || endTime === undefined || endTime === '' || isNaN(endTime)) {
        callback(new Error(this.$t('selectDate')))
      } else if (endTime <= startTime) {
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
      this.timeZone.startDate = transToUtcTimeFormat(this.cubeDesc.segments[this.cubeDesc.segments.length - 1] ? this.cubeDesc.segments[this.cubeDesc.segments.length - 1].date_range_end : this.cubeDesc.partitionDateStart) // new Date((this.cubeDesc.segments[this.cubeDesc.segments.length - 1]) ? this.cubeDesc.segments[this.cubeDesc.segments.length - 1].last_build_time : this.cubeDesc.partitionDateStart)
      this.timeZone.endDate = null
    }
  },

  locales: {
    'en': {partitionDateColumn: 'PARTITION DATE COLUMN', startDate: 'Start Date (Include)', endDate: 'End Date (Exclude)', selectDate: 'Please select the date.', legalDate: 'Please enter a complete date.', timeCompare: 'End time should be later than the start time.'},
    'zh-cn': {partitionDateColumn: '分区日期列', startDate: '起始日期 (包含)', endDate: '结束日期 (不包含)', selectDate: '请选择时间', legalDate: '请输入完整日期。', timeCompare: '结束日期应晚于起始时间'}
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
