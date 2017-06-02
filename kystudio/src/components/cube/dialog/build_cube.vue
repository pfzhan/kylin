<template>
  <el-form :model="timeZone" label-position="right" label-width="280px" :rules="rules" ref="buildCubeForm">
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
export default {
  name: 'build_cube',
  props: ['cubeDesc'],
  data () {
    return {
      timeZone: {
        startDate: new Date((this.cubeDesc.segments[this.cubeDesc.segments.length - 1]) ? this.cubeDesc.segments[this.cubeDesc.segments.length - 1].last_build_time : this.cubeDesc.partitionDateStart),
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
      let endTime = this.timeZone.endDate.getTime()
      let startTime = value.getTime()
      if (startTime === null) {
        callback(new Error(this.$t('selectDate')))
      } else if (endTime < startTime) {
        callback(new Error(this.$t('timeCompare')))
      } else {
        callback()
      }
    },
    validateEndDate: function (rule, value, callback) {
      let endTime = value.getTime()
      let startTime = this.timeZone.startDate.getTime()
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
          _this.$emit('validSuccess', {start: this.timeZone.startDate.getTime(), end: this.timeZone.endDate.getTime()})
        }
      })
    })
  },

  watch: {
    cubeDesc (cubeDesc) {
      this.timeZone.startDate = new Date((this.cubeDesc.segments[this.cubeDesc.segments.length - 1]) ? this.cubeDesc.segments[this.cubeDesc.segments.length - 1].last_build_time : this.cubeDesc.partitionDateStart)
      this.timeZone.endDate = null
    }
  },

  locales: {
    'en': {partitionDateColumn: 'PARTITION DATE COLUMN', startDate: 'Start Date (Include)', endDate: 'End Date (Exclude)', selectDate: 'Please select the date.', timeCompare: 'End time should be later than the start time.'},
    'zh-cn': {partitionDateColumn: '分区日期列', startDate: '起始日期 (包含)', endDate: '结束日期 (不包含)', selectDate: '请选择时间', timeCompare: '结束日期应晚于起始时间'}
  }
}
</script>
<style scoped="">

</style>
