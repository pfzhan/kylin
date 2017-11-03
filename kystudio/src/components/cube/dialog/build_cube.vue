<template>
  <el-form id="build-cube" :model="timeZone" label-position="top" :rules="rules" ref="buildCubeForm">

    <div v-if="cubeDesc.multilevel_partition_cols.length > 0">
      <el-form-item :label="$t('primaryPartitionColumn')" >
        <el-tag>{{cubeDesc.multilevel_partition_cols[0]}}</el-tag>
      </el-form-item>
      <el-form-item :label="$t('partitionValues')" prop="mpValues">
        <el-autocomplete
          class="inline-input"
          :props="{label: 'name', value: 'name'}"
          v-model="timeZone.mpValues"
          :fetch-suggestions="querySearch"
          :trigger-on-focus="false"
          @select="handleSelect"
        ></el-autocomplete>
      </el-form-item>
      <div class="line-primary"></div>
    </div>

    <el-form-item :label="$t('partitionDateColumn')" >
      <el-tag>{{cubeDesc.partitionDateColumn}}</el-tag>
    </el-form-item>
    <el-form-item :label="$t('startDate')" prop="startDate" class="is-required">
      <el-date-picker :clearable="false" ref="startDateInput"
                      v-model="timeZone.startDate" type="datetime">
      </el-date-picker>
    </el-form-item>
    <el-form-item :label="$t('endDate')" prop="endDate" class="is-required">
      <el-date-picker :clearable="false" ref="endDateInput"
        v-model="timeZone.endDate"
        type="datetime">
      </el-date-picker>
    </el-form-item>
  </el-form>
</template>
<script>
import { mapActions } from 'vuex'
import { transToUtcTimeFormat, transToUTCMs, handleSuccess, handleError } from '../../../util/business'
export default {
  name: 'build_cube',
  props: ['cubeDesc', 'formVisible'],
  data () {
    return {
      mpValuesList: [],
      timeZone: {
        startDate: transToUtcTimeFormat(this.cubeDesc.segments[this.cubeDesc.segments.length - 1] ? this.cubeDesc.segments[this.cubeDesc.segments.length - 1].date_range_end : this.cubeDesc.partitionDateStart, true),
        endDate: null,
        mpValues: ''
      },
      rules: {
        startDate: [
          { validator: this.validateStartDate, trigger: 'blur' }
        ],
        endDate: [
          { validator: this.validateEndDate, trigger: 'blur' }
        ],
        mpValues: [
          { required: true, message: this.$t('partitionNull'), trigger: 'blur' }
        ]
      }
    }
  },
  methods: {
    ...mapActions({
      getMPValues: 'GET_MP_VALUES'
    }),
    querySearch: function (queryString, cb) {
      let mpValuesList = this.mpValuesList
      let results = []
      if (mpValuesList.length > 0) {
        results = queryString ? mpValuesList.filter(this.createFilter(queryString)) : mpValuesList
      }
      // 调用 callback 返回建议列表的数据
      cb(results)
    },
    createFilter: function (queryString) {
      return (mpValue) => {
        return (mpValue.name.indexOf(queryString) === 0)
      }
    },
    handleSelect: function (item) {
      let mpIndex = this.mpValuesList.indexOf(item)
      let segLength = this.mpValuesList[mpIndex].cube.segments.length
      if (segLength > 0) {
        this.timeZone.startDate = transToUtcTimeFormat(this.mpValuesList[mpIndex].cube.segments[segLength - 1].date_range_end, true)
        console.log(this.timeZone.startDate)
      }
    },
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
        this.$refs['buildCubeForm'].validateField('endDate')
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
    if (this.cubeDesc.multilevel_partition_cols.length > 0) {
      this.getMPValues(this.cubeDesc.name).then((res) => {
        handleSuccess(res, (data) => {
          this.mpValuesList = data
        })
      }, (res) => {
        handleError(res)
      })
    }
    this.$on('buildCubeFormValid', (t) => {
      this.$refs['buildCubeForm'].validate((valid) => {
        if (valid) {
          this.$emit('validSuccess', {start: transToUTCMs(this.timeZone.startDate), end: transToUTCMs(this.timeZone.endDate), mpValues: this.timeZone.mpValues})
        }
      })
    })
    this.$on('resetBuildCubeForm', (t) => {
      this.$refs['buildCubeForm'].resetFields()
    })
  },

  watch: {
    formVisible (formVisible) {
      if (formVisible) {
        this.timeZone.startDate = transToUtcTimeFormat(this.cubeDesc.segments[this.cubeDesc.segments.length - 1] ? this.cubeDesc.segments[this.cubeDesc.segments.length - 1].date_range_end : this.cubeDesc.partitionDateStart, true)
        this.timeZone.endDate = null
        if (this.cubeDesc.multilevel_partition_cols.length > 0) {
          this.getMPValues(this.cubeDesc.name).then((res) => {
            handleSuccess(res, (data) => {
              this.mpValuesList = data
            })
          }, (res) => {
            handleError(res)
          })
        }
      }
    }
  },

  locales: {
    'en': {partitionDateColumn: 'Time Partition Column', startDate: 'Start Time (Include)', endDate: 'End Time (Exclude)', selectDate: 'Please select the time.', legalDate: 'Please enter a complete time formatted as YYYY-MM-DD.', timeCompare: 'End time should be later than the start time.', partitionValues: 'Partition Value', partitionNull: 'Please input partition value.', primaryPartitionColumn: 'Primary Partition Column'},
    'zh-cn': {partitionDateColumn: '时间分区列', startDate: '起始时间（包含）', endDate: '结束时间（不包含）', selectDate: '请选择时间', legalDate: '请输入完整时间，格式为YYYY-MM-DD', timeCompare: '结束时间应晚于起始时间', partitionValues: '分区值', partitionNull: '请输入分区值。', primaryPartitionColumn: '一级分区列'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  #build-cube{
    .line-primary {
      margin: 20px -20px 0px -20px;
    }
    .el-form-item__label{
      float: left!important;
    }
    .el-form-item{
      display: grid;
      margin: 15px 0px 0px 0px;
      .el-form-item__error {
        position: relative;
        top: 0;
      }
      .el-form-item__label {
        padding: 0 0 5px;
      }
      span {
        width: 100%;
        height: 36px;
        line-height: 36px;
        padding: 0px 10px 0px 10px;
      }
      .el-autocomplete {
        width: 100%;
      }
    }
    .el-date-editor{
      height: 36px;
      padding: 0;
    }
    .el-input {
      margin-left: 0px;
      width: 100%;
    }
  }
</style>
