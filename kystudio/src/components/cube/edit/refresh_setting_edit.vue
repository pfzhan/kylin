<template>
<div class="refresh-setting-edit ksd-mrl-20">
  <p class="ksd-mt-10 ksd-mb-20 refresh-title ksd-fs-16">
    <span>{{$t('kylinLang.cube.merge')}}
      <common-tip :content="$t('kylinLang.cube.refreshSetTip')" >
        <i class="el-icon-question ksd-fs-10"></i>
      </common-tip>
    </span>
  </p>
  <el-row class="ksd-mb-10 ksd-lineheight-32" :gutter="10">
    <el-col :span="4">
      <span class="ksd-fright">{{$t('autoMergeThresholds')}}</span>
    </el-col>
    <el-col :span="5">
      <el-button size="medium" type="primary" plain icon="el-icon-plus" @click.native="addNewTimeRange">{{$t('newThresholds')}} </el-button>
    </el-col>
  </el-row>
  <el-row :gutter="10" class="ksd-mb-10" v-for="(timeRange, index) in timeRanges" :key="index">
    <el-col :span="4" :offset="4" class="left-threshold">
      <el-input size="medium" v-model="timeRange.range" v-if="timeRange.type === 'days'" @change="changeTimeRange(timeRange, index)"></el-input>
      <el-select size="medium"  v-model="timeRange.range" v-else @change="changeTimeRange(timeRange, index)">
        <el-option
           v-for="(item, time_index) in timeOptions"
           :key="time_index"
           :label="item"
           :value="item">
        </el-option>
      </el-select>
    </el-col>
    <el-col :span="16">
      <el-select size="medium"  v-model="timeRange.type" @change="changeTimeRange(timeRange, index)" placeholder="">
        <el-option
           v-for="(item, range_index) in rangesOptions"
          :key="range_index"
          :label="item"
          :value="item">
        </el-option>
      </el-select>
      <el-button class="ksd-ml-16" size="medium" icon="el-icon-delete" @click.native="removeTimeRange(index)"></el-button>
    </el-col>
  </el-row>

  <el-row class="ksd-mt-20 ksd-lineheight-28" :gutter="10">
    <el-col :span="4" class="ksd-right">{{$t('retentionThreshold')}} </el-col>
    <el-col :span="4">
      <el-input size="medium" v-model="retentionRange" @change="changeRetentionRange"></el-input>
    </el-col>
    <el-col :span="1" class="ksd-left">
       {{$t('days')}}
    </el-col>
  </el-row>
  <el-row class="ksd-mtb-20 ksd-lineheight-28" :gutter="10">
    <el-col :span="4" class="ksd-right">{{$t('partitionStartDate')}} </el-col>
    <el-col :span="5">
      <el-date-picker size="medium" v-if="!isInteger" @change="changePartitionDateStart"
        v-model="partitionStartDate"
        type="datetime"
        align="right">
      </el-date-picker>
      <el-input size="medium" v-else v-model="cubeDesc.partition_date_start" class="input_width"></el-input>
    </el-col>
  </el-row>


  <div v-if="!isInteger">
    <div class="ky-line"></div>
    <p class="ksd-mtb-20 refresh-title ksd-fs-16">
      <span  class="ksd-mr-10">{{$t('kylinLang.cube.scheduler')}}
        <common-tip :content="$t('kylinLang.cube.schedulerTip')" >
          <i class="el-icon-question ksd-fs-10"></i>
        </common-tip>
      </span>
      <el-switch
        v-model="$store.state.cube.cubeSchedulerIsSetting"
        active-text="OFF"
        inactive-text="ON">
      </el-switch>
    </p>
    <el-row class="ksd-mb-20 ksd-lineheight-32" :gutter="10">
      <el-col :span="4" class="ksd-right ksd-lineheight-32">{{$t('buildTrigger')}} </el-col>
      <el-col :span="5">
        <el-date-picker size="medium" @change="changeTriggerTime()" :disabled="!$store.state.cube.cubeSchedulerIsSetting"
          v-model="scheduledRunTime" :placeholder="$t('kylinLang.common.pleaseSelect')"
          type="datetime"
          align="right">
        </el-date-picker>
      </el-col>
    </el-row>
    <el-row :gutter="10" class="ksd-lineheight-32">
      <el-col :span="4" class="ksd-right">{{$t('periddicalInterval')}} </el-col>
      <el-col :span="3">
        <el-input size="medium" v-model="intervalRange.range" :disabled="!$store.state.cube.cubeSchedulerIsSetting"  @change="changeInterval()"></el-input>
      </el-col>
      <el-col :span="3">
        <el-select size="medium" :placeholder="$t('kylinLang.common.pleaseSelect')" v-model="intervalRange.type" @change="changeInterval()" :disabled="!$store.state.cube.cubeSchedulerIsSetting">
          <el-option v-for="(item, range_index) in intervalOptions" 
            :key="range_index"
            :label="item"
            :value="item">
          </el-option>
        </el-select>
      </el-col>
    </el-row>
  </div>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, transToUtcTimeFormat, transToUTCMs, msTransDate } from '../../../util/business'
import { IntegerType } from '../../../config'
export default {
  name: 'refreshSetting',
  props: ['cubeDesc', 'scheduler', 'modelDesc'],
  data () {
    return {
      timeRanges: [],
      intervalRange: {range: '', type: ''},
      timeOptions: [0.5, 1, 2, 4, 8],
      rangesOptions: ['days', 'hours', 'minutes', ''],
      intervalOptions: ['weeks', 'days', 'hours', 'minutes'],
      selected_project: localStorage.getItem('selected_project'),
      partitionStartDate: 0,
      scheduledRunTime: 0,
      retentionRange: 0,
      isSetScheduler: false,
      pickerOptionsEnd: {
        disabledDate: (time) => {
          let nowDate = new Date()
          var result = time.getTime() - nowDate.getTime()
          return result
        }
      }
    }
  },
  methods: {
    ...mapActions({
      getScheduler: 'GET_SCHEDULER'
    }),
    conversionTime: function () {
      let _this = this
      _this.cubeDesc.auto_merge_time_ranges.forEach(function (item) {
        var transDate = {
          value: '',
          type: ''
        }
        if (_this.isInteger) {
          transDate.value = item
        } else {
          transDate = msTransDate(item, true)
        }
        let rangeObj = {
          type: transDate.type,
          range: transDate.value,
          mills: item
        }
        _this.timeRanges.push(rangeObj)
      })
    },
    removeTimeRange: function (index) {
      this.timeRanges.splice(index, 1)
      this.cubeDesc.auto_merge_time_ranges.splice(index, 1)
    },
    initRepeatInterval: function (data) {
      var transDate = msTransDate(data.partition_interval)
      this.intervalRange.type = transDate.type
      this.intervalRange.range = transDate.value
    },
    changePartitionDateStart: function () {
      this.cubeDesc.partition_date_start = transToUTCMs(this.partitionStartDate)
    },
    changeTriggerTime: function () {
      this.scheduler.desc.scheduled_run_time = transToUTCMs(this.scheduledRunTime)
    },
    changeTimeRange: function (timeRange, index) {
      let time = 0
      if (timeRange.type === 'minutes') {
        time = timeRange.range * 60000
      } else if (timeRange.type === 'hours') {
        time = timeRange.range * 3600000
      } else if (timeRange.type === 'days') {
        time = timeRange.range * 86400000
      } else {
        time = timeRange.range * 1
      }
      this.cubeDesc.auto_merge_time_ranges[index] = time
    },
    changeRetentionRange (retentionRange) {
      let time = 0
      time = retentionRange * 86400000
      this.cubeDesc.retention_range = time
    },
    initScheduler: function () {
      this.getScheduler({cubeName: this.cubeDesc.name, project: this.selected_project}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          var schedulerData = this.cubeDesc.is_draft ? data.draft : data.schedulerJob
          if (schedulerData && schedulerData.enabled) {
            this.initRepeatInterval(schedulerData)
            this.scheduler.desc.scheduled_run_time = schedulerData.scheduled_run_time
            this.scheduledRunTime = transToUtcTimeFormat(this.scheduler.desc.scheduled_run_time, true)
            this.scheduler.desc.partition_interval = schedulerData.partition_interval
          }
        })
      }).catch((res) => {
        handleError(res, () => {
        })
      })
    },
    changeInterval: function () {
      let time = 0
      if (this.intervalRange.type === 'minutes') {
        time = this.intervalRange.range * 60000
      }
      if (this.intervalRange.type === 'hours') {
        time = this.intervalRange.range * 3600000
      }
      if (this.intervalRange.type === 'days') {
        time = this.intervalRange.range * 86400000
      }
      if (this.intervalRange.type === 'weeks') {
        time = this.intervalRange.range * 604800000
      }
      this.scheduler.desc.partition_interval = time
    },
    addNewTimeRange: function () {
      this.timeRanges.unshift({type: 'days', range: 0, mills: 0})
      if (!this.cubeDesc.auto_merge_time_ranges) {
        this.cubeDesc.auto_merge_time_ranges = []
      }
      this.cubeDesc.auto_merge_time_ranges.unshift(0)
    }
  },
  computed: {
    isInteger () {
      let datatype = this.modelDesc.partition_desc.partition_date_column && this.modelDesc.columnsDetail[this.modelDesc.partition_desc.partition_date_column].datatype
      if (this.modelDesc.partition_desc.partition_date_format || IntegerType.indexOf(datatype) < 0) {
        return false
      } else {
        return true
      }
    }
  },
  created: function () {
    this.retentionRange = this.cubeDesc.retention_range / 86400000
    if (this.cubeDesc.auto_merge_time_ranges) {
      this.conversionTime()
      this.partitionStartDate = transToUtcTimeFormat(this.cubeDesc.partition_date_start, true)
      // this.cubeDesc.partition_date_start = transToUtcTimeFormat(this.cubeDesc.partition_date_start)
    }
    if (this.scheduler.desc.scheduled_run_time && this.scheduler.desc.partition_interval) {
      this.initRepeatInterval(this.scheduler.desc)
      this.scheduledRunTime = transToUtcTimeFormat(this.scheduler.desc.scheduled_run_time, true)
    } else {
      this.scheduledRunTime = null
      this.initScheduler()
    }
  },
  locales: {
    'en': {autoMergeThresholds: 'Auto Merge Thresholds', retentionThreshold: 'Retention Threshold', partitionStartDate: 'Partition Start Date', newThresholds: 'New Thresholds', buildTrigger: 'First Build Time', periddicalInterval: 'Build Cycle', days: 'days'},
    'zh-cn': {autoMergeThresholds: '触发自动合并的时间阈值', retentionThreshold: '保留时间阈值', partitionStartDate: '起始日期', newThresholds: '新建阈值', buildTrigger: '首次构建触发时间', periddicalInterval: '重复间隔', days: '天'}
  }
}
</script>
<style lang="less">
  @import '../../../assets/styles/variables.less';
  .refresh-setting-edit{
    margin-bottom: 50px;
    .refresh-title {
      color: @text-title-color;
      font-weight: bold;
    }
    .left-threshold {
      .el-select {
        width: 100%;
      }
    }
    .el-input {
      width: 100%;
    }
  }
</style>
