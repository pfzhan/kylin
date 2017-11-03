<template>
<div class="box-card card_margin border_bottom el-row" id="refresh-setting">
<h2 class="ksd-mt-40 ksd-ml-40"><!-- <el-checkbox v-model="$store.state.cube.cubeSchedulerIsSetting"> --><span style="font-size:16px;">{{$t('kylinLang.cube.merge')}} <common-tip :content="$t('kylinLang.cube.refreshSetTip')" ><icon name="question-circle" class="ksd-question-circle"></icon></common-tip></span></el-checkbox></h2>
  <el-row>
    <el-col :span="4" class="ksd-right ksd-lineheight-40 ksd-mr-10">{{$t('autoMergeThresholds')}}
      <!-- <common-tip :content="$t('kylinLang.cube.refreshSetTip')" ><icon name="question-circle-o"></icon></common-tip> -->
    </el-col>
    <el-col :span="16">
      <el-row :gutter="20" class="row_padding" v-for="(timeRange, index) in timeRanges" :key="index">
        <el-col :span="10">
          <el-input v-model="timeRange.range" v-if="timeRange.type === 'days'" @change="changeTimeRange(timeRange, index)"></el-input>
          <el-select  v-model="timeRange.range" style="width:100%" v-else @change="changeTimeRange(timeRange, index)">
            <el-option
               v-for="(item, time_index) in timeOptions"
               :key="time_index"
               :label="item"
               :value="item">
            </el-option>
          </el-select>
        </el-col>
        <el-col :span="6">
            <el-select  v-model="timeRange.type" @change="changeTimeRange(timeRange, index)">
              <el-option
                 v-for="(item, range_index) in rangesOptions"
                :key="range_index"
                :label="item"
                :value="item">
              </el-option>
            </el-select>
        </el-col>
        <el-col :span="2" class="ksd-lineheight-30 ksd-mr-10">
          <el-button type="danger" icon="minus" size="mini" @click.native="removeTimeRange(index)"></el-button>
        </el-col>
      </el-row>
      <el-button class="btn_margin ksd-mt-8"  icon="plus" @click.native="addNewTimeRange">{{$t('newThresholds')}} </el-button>
    </el-col>
  </el-row>
  <div class="line" style="margin: 8px -30px 8px -30px;"></div>
  <el-row class="row_padding">
    <el-col :span="4" class="ksd-right ksd-lineheight-40 ksd-mr-10">{{$t('retentionThreshold')}} </el-col>
    <el-col :span="6">
      <el-input class="input_width" v-model="retentionRange" @change="changeRetentionRange"></el-input>
    </el-col>
    <el-col :span="1" class="ksd-left ksd-lineheight-40 ksd-mr-10">
      {{$t('days')}}
    </el-col>
  </el-row>
  <el-row class="row_padding">
    <el-col :span="4" class="ksd-right ksd-lineheight-40 ksd-mr-10">{{$t('partitionStartDate')}} </el-col>
    <el-col :span="6">
      <el-date-picker class="input_width" @change="changePartitionDateStart"
        v-model="partitionStartDate"
        type="datetime"
        align="right">
      </el-date-picker>
    </el-col>
  </el-row>

 <div class="line" style="margin: 6px -30px 6px -30px;"></div>
 <h2 class="ksd-mt-40 ksd-ml-40"><el-checkbox v-model="$store.state.cube.cubeSchedulerIsSetting"><span style="font-size:16px;">{{$t('kylinLang.cube.scheduler')}} <common-tip :content="$t('kylinLang.cube.schedulerTip')" ><icon name="question-circle" class="ksd-question-circle"></icon></common-tip></span></el-checkbox></h2>
   <el-row class="row_padding">
    <el-col :span="4" class="ksd-right ksd-lineheight-40 ksd-mr-10">{{$t('buildTrigger')}} </el-col>
    <el-col :span="6">
      <el-date-picker class="input_width" @change="changeTriggerTime()" :disabled="!$store.state.cube.cubeSchedulerIsSetting"
        v-model="scheduledRunTime"
        type="datetime"
        align="right">
      </el-date-picker>
    </el-col>
  </el-row>
  <el-row class="row_padding">
    <el-col :span="4" class="ksd-right ksd-lineheight-40 ksd-mr-10">{{$t('periddicalInterval')}} </el-col>
    <el-col :span="8">
      <el-row :gutter="20" class="row_padding">
        <el-col :span="9">
          <el-input v-model="intervalRange.range" :disabled="!$store.state.cube.cubeSchedulerIsSetting"  @change="changeInterval()"></el-input>
        </el-col>
        <el-col :span="8">
            <el-select :placeholder="$t('kylinLang.common.pleaseSelect')" v-model="intervalRange.type" @change="changeInterval()" :disabled="!$store.state.cube.cubeSchedulerIsSetting">
              <el-option
                 v-for="(item, range_index) in intervalOptions" :key="range_index"
                :label="item"
                :value="item">
              </el-option>
            </el-select>
          </el-col>
        </el-row>
      </el-col>
  </el-row>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, transToUtcTimeFormat, transToUTCMs, msTransDate } from '../../../util/business'
export default {
  name: 'refreshSetting',
  props: ['cubeDesc', 'scheduler'],
  data () {
    return {
      timeRanges: [],
      intervalRange: {range: '', type: ''},
      timeOptions: [0.5, 1, 2, 4, 8],
      rangesOptions: ['days', 'hours', 'minutes'],
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
        var transDate = msTransDate(item, true)
        // let _day = Math.floor(item / 86400000)
        // let _hour = Math.floor(item / 3600000)
        // let _minute = item / 60000
        let rangeObj = {
          type: transDate.type,
          range: transDate.value,
          mills: item
        }
        // if (_hour === 0) {
        //   rangeObj.type = 'minutes'
        //   rangeObj.range = _minute
        //   rangeObj.mills = rangeObj.range * 60000
        // } else if (_day === 0) {
        //   rangeObj.type = 'hours'
        //   rangeObj.range = _hour
        //   rangeObj.mills = rangeObj.range * 3600000
        // } else {
        //   rangeObj.type = 'days'
        //   rangeObj.range = _day
        //   rangeObj.mills = rangeObj.range * 86400000
        // }
        _this.timeRanges.push(rangeObj)
      })
    },
    removeTimeRange: function (index) {
      this.timeRanges.splice(index, 1)
      this.cubeDesc.auto_merge_time_ranges.splice(index, 1)
    },
    initRepeatInterval: function (data) {
      // let _week = Math.floor(data.partition_interval / 604800000)
      // let _day = Math.floor(data.partition_interval / 86400000)
      // let _hour = Math.floor(data.partition_interval / 3600000)
      // var _minute = Math.floor(data.partition_interval / 60000)
      var transDate = msTransDate(data.partition_interval)
      this.intervalRange.type = transDate.type
      this.intervalRange.range = transDate.value
      // if (_week === 0) {
      //   if (_day === 0) {
      //     if (_hour === 0) {
      //       this.intervalRange.type = 'minutes'
      //       this.intervalRange.range = _minute
      //     } else {
      //       this.intervalRange.type = 'hours'
      //       this.intervalRange.range = _hour
      //     }
      //   } else {
      //     this.intervalRange.type = 'days'
      //     this.intervalRange.range = _day
      //   }
      // } else {
      //   this.intervalRange.type = 'weeks'
      //   this.intervalRange.range = _week
      // }
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
      } else {
        time = timeRange.range * 86400000
      }
      this.cubeDesc.auto_merge_time_ranges[index] = time
    },
    changeRetentionRange (retentionRange) {
      let time = 0
      time = retentionRange * 86400000
      this.cubeDesc.retention_range = time
    },
    initScheduler: function () {
      // var schedulerName = this.cubeDesc.name + (this.cubeDesc.status === 'DRAFT' ? '_draft' : '')
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
      this.timeRanges.push({type: 'days', range: 0, mills: 0})
      this.cubeDesc.auto_merge_time_ranges.push(0)
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
      // this.scheduler.desc.scheduled_run_time = transToUtcTimeFormat((new Date()).getTime())
      this.scheduledRunTime = null
      this.initScheduler()
    }
  },
  locales: {
    'en': {autoMergeThresholds: 'Auto Merge Thresholds: ', retentionThreshold: 'Retention Threshold: ', partitionStartDate: 'Partition Start Date: ', newThresholds: 'New Thresholds', buildTrigger: 'First Build Time: ', periddicalInterval: 'Build Cycle: ', days: 'days'},
    'zh-cn': {autoMergeThresholds: '触发自动合并的时间阈值：', retentionThreshold: '保留时间阈值：', partitionStartDate: '起始日期：', newThresholds: '新建阈值', buildTrigger: '首次构建触发时间：', periddicalInterval: '重复间隔：', days: '天'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .card_margin {
    font-size: 14px;
    margin-bottom: 20px;
  }
  .btn_margin {
    margin-bottom: 4px;
  }
  .row_padding {
    padding-top: 5px;
    padding-bottom: 5px;
  }
  .input_width {
    width: 40%;
  }
  .border_bottom {
    border-bottom: 1px solid @grey-color;
  }
  #refresh-setting{
    .el-button--default{
      background: transparent;
      border-color: @grey-color;
    }
    .el-button--default:hover{
      border-color: @base-color;
    }
  }
</style>
