<template>
<div class="box-card card_margin">
  <el-row class="border_bottom">
    <el-col :span="4" class="ksd-right ksd-lineheight-40 ksd-mr-10">{{$t('autoMergeThresholds')}} </el-col>
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
          <el-button type="primary" icon="minus" size="mini" @click.native="removeTimeRange(index)"></el-button>
        </el-col>                
      </el-row>
      <el-button class="btn_margin"  icon="plus" @click.native="addNewTimeRange">{{$t('newThresholds')}} </el-button>
    </el-col>  
  </el-row>
  <el-row class="row_padding">
    <el-col :span="4" class="ksd-right ksd-lineheight-40 ksd-mr-10">{{$t('buildTrigger')}} </el-col>
    <el-col :span="16">
      <el-date-picker class="input_width" @change="changeTriggerTime()"
        v-model="scheduler.desc.scheduled_run_time"
        type="datetime"
        align="right">
      </el-date-picker>
    </el-col>
  </el-row>
  <el-row class="row_padding border_bottom">
    <el-col :span="4" class="ksd-right ksd-lineheight-40 ksd-mr-10">{{$t('periddicalInterval')}} </el-col>
    <el-col :span="16">
      <el-row :gutter="20" class="row_padding">
        <el-col :span="10">
          <el-input v-model="intervalRange.range"  @change="changeInterval()"></el-input>          
        </el-col>
        <el-col :span="6">
            <el-select v-model="intervalRange.type" @change="changeInterval()">
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
  <el-row class="row_padding">
    <el-col :span="4" class="ksd-right ksd-lineheight-40 ksd-mr-10">{{$t('retentionThreshold')}} </el-col>
    <el-col :span="16">
      <el-input class="input_width" v-model="cubeDesc.retention_range" ></el-input>
    </el-col>
  </el-row>
  <el-row class="row_padding">
    <el-col :span="4" class="ksd-right ksd-lineheight-40 ksd-mr-10">{{$t('partitionStartDate')}} </el-col>
    <el-col :span="16">
      <el-date-picker class="input_width" @change="changePartitionDateStart"
        v-model="cubeDesc.partition_date_start"
        type="datetime"
        align="right">
      </el-date-picker>
    </el-col>
  </el-row>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../../util/business'
export default {
  name: 'refreshSetting',
  props: ['cubeDesc', 'scheduler'],
  data () {
    return {
      timeRanges: [],
      intervalRange: {range: '', type: ''},
      timeOptions: [0.5, 1, 2, 4, 8],
      rangesOptions: ['days', 'hours'],
      intervalOptions: ['weeks', 'days', 'hours'],
      selected_project: localStorage.getItem('selected_project')
    }
  },
  methods: {
    ...mapActions({
      getScheduler: 'GET_SCHEDULER'
    }),
    conversionTime: function () {
      let _this = this
      _this.cubeDesc.auto_merge_time_ranges.forEach(function (item) {
        let _day = Math.floor(item / 86400000)
        let _hour = (item % 86400000) / 3600000
        let rangeObj = {
          type: 'days',
          range: 0,
          mills: 0
        }
        if (_day === 0) {
          rangeObj.type = 'hours'
          rangeObj.range = _hour
          rangeObj.mills = rangeObj.range * 3600000
        } else {
          rangeObj.type = 'days'
          rangeObj.range = _day
          rangeObj.mills = rangeObj.range * 86400000
        }
        _this.timeRanges.push(rangeObj)
      })
    },
    removeTimeRange: function (index) {
      this.timeRanges.splice(index, 1)
      this.cubeDesc.auto_merge_time_ranges.splice(index, 1)
    },
    initRepeatInterval: function (data) {
      let _week = Math.floor(data.partition_interval / 604800000)
      let _day = Math.floor(data.partition_interval / 86400000)
      let _hour = Math.floor(data.partition_interval / 3600000)
      if (_week === 0) {
        if (_day === 0) {
          this.intervalRange.type = 'hours'
          this.intervalRange.range = _hour
        } else {
          this.intervalRange.type = 'days'
          this.intervalRange.range = _day
        }
      } else {
        this.intervalRange.type = 'weeks'
        this.intervalRange.range = _week
      }
    },
    changePartitionDateStart: function () {
      this.cubeDesc.partition_date_start = this.cubeDesc.partition_date_start.getTime()
    },
    changeTriggerTime: function () {
      this.scheduler.desc.scheduled_run_time = this.scheduler.desc.scheduled_run_time.getTime()
    },
    changeTimeRange: function (timeRange, index) {
      let time = 0
      if (timeRange.type === 'hours') {
        time = timeRange.range * 3600000
      } else {
        time = timeRange.range * 86400000
      }
      this.cubeDesc.auto_merge_time_ranges[index] = time
    },
    initScheduler: function () {
      var schedulerName = this.cubeDesc.name + (this.cubeDesc.status === 'DRAFT' ? '_draft' : '')
      this.getScheduler(schedulerName).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.initRepeatInterval(data)
          this.scheduler.desc.scheduled_run_time = data.scheduled_run_time
          this.scheduler.desc.partition_interval = data.partition_interval
        })
      }).catch((res) => {
        handleError(res, () => {
        })
      })
    },
    changeInterval: function () {
      let time = 0
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
    if (this.cubeDesc.auto_merge_time_ranges) {
      this.conversionTime()
    }
    if (this.scheduler.desc.scheduled_run_time && this.scheduler.desc.partition_interval) {
      this.initRepeatInterval(this.scheduler.desc)
    } else {
      this.initScheduler()
    }
  },
  locales: {
    'en': {autoMergeThresholds: 'Auto Merge Thresholds: ', retentionThreshold: 'Retention Threshold: ', partitionStartDate: 'Partition Start Date: ', newThresholds: 'New Thresholds', buildTrigger: 'Auto Build Trigger: ', periddicalInterval: 'Periddical Interval: '},
    'zh-cn': {autoMergeThresholds: '触发自动合并的时间阈值：', retentionThreshold: '保留时间阈值：', partitionStartDate: '起始日期：', newThresholds: '新建阈值', buildTrigger: '自动构建触发时间：', periddicalInterval: '重复间隔：'}
  }
}
</script>
<style scoped>
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
  width: 40%
 }
.border_bottom {
  border-bottom: 1px solid #ddd;
 }
</style>
