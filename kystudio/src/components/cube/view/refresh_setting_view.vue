<template>
<el-card class="box-card" id="refresh-settion-view">
  <el-row class="border_bottom">
    <el-col :span="6">{{$t('autoMergeThresholds')}}</el-col>
    <el-col :span="18">
      <el-row v-for="timeRange in cubeDesc.desc.auto_merge_time_ranges" :key="timeRange">
        <el-col :span="24">{{timeRange|timeSize}}</el-col>
      </el-row>
    </el-col>  
  </el-row>
  <el-row >
    <el-col :span="6">{{$t('buildTrigger')}}</el-col>
    <el-col :span="18" v-if="cubeDesc.scheduler">{{toGmtTime(cubeDesc.scheduler.triggerTime)}}</el-col>
  </el-row>
  <el-row class="border_bottom">
    <el-col :span="6">{{$t('periddicalInterval')}}</el-col>
    <el-col :span="18" v-if="cubeDesc.scheduler">{{cubeDesc.scheduler.repeatInterval}}</el-col>
  </el-row>
  <el-row >
    <el-col :span="6">{{$t('retentionThreshold')}}</el-col>
    <el-col :span="18">{{cubeDesc.desc.retention_range|timeSize}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="6">{{$t('partitionStartDate')}}</el-col>
    <el-col :span="18">{{toGmtTime(cubeDesc.desc.partition_date_start)}}</el-col>
  </el-row>        
</el-card>
</template>

<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, transToGmtTime } from '../../../util/business'
export default {
  name: 'refreshSetting',
  props: ['cubeDesc'],
  data () {
    return {
      selected_project: localStorage.getItem('selected_project')
    }
  },
  methods: {
    ...mapActions({
      getScheduler: 'GET_SCHEDULER'
    }),
    toGmtTime: transToGmtTime
  },
  created () {
    let _this = this
    if (!_this.cubeDesc.scheduler) {
      _this.getScheduler(this.cubeDesc.name).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          var schedulerData = data.schedulerJob || data.draft
          _this.$set(_this.cubeDesc, 'scheduler', schedulerData)
        })
      }).catch((res) => {
        handleError(res, () => {})
      })
    }
  },
  locales: {
    'en': {autoMergeThresholds: 'Auto Merge Thresholds', retentionThreshold: 'Retention Threshold', partitionStartDate: 'Partition Start Date', buildTrigger: 'Auto Build Trigger', periddicalInterval: 'Periddical Interval'},
    'zh-cn': {autoMergeThresholds: '触发自动合并的时间阈值', retentionThreshold: '保留时间阈值', partitionStartDate: '起始日期', buildTrigger: '自动构建触发时间', periddicalInterval: '重复间隔'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .border_bottom {
    border-bottom: 2px solid #ddd;
  }
  #refresh-settion-view{
    border-color: @grey-color;
    padding: 10px;
  }
</style>
