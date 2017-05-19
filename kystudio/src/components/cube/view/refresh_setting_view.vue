<template>
<el-card class="box-card">
  <el-row class="border_bottom">
    <el-col :span="6">{{$t('autoMergeThresholds')}}</el-col>
    <el-col :span="18">
      <el-row v-for="timeRange in cubeDesc.desc.auto_merge_time_ranges">
        <el-col :span="24">{{timeRange}}</el-col>
      </el-row>
    </el-col>  
  </el-row>
  <el-row >
    <el-col :span="6">{{$t('buildTrigger')}}</el-col>
    <el-col :span="18" v-if="cubeDesc.scheduler">{{cubeDesc.scheduler.triggerTime}}</el-col>
  </el-row>
  <el-row class="border_bottom">
    <el-col :span="6">{{$t('periddicalInterval')}}</el-col>
    <el-col :span="18" v-if="cubeDesc.scheduler">{{cubeDesc.scheduler.repeatInterval}}</el-col>
  </el-row>
  <el-row >
    <el-col :span="6">{{$t('retentionThreshold')}}</el-col>
    <el-col :span="18">{{cubeDesc.desc.retention_range}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="6">{{$t('partitionStartDate')}}</el-col>
    <el-col :span="18">{{cubeDesc.desc.partition_date_start}}</el-col>
  </el-row>        
</el-card>
</template>

<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../../util/business'
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
    })
  },
  created () {
    let _this = this
    if (!_this.cubeDesc.scheduler) {
      _this.getScheduler(this.cubeDesc.name).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          _this.$set(_this.cubeDesc, 'scheduler', data)
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
        })
      })
    }
  },
  locales: {
    'en': {autoMergeThresholds: 'Auto Merge Thresholds', retentionThreshold: 'Retention Threshold', partitionStartDate: 'Partition Start Date', buildTrigger: 'Auto Build Trigger', periddicalInterval: 'Periddical Interval'},
    'zh-cn': {autoMergeThresholds: '触发自动合并的时间阈值', retentionThreshold: '保留时间阈值', partitionStartDate: '起始日期', buildTrigger: '自动构建触发时间', periddicalInterval: '重复间隔'}
  }
}
</script>
<style scoped>
.border_bottom {
  border-bottom: 2px solid #ddd;
 }
</style>
