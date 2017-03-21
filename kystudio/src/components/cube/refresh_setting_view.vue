<template>
<el-card class="box-card">
  <el-row>
    <el-col :span="8">{{$t('autoMergeThresholds')}}</el-col>
    <el-col :span="16">
      <el-row v-for="timeRange in desc.auto_merge_time_ranges">
        <el-col :span="24">{{timeRange}}</el-col>
      </el-row>
    </el-col>  
  </el-row>
  <el-row>
    <el-col :span="8">{{$t('retentionThreshold')}}</el-col>
    <el-col :span="16">{{desc.retention_range}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="8">{{$t('partitionStartDate')}}</el-col>
    <el-col :span="16">{{desc.partition_date_start}}</el-col>
  </el-row>        
</el-card>
</template>

<script>
export default {
  name: 'cubedesc',
  props: ['desc'],
  data () {
    return {
      selected_project: localStorage.getItem('selected_project')
    }
  },
  locales: {
    'en': {autoMergeThresholds: 'Auto Merge Thresholds', retentionThreshold: 'Retention Threshold', partitionStartDate: 'Partition Start Date'},
    'zh-cn': {autoMergeThresholds: '触发自动合并的时间阈值', retentionThreshold: '保留时间阈值', partitionStartDate: '起始日期'}
  }
}
</script>
<style scoped="">

</style>
