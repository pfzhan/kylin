<template>
<el-card class="box-card">
  <el-row>
    <el-col :span="8">{{$t('autoMergeThresholds')}}</el-col>
    <el-col :span="16">
      <el-row :gutter="20" v-for="(timeRange, index) in cubeDesc.auto_merge_time_ranges">
        <el-col :span="10">
          <el-input></el-input>
        </el-col>
        <el-col :span="10">
            <el-select>
              <el-option
                 v-for="item in timeOptions"
                :label="item"
                :value="item">
              </el-option>
            </el-select>
        </el-col>
        <el-col :span="4">
          <el-button type="primary" icon="minus" @click.native="removeTimeRange(index)"></el-button>
        </el-col>                
      </el-row>
      <el-button type="primary" icon="plus" @click.native="addNewTimeRange">{{$t('newThresholds')}}</el-button>
    </el-col>  
  </el-row>
  <el-row>
    <el-col :span="8">{{$t('retentionThreshold')}}</el-col>
    <el-col :span="16">
      <el-input v-model="cubeDesc.retention_range"></el-input>
    </el-col>
  </el-row>
  <el-row>
    <el-col :span="8">{{$t('partitionStartDate')}}</el-col>
    <el-col :span="16">
      <el-date-picker
        v-model="cubeDesc.partition_date_start"
        type="datetime"
        align="right">
      </el-date-picker>
    </el-col>
  </el-row>        
</el-card>
</template>

<script>
export default {
  name: 'refreshSetting',
  props: ['cubeDesc'],
  data () {
    return {
      timeOptions: ['days', 'hours'],
      selected_project: localStorage.getItem('selected_project')
    }
  },
  methods: {
    removeTimeRange (index) {
      this.cubeDesc.auto_merge_time_ranges.splice(index, 1)
    },
    addNewTimeRange () {
      this.cubeDesc.auto_merge_time_ranges.push('')
    }
  },
  locales: {
    'en': {autoMergeThresholds: 'Auto Merge Thresholds', retentionThreshold: 'Retention Threshold', partitionStartDate: 'Partition Start Date', newThresholds: 'New Thresholds'},
    'zh-cn': {autoMergeThresholds: '触发自动合并的时间阈值', retentionThreshold: '保留时间阈值', partitionStartDate: '起始日期', newThresholds: '新建阈值'}
  }
}
</script>
<style scoped="">

</style>
