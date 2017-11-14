<template>
<div>
  <div class="refresh-settion-view ksd-common-table ksd-mt-10">
    <el-row class="tableheader" v-for="(timeRange, index) in cubeDesc.desc && cubeDesc.desc.auto_merge_time_ranges || []" :key="timeRange">
      <el-col :span="6" class="left-part"> <b>{{index === 0 ? $t('autoMergeThresholds') : '&nbsp;'}}</b></el-col>
      <el-col :span="18">
        {{timeRange|timeSize}}
      </el-col>
    </el-row>
  </div>
  <div style="clear:both;"></div>
  <div class="refresh-settion-view ksd-common-table ksd-mt-10">
    <el-row class="tableheader">
      <el-col :span="6" class="left-part"><b>{{$t('retentionThreshold')}}</b></el-col>
      <el-col :span="18"> {{cubeDesc.desc.retention_range/86400000}} {{$t('days')}}</el-col>
    </el-row>
    <el-row class="tableheader">
      <el-col :span="6" class="left-part"><b>{{$t('partitionStartDate')}}</b></el-col>
      <el-col :span="18"> {{cubeDesc.isStandardPartitioned ? toGmtTime(cubeDesc.desc.partition_date_start) : cubeDesc.desc.partition_date_start}}</el-col>
    </el-row>
  </div>
  <div class="refresh-settion-view ksd-common-table ksd-mt-10" v-if="cubeDesc.isStandardPartitioned">
    <el-row class="tableheader">
      <!-- <h2 class="ksd-mt-40 ksd-ml-40"><span style="font-size:16px;">{{$t('kylinLang.cube.scheduler')}}</span></h2> -->
      <el-col :span="6" class="left-part"><b>{{$t('buildTrigger')}}</b></el-col>
      <el-col :span="18" v-if="cubeDesc.scheduler"> {{toGmtTime(cubeDesc.scheduler.scheduled_run_time)}}</el-col>
    </el-row>
    <el-row class="tableheader">
      <el-col :span="6" class="left-part"><b>{{$t('periddicalInterval')}}</b></el-col>
      <el-col :span="18" v-if="cubeDesc.scheduler"> {{cubeDesc.scheduler.repeat_interval|timeSize}}</el-col>
    </el-row>
  </div>
</div>
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
      _this.getScheduler({cubeName: this.cubeDesc.name, project: this.selected_project}).then((res) => {
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
    'en': {autoMergeThresholds: 'Auto Merge Thresholds:', retentionThreshold: 'Retention Threshold:', partitionStartDate: 'Partition Start Date:', buildTrigger: 'First Build Time:', periddicalInterval: 'Build Cycle:', 'days': 'days'},
    'zh-cn': {autoMergeThresholds: '触发自动合并的时间阈值：', retentionThreshold: '保留时间阈值：', partitionStartDate: '起始日期：', buildTrigger: '首次构建触发时间：', periddicalInterval: '重复间隔：', 'days': '天'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .refresh-settion-view{
    .tableheader{
      .el-col{
        text-align: left;
        padding-left: 20px;
      }
      .left-part{
        border-right:solid 1px #393e53;
        text-align: right;
        padding-right: 20px;
      }
    }
    // border-color: @grey-color;
    // padding: 10px;
    // background: @tableBC;
    // padding: 0;
    // .padding-m{
    //   padding: 5px 0px 5px 20px;
    // }
  }
</style>
