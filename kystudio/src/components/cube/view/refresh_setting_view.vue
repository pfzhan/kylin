<template>
<div class="refresh-settion-view ksd-mt-20">
  <table class="ksd-table">
    <tr class="ksd-tr" v-for="(timeRange, index) in cubeDesc.desc && cubeDesc.desc.auto_merge_time_ranges || []" :key="timeRange">
      <th>
        {{index === 0 ? $t('autoMergeThresholds') : '&nbsp;'}}
      </th>
      <td>        
        <span v-if="!isInteger">{{timeRange|timeSize}}</span>
        <span v-else>{{timeRange}}</span>
      </td>
    </tr>
  </table>

  <table class="ksd-table ksd-mt-10">
    <tr class="ksd-tr">
      <th>
        {{$t('retentionThreshold')}}
      </th>
      <td>        
        {{cubeDesc.desc.retention_range/86400000}} {{$t('days')}}
      </td>
    </tr>
    <tr class="ksd-tr">
      <th>
        {{$t('partitionStartDate')}}
      </th>
      <td>        
         {{cubeDesc.isStandardPartitioned ? toGmtTime(cubeDesc.desc.partition_date_start) : cubeDesc.desc.partition_date_start}}
      </td>
    </tr>
  </table>

  <table class="ksd-table ksd-mt-10" v-if="!isInteger">
    <tr class="ksd-tr">
      <th>
        {{$t('buildTrigger')}}
      </th>
      <td>        
        <span v-if="cubeDesc.scheduler"> 
          {{utcToConfigTimeZone(cubeDesc.scheduler.scheduled_run_time, 'UTC', 'YYYY-MM-DD HH:mm:ss')}}
        </span>
      </td>
    </tr>
    <tr class="ksd-tr">
      <th>
        {{$t('periddicalInterval')}}
      </th>
      <td>        
        <span v-if="cubeDesc.scheduler">
          {{cubeDesc.scheduler.repeat_interval|timeSize}}
        </span>
      </td>
    </tr>
  </table>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, transToGmtTime } from '../../../util/business'
import { utcToConfigTimeZone } from '../../../util/index'
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
    toGmtTime: transToGmtTime,
    utcToConfigTimeZone: utcToConfigTimeZone
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
  computed: {
    isInteger () {
      if (!this.cubeDesc.partitionDateColumn) {
        return false
      } else if (this.cubeDesc.isStandardPartitioned) {
        return false
      } else {
        return true
      }
    }
  },
  locales: {
    'en': {autoMergeThresholds: 'Auto Merge Thresholds:', retentionThreshold: 'Retention Threshold:', partitionStartDate: 'Partition Start Date:', buildTrigger: 'First Build Time:', periddicalInterval: 'Build Cycle:', 'days': 'days'},
    'zh-cn': {autoMergeThresholds: '触发自动合并的时间阈值：', retentionThreshold: '保留时间阈值：', partitionStartDate: '起始日期：', buildTrigger: '首次构建触发时间：', periddicalInterval: '重复间隔：', 'days': '天'}
  }
}
</script>
<style lang="less">
  @import '../../../assets/styles/variables.less';
  .refresh-settion-view {
      tr th {
      width: 178px;
    }
  }
</style>
