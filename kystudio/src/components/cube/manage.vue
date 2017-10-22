<template>
  <div id="cubeManage">
    <manageCard :title="$t('partitionStartDate')" v-on:save="update('partition_start_date')">
      <div slot="content">
        <el-row class="row_padding" >
          <el-col :span="4" class="ksd-lineheight-10" :offset="1">{{$t('partitionStartDate')}} </el-col>
        </el-row>
        <el-row class="row_padding" >
          <el-col :span="6" class="ksd-lineheight-20" :offset="1">
            <el-date-picker @change="changePartitionDateStart"
              v-model="partitionStartDate"
              type="datetime"
              align="right">
            </el-date-picker>
          </el-col>
        </el-row>
      </div>  
    </manageCard>

    <manageCard :title="$t('notificationSetting')" v-on:save="update('notification_setting')"> 
      <div slot="content">
        <el-row class="row_padding">
          <el-col :span="4" class="ksd-lineheight-10" :offset="1">{{$t('notificationEmailList')}} </el-col>
        </el-row>
        <el-row class="row_padding">
          <el-col :span="6" class="ksd-lineheight-20" :offset="1">
            <el-input v-model="getNotifyList" placeholder="Comma Separated" @change="changeNotifyList"></el-input>
          </el-col>
        </el-row> 
        <el-row class="row_padding">
          <el-col :span="4" class="ksd-lineheight-10" :offset="1"> {{$t('notificationEvents')}}
            <common-tip :content="$t('kylinLang.cube.noticeTip')" ><icon name="question-circle-o"></icon></common-tip>
          </el-col>
        </el-row>
        <el-row class="row_padding">
          <el-col :span="9" class="ksd-lineheight-20" :offset="1">
            <area_label  :labels="options" :datamap="{label: 'label', value: 'value'}" :selectedlabels="saveData.cubeDescData.status_need_notify" @refreshData="refreshNotificationEvents"> 
            </area_label>
          </el-col>
        </el-row>
      </div>  
    </manageCard>

    <manageCard :title="$t('mergeSetting')" v-on:save="update('merge_setting')">
      <common-tip :content="$t('kylinLang.cube.refreshSetTip')" slot="tip">
        <icon name="question-circle-o"></icon>
      </common-tip>
      <div slot="content">  
        <el-row class="row_padding">
          <el-col :span="4" class="ksd-lineheight-10" :offset="1">
            {{$t('autoMergeThresholds')}} 
          </el-col>
        </el-row>

        <el-row :gutter="20" class="row_padding" v-for="(timeRange, index) in timeRanges" :key="index">
          <el-col :span="4" class="ksd-lineheight-20" :offset="1">
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
          <el-col :span="4" class="ksd-lineheight-20" >
              <el-select  v-model="timeRange.type" @change="changeTimeRange(timeRange, index)">
                <el-option
                   v-for="(item, range_index) in rangesOptions"
                  :key="range_index"
                  :label="item"
                  :value="item">
                </el-option>
              </el-select>
          </el-col>
          <el-col :span="2" class="ksd-lineheight-20">
            <el-button type="danger" icon="minus" size="mini" @click.native="removeTimeRange(index)"></el-button>
          </el-col>                
        </el-row>

        <el-row class="row_padding">
          <el-col class="ksd-lineheight-20" :offset="1">
            <el-button class="btn_margin ksd-mt-8"  icon="plus" @click.native="addNewTimeRange">{{$t('newThresholds')}} </el-button>
          </el-col>
        </el-row>
      </div>
    </manageCard>
    
    <manageCard :title="$t('schedulerSetting')" :hasCheck=true v-on:save="checkSchedulerFrom"> 
      <el-checkbox v-model="isSetScheduler" slot="header" style="float:right"></el-checkbox>
      <common-tip :content="$t('kylinLang.cube.schedulerTip')" slot="tip">
        <icon name="question-circle-o"></icon>
      </common-tip>
      <div slot="content"> 
        <el-row class="row_padding">
          <el-col :span="4" class="ksd-lineheight-10" :offset="1">{{$t('buildTrigger')}} </el-col>
        </el-row>
        <el-row class="row_padding"> 
          <el-col :span="6" class="ksd-lineheight-20" :offset="1">
            <el-date-picker class="input_width" @change="changeTriggerTime()" :disabled="!isSetScheduler"
              v-model="scheduledRunTime"
              type="datetime"
              align="right">
            </el-date-picker>
          </el-col>
        </el-row>
        <el-row class="row_padding">
          <el-col :span="4" class="ksd-lineheight-10" :offset="1">
          {{$t('periddicalInterval')}} 
          </el-col>
        </el-row>
        <el-row :gutter="20" class="row_padding">
          <el-col :span="3" class="ksd-lineheight-20" :offset="1">
            <el-input v-model="intervalRange.range" :disabled="!isSetScheduler"  @change="changeInterval()"></el-input>      
          </el-col>
          <el-col :span="8" class="ksd-lineheight-20">
            <el-select :placeholder="$t('kylinLang.common.pleaseSelect')" v-model="intervalRange.type" @change="changeInterval()" :disabled="!isSetScheduler">
              <el-option
                v-for="(item, range_index) in intervalOptions" :key="range_index"
                :label="item"
                :value="item">
              </el-option>
            </el-select>
          </el-col>
        </el-row>
      </div>
    </manageCard>

    <manageCard :title="$t('cubeEngine')" v-on:save="update('cube_engine')"> 
        <div slot="content">
        <el-row class="row_padding" :offset="1">
          <el-col :span="4" class="ksd-lineheight-10" :offset="1">{{$t('cubeEngine')}} 
          </el-col>
        </el-row>
        <el-row class="row_padding">
          <el-col class="ksd-lineheight-20" :offset="1">
            <el-select v-model="saveData.cubeDescData.engine_type">
              <el-option  v-for="item in engineType"
              :key="item.value"
              :label="item.name"
              :value="item.value"></el-option>
            </el-select>
          </el-col>
        </el-row>
      </div>
    </manageCard>

    <manageCard :title="$t('retentionSetting')" v-on:save="update('retention_setting')">  
      <div slot="content">
        <el-row class="row_padding">
          <el-col :span="4" class="ksd-lineheight-10" :offset="1">{{$t('retentionThreshold')}} 
          </el-col>
        </el-row>
        <el-row class="row_padding">
          <el-col :span="6" class="ksd-lineheight-20" :offset="1">
            <el-input class="input_width" v-model="saveData.cubeDescData.retention_range" ></el-input> {{$t('days')}}
          </el-col>
        </el-row>
      </div>
    </manageCard>

    <manageCard :title="$t('cubeConfig')" v-on:save="checkConfigFrom">
      <configuration_overwrites class="config" :cubeDesc="saveData.cubeDescData" slot="content">
      </configuration_overwrites>
    </manageCard>
  </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, transToUTCMs, msTransDate, transToUtcTimeFormat } from '../../util/business'
import { engineTypeKylin, engineTypeKap } from '../../config/index'
import manageCard from '../common/manage_card'
import areaLabel from '../common/area_label'
import configurationOverwrites from './edit/configuration_overwrites_edit'
export default {
  name: 'cubeManage',
  props: ['extraoption'],
  data () {
    return {
      saveData: {
        cubeDescData: {
          status_need_notify: [],
          notify_list: [],
          engine_type: 100,
          override_kylin_properties: {}
        }
      },
      partitionStartDate: 0,
      getNotifyList: '',
      options: [{label: 'ERROR', value: 'ERROR'}, {label: 'DISCARDED', value: 'DISCARDED'}, {label: 'SUCCEED', value: 'SUCCEED'}],
      timeRanges: [],
      intervalRange: {range: '', type: ''},
      timeOptions: [0.5, 1, 2, 4, 8],
      rangesOptions: ['days', 'hours', 'minutes'],
      intervalOptions: ['weeks', 'days', 'hours', 'minutes'],
      scheduledRunTime: 0,
      isSetScheduler: false
    }
  },
  components: {
    'manageCard': manageCard,
    'area_label': areaLabel,
    'configuration_overwrites': configurationOverwrites
  },
  methods: {
    ...mapActions({
      loadCubeDesc: 'LOAD_CUBE_DESC',
      getScheduler: 'GET_SCHEDULER',
      manageCube: 'MANAGE_CUBE'
    }),
    checkSchedulerFrom () {
      if (this.saveData.schedulerJobData.scheduled_run_time === 0 || this.saveData.schedulerJobData.partition_interval === 0) {
        this.$message(this.$t('unsetScheduler'))
        return false
      }
      this.update('scheduler_setting')
    },
    checkConfigFrom () {
      for (var key in this.saveData.cubeDescData.override_kylin_properties) {
        if (key === '') {
          this.$message({
            showClose: true,
            duration: 3000,
            message: this.$t('checkCOKey'),
            type: 'error'
          })
          return false
        }
        if (this.saveData.cubeDescData.override_kylin_properties[key] === '') {
          this.$message({
            showClose: true,
            message: this.$t('checkCOValue'),
            type: 'error'
          })
          return false
        }
      }
      this.update('cube_config')
    },
    update (type) {
      let content = {
        'type': type,
        'name': this.extraoption.cubeName,
        'data': {},
        'project': this.extraoption.project
      }
      switch (type) {
        case 'partition_start_date':
          this.$set(content.data, 'partitionDateStart', this.saveData.cubeDescData.partition_date_start)
          break
        case 'notification_setting':
          this.$set(content.data, 'notifyList', this.saveData.cubeDescData.notify_list)
          this.$set(content.data, 'statusNeedNotify', this.saveData.cubeDescData.status_need_notify)
          break
        case 'merge_setting':
          this.$set(content.data, 'autoMergeTimeRanges', this.saveData.cubeDescData.auto_merge_time_ranges)
          break
        case 'scheduler_setting':
          this.$set(content.data, 'enabled', this.isSetScheduler)
          this.$set(content.data, 'triggerTime', this.saveData.schedulerJobData.scheduled_run_time)
          this.$set(content.data, 'repeatInterval', this.saveData.schedulerJobData.partition_interval)
          break
        case 'cube_engine':
          this.$set(content.data, 'engineType', this.saveData.cubeDescData.engine_type)
          break
        case 'retention_setting':
          this.$set(content.data, 'retentionRange', this.saveData.cubeDescData.retention_range * 86400000)
          break
        case 'cube_config':
          this.$set(content.data, 'overrideKylinProperties', this.saveData.cubeDescData.override_kylin_properties)
          break
      }
      this.manageCube(content).then((res) => {
        handleSuccess(res, () => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.saveSuccess')
          })
        })
      }, (res) => {
        handleError(res)
      })
    },
    changePartitionDateStart () {
      this.saveData.cubeDescData.partition_date_start = transToUTCMs(this.partitionStartDate)
    },
    changeNotifyList: function () {
      this.saveData.cubeDescData.notify_list = this.getNotifyList.split(',')
    },
    refreshNotificationEvents (data) {
      this.$set(this.saveData.cubeDescData, 'status_need_notify', data)
    },
    initTimeRange () {
      this.saveData.cubeDescData.auto_merge_time_ranges.forEach((item) => {
        var transDate = msTransDate(item, true)
        let rangeObj = {
          type: transDate.type,
          range: transDate.value,
          mills: item
        }
        this.timeRanges.push(rangeObj)
      })
    },
    addNewTimeRange () {
      this.timeRanges.push({type: 'days', range: 0, mills: 0})
      this.saveData.cubeDescData.auto_merge_time_ranges.push(0)
    },
    changeTimeRange (timeRange, index) {
      let time = 0
      if (timeRange.type === 'minutes') {
        time = timeRange.range * 60000
      } else if (timeRange.type === 'hours') {
        time = timeRange.range * 3600000
      } else {
        time = timeRange.range * 86400000
      }
      this.saveData.cubeDescData.auto_merge_time_ranges[index] = time
    },
    removeTimeRange (index) {
      this.timeRanges.splice(index, 1)
      this.saveData.cubeDescData.auto_merge_time_ranges.splice(index, 1)
    },
    changeInterval () {
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
      this.saveData.schedulerJobData.partition_interval = time
    },
    changeTriggerTime: function () {
      this.saveData.schedulerJobData.scheduled_run_time = transToUTCMs(this.scheduledRunTime)
    }
  },
  created () {
    this.loadCubeDesc({cubeName: this.extraoption.cubeName, project: this.extraoption.project}).then((res) => {
      handleSuccess(res, (data) => {
        this.$set(this.saveData, 'cubeDescData', data.cube)
        this.getNotifyList = this.saveData.cubeDescData.notify_list.toString()
        this.partitionStartDate = transToUtcTimeFormat(this.saveData.cubeDescData.partition_date_start)
        this.saveData.cubeDescData.retention_range = this.saveData.cubeDescData.retention_range / 86400000
        this.initTimeRange()
      })
    })
    this.getScheduler({cubeName: this.extraoption.cubeName, project: this.extraoption.project}).then((res) => {
      handleSuccess(res, (data) => {
        if (data) {
          this.isSetScheduler = data.schedulerJob.enabled
          this.$set(this.saveData, 'schedulerJobData', data.schedulerJob)
          this.scheduledRunTime = transToUtcTimeFormat(this.saveData.schedulerJobData.scheduled_run_time)
          var transDate = msTransDate(this.saveData.schedulerJobData.partition_interval)
          this.intervalRange.type = transDate.type
          this.intervalRange.range = transDate.value
        }
      })
    })
  },
  computed: {
    engineType () {
      if (this.saveData.cubeDescData.storage_type === 2) {
        return engineTypeKylin
      } else if (this.saveData.cubeDescData.storage_type === 99) {
        return engineTypeKap
      }
    }
  },
  locales: {
    'en': {autoMergeThresholds: 'Auto Merge Thresholds', retentionThreshold: 'Retention Threshold', partitionStartDate: 'Partition Start Date', newThresholds: 'New Thresholds', buildTrigger: 'First Build Time', periddicalInterval: 'Build Cycle', tip: 'Tips', propertyTip: 'Cube level properties will overwrite configuration in kylin.properties', addConfiguration: 'Add Configuration', engineTip: 'Select cube engine for building cube.', cubeDefault: 'Cube default configuration', jobRelated: 'Job related configuration', hiveRelated: 'Hive related configuration', notificationEmailList: 'Notification Email List', notificationEvents: 'Notification Events', notificationSetting: 'Notification Setting', mergeSetting: 'Merge Setting', schedulerSetting: 'Scheduler Setting', cubeEngine: 'Cube Engine', retentionSetting: 'Retention Setting', cubeConfig: 'Cube Config', unsetScheduler: 'Scheduler configuration is imperfect', checkCOKey: 'Property name is required!', checkCOValue: 'Property value is required!', days: 'days'},
    'zh-cn': {autoMergeThresholds: '触发自动合并的时间阈值', retentionThreshold: '保留时间阈值', partitionStartDate: '起始日期', newThresholds: '新建阈值', buildTrigger: '首次构建触发时间：', periddicalInterval: '重复间隔', tip: '提示', propertyTip: 'Cube级的属性值将会覆盖kylin.properties中的属性值', addConfiguration: '添加配置', engineTip: '选择一个Cube构建引擎。', cubeDefault: 'Cube 默认配置', jobRelated: 'Job 相关配置', hiveRelated: 'Hive 相关配置', notificationEmailList: '通知邮件列表', notificationEvents: '需通知的事件', notificationSetting: '通知设置', mergeSetting: '合并设置', schedulerSetting: '调度器设置', cubeEngine: 'Cube引擎', retentionSetting: '保留时间设置', cubeConfig: 'Cube配置', unsetScheduler: 'Scheduler 参数设置不完整', checkCOKey: '属性名不能为空!', checkCOValue: '属性值不能为空!', days: '天'}
  }
}
</script>
<style lang="less">
@import '../../less/config.less';
 #cubeManage {
  padding: 0px 0px 40px 0px;
  .row_padding {
    padding: 0px 30px 0px 30px;
    font-size: 12px;
    font-weight:bold;
  }
  .el-select {
    .el-select__tags {
      left: 0px;
    }
    .el-input__inner {
      padding: 1px 5px 3px 3px;
    }
  }
  .config {
    padding-left: 9px;
  }
 }
</style>
