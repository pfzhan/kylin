<template>
  <div class="jobs_list ksd-mrl-20" @click.stop>
    <div class="ksd-title-label ksd-mt-20">{{$t('jobsList')}}</div>
    <el-row :gutter="20" class="jobs_tools_row ksd-mt-10 ksd-mb-10">
      <el-col :span="18">
        <el-dropdown class="ksd-fleft waiting-jobs" trigger="click" placement="bottom-start" @command="handleCommand">
          <el-button plain class="el-dropdown-link" size="medium" :disabled="!waittingJobModels.size">
            {{waittingJobModels.size}} {{$t('waitingjobs')}}<i class="el-icon-arrow-down el-icon--right"></i>
          </el-button>
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item v-for="(item, uuid) in waittingJobModels.data" :key="item.model_alias" :command="uuid">
              {{$t('kylinLang.common.model')}}{{item.model_alias}}: {{item.size}}{{$t('jobs')}}
            </el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
        <el-button-group class="action_groups ksd-ml-10 ksd-fleft">
          <el-button plain size="medium" icon="el-icon-ksd-table_resume" :disabled="!batchBtnsEnabled.resume" @click="batchResume">{{$t('jobResume')}}</el-button>
          <el-button plain size="medium" icon="el-icon-ksd-restart" :disabled="!batchBtnsEnabled.restart" @click="batchRestart">{{$t('jobRestart')}}</el-button>
          <el-button plain size="medium" icon="el-icon-ksd-pause" :disabled="!batchBtnsEnabled.pause" @click="batchPause">{{$t('jobPause')}}</el-button>
          <el-button plain size="medium" icon="el-icon-ksd-table_delete" :disabled="!batchBtnsEnabled.drop" @click="batchDrop">{{$t('jobDrop')}}</el-button>
        </el-button-group><el-button
        plain size="medium" class="ksd-ml-10 ksd-fleft" icon="el-icon-refresh" @click="refreshJobs">{{$t('kylinLang.common.refresh')}}</el-button>
      </el-col>
      <el-col :span="6">
        <el-input :placeholder="$t('kylinLang.common.pleaseFilter')" v-model="filter.subjectAlias"  @input="filterChange" class="show-search-btn ksd-fright" size="medium" prefix-icon="el-icon-search">
        </el-input>
      </el-col>
    </el-row>
    <transition name="fade">
      <div class="selectLabel" v-if="isSelectAllShow">
        <span>{{$t('selectedJobs', {selectedNumber: selectedNumber})}}</span>
        <el-checkbox v-model="isSelectAll" @change="selectAllChange">{{$t('selectAll')}}</el-checkbox>
      </div>
    </transition>
    <el-table class="ksd-el-table jobs-table"
      tooltip-effect="dark"
      border
      :data="jobsList"
      highlight-current-row
      :default-sort = "{prop: 'create_time', order: 'descending'}"
      @sort-change="sortJobList"
      @selection-change="handleSelectionChange"
      @select="handleSelect"
      @select-all="handleSelectAll"
      @cell-click="showLineSteps"
      :row-class-name="tableRowClassName"
      :style="{width:showStep?'70%':'100%'}"
    >
      <el-table-column type="selection" align="center" width="40"></el-table-column>
      <el-table-column align="center" width="40" prop="icon">
        <template slot-scope="scope">
          <common-tip :content="$t('openJobSteps')">
            <i :class="{
            'el-icon-ksd-dock_to_right_return': scope.row.id !== selectedJob.id || !showStep,
            'el-icon-ksd-dock_to_right': scope.row.id == selectedJob.id && showStep}"
            ></i>
          </common-tip>
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn" prop="job_name" width="140"></el-table-column>
      <el-table-column
        :label="$t('TargetSubject')"
        sortable
        min-width="140"
        show-overflow-tooltip
        prop="target_model_alias">
      </el-table-column>
       <el-table-column
        :label="$t('dataRange')"
        min-width="180"
        show-overflow-tooltip>
        <template slot-scope="scope">
          <span v-if="scope.row.data_range_end==9223372036854776000">{{$t('fullLoad')}}</span>
          <span v-else>{{scope.row.data_range_start | utcTime}} - {{scope.row.data_range_end | utcTime}}</span>
        </template>
      </el-table-column>
      <el-table-column
        width="180"
        :renderHeader="renderColumn2">
        <template slot-scope="scope">
          <kap-progress :percent="scope.row.step_ratio * 100 | number(0)" :status="scope.row.job_status"></kap-progress>
        </template>
      </el-table-column>
      <el-table-column
        width="218"
        :label="$t('startTime')"
        show-overflow-tooltip
        prop="create_time"
        sortable>
        <template slot-scope="scope">
          {{transToGmtTime(scope.row.create_time)}}
        </template>
      </el-table-column>
      <el-table-column
        width="105"
        sortable
        prop="duration"
        :label="$t('Duration')">
        <template slot-scope="scope">
          {{scope.row.duration/60 | number(2) }}  mins
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('Actions')"
        width="83">
        <template slot-scope="scope">
          <common-tip :content="$t('jobDrop')" v-if="scope.row.job_status=='DISCARDED' || scope.row.job_status=='FINISHED'">
            <i class="el-icon-ksd-table_delete ksd-fs-14" @click.stop="drop([scope.row.id])"></i>
          </common-tip>
          <common-tip :content="$t('jobRestart')" v-if="scope.row.job_status=='ERROR'|| scope.row.job_status=='STOPPED'||scope.row.job_status=='RUNNING'">
            <i class="el-icon-ksd-restart ksd-fs-14" @click.stop="restart([scope.row.id])"></i>
          </common-tip>
          <common-tip :content="$t('jobResume')" v-if="scope.row.job_status=='ERROR'|| scope.row.job_status=='STOPPED'">
            <i class="el-icon-ksd-table_resume ksd-fs-14" @click.stop="resume([scope.row.id])"></i>
          </common-tip>
          <common-tip :content="$t('jobPause')" v-if="scope.row.job_status=='RUNNING'|| scope.row.job_status=='PENDING'">
            <i class="el-icon-ksd-pause ksd-fs-14" @click.stop="pause([scope.row.id])"></i>
          </common-tip>
        </template>
      </el-table-column>
    </el-table>


    <kap-pager :totalSize="jobTotal"  v-on:handleCurrentChange='currentChange' ref="jobPager" class="ksd-mtb-10 ksd-center" ></kap-pager>

    <el-card v-show="showStep" class="card-width job-step" id="stepList">

      <div class="timeline-item">
        <div class="timeline-body">
          <table class="table table-striped table-bordered ksd-table" cellpadding="0" cellspacing="0">
            <tr>
              <td>{{$t('kylinLang.common.jobs')}} ID</td>
              <td class="single-line greyd0">
                {{selectedJob.id}}
              </td>
            </tr>
            <tr>
              <td>{{$t('TargetSubject')}}</td>
              <td>
                <a @click="gotoModelList(selectedJob.target_model_alias)">{{selectedJob.target_model_alias}}</a>
              </td>
            </tr>
            <tr>
              <td>{{$t('kylinLang.common.status')}}</td>
              <td>
                <el-tag
                  size="small"
                  :type="getJobStatusTag">
                  {{selectedJob.job_status}}
                </el-tag>
              </td>
            </tr>
            <tr>
              <td>{{$t('duration')}}</td>
              <td class="greyd0">{{selectedJob.duration/60 | number(2)}} mins</td>
            </tr>
          </table>
        </div>
      </div>
      <p class="time-hd">
        {{$t('jobDetails')}}
      </p>
      <ul class="timeline">

        <li v-for="(step, index) in selectedJob.details" :key="index" :class="{'finished' : step.step_status=='FINISHED'}">
          <el-popover
            placement="left"
            width="300"
            trigger="hover" popper-class="jobPoplayer">
            <i slot="reference" class="fa"
               :class="{
              'el-icon-ksd-more_05' : step.step_status=='PENDING',
              'el-icon-loading' : step.step_status=='WAITING' || step.step_status=='RUNNING',
              'el-icon-ksd-good_health' : step.step_status=='FINISHED',
              'el-icon-ksd-error_01' : step.step_status=='ERROR',
              'el-icon-ksd-table_discard' : step.step_status=='DISCARDED'
            }">
            </i>
            <ul >
              <li>SequenceID: {{step.sequence_id}}</li>
              <li>Status: {{step.step_status}}</li>
              <li>Duration: {{timerlineDuration(step)}}</li>
              <li>Start At: {{transToGmtTime(step.exec_start_time !=0 ? step.exec_start_time:'')}}</li>
              <li>End At: {{transToGmtTime(step.exec_end_time !=0 ? step.exec_end_time :'')}}</li>
              <li v-if="step.info.hdfs_bytes_written">Data Size: <span>{{ step.info.hdfs_bytes_written | dataSize}}</span></li>
            </ul>
          </el-popover>

          <div class="timeline-item timer-line">
            <div class="timeline-header ">
              <p class="stepname single-line">{{step.name}}</p>
            </div>
            <div class="timeline-body">
              <span class="steptime jobActivityLabel" v-if="step.exec_start_time && step.exec_end_time">
                <i class="el-icon-time"></i>
                {{transToGmtTime(step.exec_start_time!=0? step.exec_start_time: '')}}
              </span>

              <div v-if="step.info.hdfs_bytes_written">
                <span class="jobActivityLabel">Data Size: </span>
                <span>{{step.info.hdfs_bytes_written|dataSize}}</span>
              </div>
              <div>
                <span class="jobActivityLabel">{{$t('duration')}}: </span>
                <span v-if="step.exec_start_time && step.exec_end_time">{{timerlineDuration(step)}}</span>
                <span v-if="!step.exec_start_time || !step.exec_end_time">
                  <img src="../../assets/img/dot.gif" height="12px" width="10px"/>
                </span>
                <br />
              </div>
            </div>
            <div class="timeline-footer">
              
              <!-- <i name="key" v-if="step.exec_cmd" class="el-icon-ksd-paramters" @click="clickKey(step)"></i> -->
              <common-tip :content="$t('sparkJobTip')">
                <a :href="step.info.yarn_application_tracking_url" target="_blank">
                    <i name="tasks" v-if="step.info.yarn_application_tracking_url" class="el-icon-ksd-export"></i>
                </a>
              </common-tip>
              <common-tip :content="$t('logInfoTip')">
                <i name="file" v-if="step.step_status!='PENDING'" class="el-icon-ksd-details ksd-ml-4" @click="clickFile(step)"></i>
              </common-tip>
            </div>
          </div>
        </li>
      </ul>
      <div class='job-btn' @click='showStep=false'><i class='el-icon-d-arrow-right' aria-hidden='true'></i>
      </div>
    </el-card>
    <el-dialog :title="$t('waitingJobList')" :close-on-press-escape="false" :close-on-click-modal="false" :visible.sync="waitingJobListVisibel" width="480px">
      <div v-if="waitingJob">
        <div style="height:14px;line-height:14px;">
          <span class="ksd-title-label ksd-fs-14">{{$t('jobTarget')}}</span><span class="ky-title-color">{{waitingJob.modelName}}</span>
        </div>
        <el-table :data="waitingJob.jobsList" border class="ksd-mt-10">
          <el-table-column type="index" :label="$t('order')" width="60" :resizable="false"></el-table-column>
          <el-table-column property="job_type" :label="$t('JobType')" show-overflow-tooltip :resizable="false"></el-table-column>
          <el-table-column property="create_time" :label="$t('triggerTime')" width="218" :resizable="false">
            <template slot-scope="scope">
              {{transToGmtTime(scope.row.create_time)}}
            </template>
          </el-table-column>
        </el-table>
        <kap-pager :totalSize="waitingJob.jobsSize" v-if="waitingJob.jobsSize>10" v-on:handleCurrentChange='waitingJobsCurrentChange' ref="waitingJobPager" class="ksd-mtb-10 ksd-center" ></kap-pager>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" plain size="medium" @click="waitingJobListVisibel = false">{{$t('kylinLang.common.ok')}}</el-button>
      </span>
    </el-dialog>
    <el-dialog
      id="show-diagnos"
      :title="stepAttrToShow == 'cmd' ? $t('parameters') : $t('output')"
      :visible.sync="dialogVisible"
      :close-on-press-escape="false"
      :close-on-click-modal="false">
      <job_dialog :stepDetail="outputDetail"></job_dialog>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" plain size="medium" @click="dialogVisible = false">{{$t('kylinLang.common.close')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import jobDialog from './job_dialog'
import TWEEN from '@tweenjs/tween.js'
import $ from 'jquery'
import { pageCount } from '../../config'
import { transToGmtTime, kapConfirm, handleError, handleSuccess } from 'util/business'
@Component({
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      loadJobsList: 'LOAD_JOBS_LIST',
      getJobDetail: 'GET_JOB_DETAIL',
      loadStepOutputs: 'LOAD_STEP_OUTPUTS',
      removeJob: 'REMOVE_JOB',
      pauseJob: 'PAUSE_JOB',
      restartJob: 'RESTART_JOB',
      resumeJob: 'RESUME_JOB',
      losdWaittingJobModels: 'LOAD_WAITTING_JOB_MODELS',
      laodWaittingJobsByModel: 'LOAD_WAITTING_JOBS_BY_MODEL'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    'job_dialog': jobDialog
  },
  locales: {
    'en': {
      dataRange: 'Data Range',
      JobType: 'Job Type',
      JobName: 'Job Name',
      TargetSubject: 'Target Subject',
      ProgressStatus: 'Job Status',
      startTime: 'Start Time',
      Duration: 'Duration',
      Actions: 'Actions',
      jobResume: 'Resume',
      jobDiscard: 'Discard',
      jobPause: 'Pause',
      jobDrop: 'Drop',
      jobRestart: 'Restart',
      tip_jobResume: 'Resume the Job',
      tip_jobPause: 'Pause the Job',
      tip_jobDiscard: 'Discard the Job',
      cubeName: 'Cube Name',
      NEW: 'NEW',
      PENDING: 'PENDING',
      RUNNING: 'RUNNING',
      FINISHED: 'FINISHED',
      ERROR: 'ERROR',
      DISCARDED: 'DISCARDED',
      STOPPED: 'STOPPED',
      LASTONEDAY: 'LAST ONE DAY',
      LASTONEWEEK: 'LAST ONE WEEK',
      LASTONEMONTH: 'LAST ONE MONTH',
      LASTONEYEAR: 'LAST ONE YEAR',
      ALL: 'ALL',
      parameters: 'Parameters',
      output: 'Output',
      load: 'Loading ... ',
      cmdOutput: 'cmd_output',
      resumeJob: 'Are you sure to resume the job(s) below?',
      resumeJobTitle: 'Resume Job',
      restartJob: 'Are you sure to restart the job(s) below?',
      restartJobTitle: 'Restart Job',
      pauseJob: 'Are you sure to pause the job(s) below?',
      pauseJobTitle: 'Pause Job',
      dropJob: 'Are you sure to drop the job(s) below?',
      dropJobTitle: 'Drop Job',
      jobName: 'Job Name',
      duration: 'Duration',
      waiting: 'Waiting',
      noSelectJobs: 'Please check at least one job.',
      selectedJobs: '{selectedNumber} jobs have been selected',
      selectAll: 'All Select',
      fullLoad: 'Full Load',
      jobDetails: 'Job Details',
      waitingjobs: 'Waiting Jobs',
      jobs: 'Job(s)',
      waitingJobList: 'Waiting Job List',
      triggerTime: 'Trigger Time',
      order: 'Order',
      jobTarget: 'Job Target:',
      jobsList: 'Jobs List',
      sparkJobTip: 'Spark Job',
      logInfoTip: 'Log Output',
      openJobSteps: 'Open Job Steps'
    },
    'zh-cn': {
      dataRange: '数据范围',
      JobType: 'Job 类型',
      JobName: '任务',
      TargetSubject: '任务对象',
      ProgressStatus: '任务状态',
      startTime: '任务开始时间',
      Duration: '耗时',
      Actions: '操作',
      jobResume: '恢复',
      jobDiscard: '终止',
      jobPause: '暂停',
      jobDrop: '删除',
      jobRestart: '重启',
      tip_jobResume: '恢复Job',
      tip_jobPause: '暂停Job',
      tip_jobDiscard: '终止Job',
      cubeName: 'Cube 名称',
      NEW: '新建',
      PENDING: '等待',
      RUNNING: '运行',
      FINISHED: '完成',
      ERROR: '错误',
      DISCARDED: '终止',
      STOPPED: '暂停',
      LASTONEDAY: '最近一天',
      LASTONEWEEK: '最近一周',
      LASTONEMONTH: '最近一月',
      LASTONEYEAR: '最近一年',
      ALL: '所有',
      parameters: '参数',
      output: '输出',
      load: '下载中 ... ',
      cmdOutput: 'cmd_output',
      resumeJob: '确定要恢复以下任务?',
      resumeJobTitle: '恢复任务',
      restartJob: '确定要重启以下任务?',
      restartJobTitle: '重启任务',
      pauseJob: '确定要暂停以下任务?',
      pauseJobTitle: '暂停任务',
      dropJob: '确定要删除以下任务？',
      dropJobTitle: '删除任务',
      jobName: '任务名',
      duration: '持续时间',
      waiting: '等待时间',
      noSelectJobs: '请勾选至少一项任务。',
      selectedJobs: '目前已选择当页{selectedNumber}条任务。',
      selectAll: '全选',
      fullLoad: '全量加载',
      jobDetails: '任务详情',
      waitingjobs: '个任务在等待',
      jobs: '个任务',
      waitingJobList: '等待任务列表',
      triggerTime: '触发时间',
      order: '排序',
      jobTarget: '任务目标：',
      jobsList: '任务列表',
      sparkJobTip: 'Spark任务详情',
      logInfoTip: '日志详情',
      openJobSteps: '展开任务详情'
    }
  }
})
export default class JobsList extends Vue {
  project = localStorage.getItem('selected_project')
  filterName = ''
  filterStatus = []
  lockST = null
  scrollST = null
  stCycle = null
  showStep = false
  selectedJob = {}
  dialogVisible = false
  outputDetail = ''
  stepAttrToShow = ''
  beforeScrollPos = 0
  multipleSelection = []
  isPausePolling = false
  isSelectAllShow = false
  isSelectAll = false
  selectedNumber = 0
  idsArr = []
  idsArrCopy = []
  filter = {
    pageOffset: 0,
    pageSize: pageCount,
    timeFilter: 4,
    jobNames: [],
    sortBy: 'create_time',
    reverse: true,
    status: '',
    subjectAlias: ''
  }
  waittingJobsFilter = {
    offset: 0,
    limit: 10
  }
  jobsList = []
  jobTotal = 0
  allStatus = ['ALL', 'PENDING', 'RUNNING', 'FINISHED', 'ERROR', 'DISCARDED', 'STOPPED']
  jobTypeFilteArr = ['INDEX_REFRESH', 'INDEX_MERGE', 'INDEX_BUILD', 'INDEX_RECONSTRUCT']
  targetId = ''
  searchLoading = false
  batchBtnsEnabled = {
    resume: false,
    restart: false,
    pause: false,
    drop: false
  }
  waitingJobListVisibel = false
  waitingJob = {modelName: '', jobsList: [], jobsSize: 0}
  waittingJobModels = {size: 0, data: null}
  handleCommand (uuid) {
    this.waitingJobListVisibel = true
    this.waittingJobsFilter.project = this.currentSelectedProject
    this.waittingJobsFilter.model = uuid
    this.waitingJob.modelName = this.waittingJobModels.data[uuid].model_alias
    this.laodWaittingJobsByModel(this.waittingJobsFilter).then((res) => {
      handleSuccess(res, (data) => {
        this.waitingJob.jobsList = data.data
        this.waitingJob.jobsSize = data.size
      })
    })
  }
  getBatchBtnStatus (statusArr) {
    const batchBtns = {
      resume: ['ERROR', 'STOPPED'],
      restart: ['ERROR', 'STOPPED', 'RUNNING'],
      pause: ['PENDING', 'RUNNING'],
      drop: ['DISCARDED', 'FINISHED']
    }
    $.each(batchBtns, (key, item) => {
      this.batchBtnsEnabled[key] = this.isContain(item, statusArr)
    })
  }
  isContain (arr1, arr2) {
    for (let i = arr2.length - 1; i >= 0; i--) {
      if (!arr1.includes(arr2[i])) {
        return false
      }
    }
    return true
  }
  gotoModelList (modelAlias) {
    this.$router.push({name: 'ModelList', params: { modelAlias: modelAlias }})
  }
  renderColumn (h) {
    let items = []
    for (let i = 0; i < this.jobTypeFilteArr.length; i++) {
      items.push(<el-checkbox label={this.jobTypeFilteArr[i]} key={this.jobTypeFilteArr[i]}></el-checkbox>)
    }
    return (<span>
      <span>{this.$t('JobType')}</span>
      <el-popover
        ref="jobTypeFilterPopover"
        placement="bottom"
        popperClass="filter-popover">
        <el-checkbox-group class="filter-groups" value={this.filter.jobNames} onInput={val => (this.filter.jobNames = val)} onChange={this.filterChange2}>
          {items}
        </el-checkbox-group>
        <i class={this.filter.jobNames.length ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
      </el-popover>
    </span>)
  }
  renderColumn2 (h) {
    let items = []
    for (let i = 0; i < this.allStatus.length; i++) {
      items.push(
        <div onClick={() => { this.filterChange2(this.allStatus[i]) }}>
          <el-dropdown-item class={this.allStatus[i] === this.filter.status ? 'active' : ''} key={i}>{this.$t(this.allStatus[i])}</el-dropdown-item>
        </div>
      )
    }
    return (<span>
      <span>{this.$t('ProgressStatus')}</span>
      <el-dropdown hide-on-click={false} trigger="click">
        <i class={(this.filter.status && this.filter.status !== 'ALL') ? 'el-icon-ksd-filter el-dropdown-link isFilter' : 'el-icon-ksd-filter el-dropdown-link'}></i>
        <template slot="dropdown">
          <el-dropdown-menu class="jobs-dropdown">
            {items}
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </span>)
  }
  autoFilter () {
    clearTimeout(this.stCycle)
    this.stCycle = setTimeout(() => {
      if (!this.isPausePolling) {
        this.refreshJobs().then((res) => {
          handleSuccess(res, (data) => {
            if (this._isDestroyed) {
              return
            }
            this.autoFilter()
          })
        }, (res) => {
          handleError(res)
        })
      }
    }, 5000)
  }
  created () {
    const { modelAlias, jobStatus } = this.$route.query
    modelAlias && (this.filter.subject = modelAlias)
    jobStatus && (this.filter.status = jobStatus)
    this.selectedJob = {} // 防止切换project时，发一个不存在该项目jobId的jobDetail的请求
    this.filter.project = this.currentSelectedProject
    if (this.currentSelectedProject) {
      this.autoFilter()
      this.getJobsList()
      this.getWaittingJobModels()
    }
  }
  destroyed () {
    clearTimeout(this.stCycle)
  }
  mounted () {
    window.addEventListener('click', this.closeIt)
    if (document.getElementById('scrollBox')) {
      document.getElementById('scrollBox').addEventListener('scroll', this.scrollRightBar, false)
    }
  }
  beforeDestroy () {
    window.clearTimeout(this.stCycle)
    window.clearTimeout(this.scrollST)
    window.removeEventListener('click', this.closeIt)
    if (document.getElementById('scrollBox')) {
      document.getElementById('scrollBox').removeEventListener('scroll', this.scrollRightBar, false)
    }
  }
  getJobsList () {
    return new Promise((resolve, reject) => {
      this.loadJobsList(this.filter).then((res) => {
        handleSuccess(res, (data) => {
          if (data.size) {
            this.jobsList = data.jobList.map((m) => {
              if (this.selectedJob) {
                if (m.id === this.selectedJob.id) {
                  this.getJobDetail({project: this.filter.project, jobId: m.id}).then((res) => {
                    handleSuccess(res, (data) => {
                      this.selectedJob = m
                      this.selectedJob['details'] = data
                    })
                  }, (resError) => {
                    handleError(resError)
                  })
                }
              }
              return m
            })
            this.jobTotal = data.size
          } else {
            this.jobsList = []
            this.jobTotal = 0
          }
          this.searchLoading = false
          resolve()
        })
      }, (res) => {
        handleError(res)
        this.searchLoading = false
        reject()
      })
    })
  }
  getWaittingJobModels () {
    return new Promise((resolve, reject) => {
      this.losdWaittingJobModels({project: this.filter.project}).then((res) => {
        handleSuccess(res, (data) => {
          this.$nextTick(() => {
            this.waittingJobModels = data
          })
          resolve()
        })
      }, (res) => {
        handleError(res)
        reject()
      })
    })
  }
  get getJobStatusTag () {
    if (this.selectedJob.job_status === 'PENDING') {
      return 'gray'
    }
    if (this.selectedJob.job_status === 'RUNNING') {
      return 'primary'
    }
    if (this.selectedJob.job_status === 'FINISHED') {
      return 'success'
    }
    if (this.selectedJob.job_status === 'ERROR') {
      return 'danger'
    }
    if (this.selectedJob.job_status === 'DISCARDED') {
      return ''
    }
  }
  scrollRightBar () {
    clearTimeout(this.scrollST)
    this.scrollST = setTimeout(() => {
      if (this.showStep) {
        var sTop = document.getElementById('scrollBox').scrollTop
        if (sTop < this.beforeScrollPos) {
          var result = sTop
          if (sTop < 90) {
            result = 90
          }
          document.getElementById('stepList').style.top = result + 'px'
        }
        if (sTop === 0) {
          this.beforeScrollPos = 0
        }
      }
    }, 400)
  }
  animatedNum (newValue, oldValue) {
    new TWEEN.Tween({
      number: oldValue
    })
    .to({
      number: newValue
    }, 500)
    .onUpdate(tween => {
      this.selectedNumber = tween.number.toFixed(0)
    })
    .start()
    function animate () {
      if (TWEEN.update()) {
        requestAnimationFrame(animate)
      }
    }
    animate()
  }
  reCallPolling () {
    this.isPausePolling = false
    this.autoFilter()
  }
  handleSelectionChange (val) {
    if (val && val.length) {
      this.multipleSelection = val
      this.isPausePolling = true
      const selectedStatus = this.multipleSelection.map((item) => {
        return item.job_status
      })
      this.getBatchBtnStatus(selectedStatus)
      this.idsArr = this.multipleSelection.map((item) => {
        return item.id
      })
    } else {
      this.reCallPolling()
      this.batchBtnsEnabled = {
        resume: false,
        discard: false,
        pause: false,
        drop: false
      }
      this.idsArr = []
    }
  }
  handleSelectAll (val) {
    if (this.jobTotal > 10 && this.filter.status !== '') {
      this.isSelectAllShow = !this.isSelectAllShow
      this.isSelectAll = false
      this.selectedNumber = this.animatedNum(this.jobsList.length, 0)
    }
  }
  handleSelect (val) {
    if (this.jobTotal > 10 && this.filter.status !== '') {
      if (this.multipleSelection.length < 10) {
        this.isSelectAllShow = false
      } else {
        this.isSelectAllShow = true
      }
    }
  }
  selectAllChange (val) {
    if (val) {
      this.selectAll()
    } else {
      this.cancelSelectAll()
    }
  }
  selectAll () {
    this.idsArrCopy = this.idsArr
    this.idsArr = []
    this.animatedNum(this.jobTotal, this.jobsList.length)
  }
  cancelSelectAll () {
    this.idsArr = this.idsArrCopy
    this.animatedNum(this.jobsList.length, this.jobTotal)
  }
  getJobIds () {
    const jobIds = this.multipleSelection.map((item) => {
      return item.id
    })
    return jobIds
  }
  getJobNames () {
    const jobNames = this.multipleSelection.map((item) => {
      return item.name
    })
    return jobNames
  }
  batchResume () {
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.resume([], 'batchAll')
      } else {
        const jobIds = this.getJobIds()
        this.resume(jobIds, 'batch')
      }
    }
  }
  batchRestart () {
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.restart([], 'batchAll')
      } else {
        const jobIds = this.getJobIds()
        this.restart(jobIds, 'batch')
      }
    }
  }
  batchPause () {
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.pause([], 'batchAll')
      } else {
        const jobIds = this.getJobIds()
        this.pause(jobIds, 'batch')
      }
    }
  }
  batchDrop () {
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.drop([], 'batchAll')
      } else {
        const jobIds = this.getJobIds()
        this.drop(jobIds, 'batch')
      }
    }
  }
  resetSelection () {
    this.isSelectAllShow = false
    this.isSelectAll = false
    this.multipleSelection = []
    this.idsArrCopy = []
    this.idsArr = []
  }
  currentChange (size, count) {
    this.filter.pageOffset = size
    this.filter.pageSize = count
    this.resetSelection()
    this.getJobsList()
    this.closeIt()
  }
  waitingJobsCurrentChange (size, count) {
    this.waittingJobsFilter.offset = size
    this.waittingJobsFilter.limit = count
    this.getWaittingJobModels()
  }
  closeIt () {
    if (this.showStep) {
      this.showStep = false
    }
  }
  filterChange (val) {
    this.searchLoading = true
    clearTimeout(this.lockST)
    this.lockST = setTimeout(() => {
      this.refreshJobs()
      this.showStep = false
    }, 1000)
  }
  filterChange2 (status) {
    if (this.allStatus.indexOf(status) !== -1) {
      this.filter.status = status === 'ALL' ? '' : status
    }
    this.refreshJobs()
    this.showStep = false
  }
  tableRowClassName ({row, rowIndex}) {
    if (row.id === this.selectedJob.id && this.showStep) {
      return 'current-row2'
    }
  }
  refreshJobs () {
    this.filter.project = this.currentSelectedProject
    return Promise.all([this.getJobsList(), this.getWaittingJobModels()])
  }
  sortJobList ({ column, prop, order }) {
    if (order === 'ascending') {
      this.filter.reverse = false
    } else {
      this.filter.reverse = true
    }
    this.filter.sortBy = prop
    this.currentSelectedProject && this.getJobsList()
  }
  resume (jobIds, isBatch) {
    kapConfirm(this._renderConfirmContent(this.$t('resumeJob'), jobIds), null, this.$t('resumeJobTitle')).then(() => {
      this.resumeJob({jobIds: jobIds, project: this.currentSelectedProject, action: 'RESUME', status: this.filter.status}).then(() => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.actionSuccess')
        })
        if (isBatch) {
          if (isBatch === 'batchAll') {
            this.filter.status = ''
          }
          this.reCallPolling()
          this.resetSelection()
        } else {
          this.refreshJobs()
        }
      }).catch((res) => {
        handleError(res)
      })
    })
  }
  restart (jobIds, isBatch) {
    kapConfirm(this._renderConfirmContent(this.$t('restartJob'), jobIds), null, this.$t('restartJobTitle')).then(() => {
      this.restartJob({jobIds: jobIds, project: this.currentSelectedProject, action: 'RESTART', status: this.filter.status}).then(() => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.actionSuccess')
        })
        if (isBatch) {
          if (isBatch === 'batchAll') {
            this.filter.status = ''
          }
          this.reCallPolling()
          this.resetSelection()
        } else {
          this.refreshJobs()
        }
      }).catch((res) => {
        handleError(res)
      })
    })
  }
  pause (jobIds, isBatch) {
    kapConfirm(this._renderConfirmContent(this.$t('pauseJob'), jobIds), null, this.$t('pauseJobTitle')).then(() => {
      this.pauseJob({jobIds: jobIds, project: this.currentSelectedProject, action: 'PAUSE', status: this.filter.status}).then(() => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.actionSuccess')
        })
        if (isBatch) {
          if (isBatch === 'batchAll') {
            this.filter.status = ''
          }
          this.reCallPolling()
          this.resetSelection()
        } else {
          this.refreshJobs()
        }
      }).catch((res) => {
        handleError(res)
      })
    })
  }
  _renderConfirmContent (confirmMessage, jobIds) {
    return (
      <div>
        <p class="break-all ksd-mb-4">{confirmMessage}</p>
        {
          jobIds.map((kId) => {
            return <p>[{kId}]</p>
          })
        }
      </div>
    )
  }
  drop (jobIds, isBatch) {
    kapConfirm(this._renderConfirmContent(this.$t('dropJob'), jobIds), null, this.$t('dropJobTitle')).then(() => {
      this.removeJob({jobIds: jobIds, project: this.currentSelectedProject, status: this.filter.status}).then(() => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
        if (isBatch) {
          if (isBatch === 'batchAll') {
            this.filter.status = ''
          }
          this.reCallPolling()
          this.resetSelection()
        } else {
          this.refreshJobs()
        }
      }).catch((res) => {
        handleError(res)
      })
    })
  }
  showLineSteps (row, column, cell) {
    if (column.property === 'icon') {
      var needShow = false
      if (row.id !== this.selectedJob.id) {
        needShow = true
      } else {
        needShow = !this.showStep
      }
      this.showStep = needShow
      this.selectedJob = row
      this.getJobDetail({project: this.currentSelectedProject, jobId: row.id}).then((res) => {
        handleSuccess(res, (data) => {
          this.$nextTick(() => {
            this.$set(this.selectedJob, 'details', data)
            var sTop = document.getElementById('scrollBox').scrollTop
            this.beforeScrollPos = sTop
            var result = sTop
            if (sTop < 90) {
              result = 90
            }
            document.getElementById('stepList').style.top = result + 'px'
          })
        }, (resError) => {
          handleError(resError)
        })
      })
    }
  }
  clickKey (step) {
    this.stepAttrToShow = 'cmd'
    this.outputDetail = step.exec_cmd
    this.dialogVisible = true
  }
  clickFile (step) {
    this.stepAttrToShow = 'output'
    this.dialogVisible = true
    this.outputDetail = this.$t('load')
    this.loadStepOutputs({jobId: this.selectedJob.id, stepId: step.id, project: this.currentSelectedProject}).then((res) => {
      handleSuccess(res, (data) => {
        this.outputDetail = data.cmd_output
      })
    }, (resError) => {
      this.outputDetail = this.$t('cmdOutput')
    })
  }
  timerlineDuration (step) {
    let min = 0
    if (!step.exec_start_time || !step.exec_end_time) {
      return '0 seconds'
    } else {
      min = (step.exec_end_time - step.exec_start_time) / 1000 / 60
      return min.toFixed(2) + ' mins'
    }
  }
  closeLoginOpenKybot () {
    this.kyBotUploadVisible = false
    this.infoKybotVisible = true
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  .jobs_list {
    #show-diagnos .el-textarea__inner:focus {
      border-color: @line-border-color;
    }
    .jobs_tools_row {
      font-size: 0px;
    }
    .show-search-btn {
      width: 300px;
    }
    .fade-enter-active, .fade-leave-active {
      transition: opacity .5s;
    }
    .fade-enter, .fade-leave-to /* .fade-leave-active below version 2.1.8 */ {
      opacity: 0;
    }
    .selectLabel {
      background-color: @base-color-10;
      height: 32px;
      line-height: 32px;
      margin: 10px 0;
      padding-left: 10px;
      color: @text-title-color;
      .el-checkbox__label {
        color: @text-title-color;
      }
    }
    .action_groups {
      vertical-align: top;
      .el-button.el-button--default {
        background: #fff;
        border-color: @base-color;
        color: @base-color;
        z-index: 1;
        &:hover {
          background: @base-color;
          color: @fff;
        }
      }
      .el-button.is-disabled.is-plain, .el-button.is-disabled.is-plain:hover {
        border-color: @line-border-color;
        background-color: @background-disabled-color;
        z-index: 0;
        color: @text-disabled-color;
      }
    }
    .waiting-jobs {
      color: @text-title-color;
      &:hover {
        .el-icon-arrow-down {
          color: @base-color;
        }
      }
      .el-button.is-disabled {
        .el-icon-arrow-down {
          color: inherit;
          cursor: not-allowed;
        }
        &:hover {
          .el-icon-arrow-down {
            color: inherit;
          }
        }
      }
    }
    .el-progress-bar__innerText {
      top: -1px;
      position: relative;
    }
    .job-step {
      width: 30%;
      min-height: calc(~'100vh - 167px');
      box-sizing: border-box;
      z-index: 100;
      position: absolute;
      top: 0;
      right: 0;
        &.el-card {
          border-radius: 0;
          padding: 15px;
          .el-card__body {
            padding: 0;
          }
        }
      .table-bordered {
        border: 1px solid @border-color-base;
        tr{
           td {
              border-bottom: 1px solid @border-color-base;
              word-break: break-word;
            }
          &:last-child {
            td {
              border-bottom: 0;
            }
          }
          &:nth-child(odd) {
            td {
              background-color: @table-stripe-color;
            }
          }
          td:first-child{
            width: 25%;
            font-weight: 500;
          }
        }
      }
      .time-hd {
        height:20px;
        line-height:20px;
        margin:15px 0 10px 0;
        font-size: 14px;
        font-weight: 500;
      }
      .job-btn {
        position: absolute;
        left: 0px;
        top: 310px;
        height: 70px;
        width: 13px;
        line-height: 70px;
        padding-left: 0px;
        font-size: 12px;
        border-radius: 0;
        background-color: @base-color-9;
        border: 1px solid @border-color-base;
        cursor: pointer;
        i {
          position: relative;
          left: 1px;
        }
      }
      .timeline {
        position: relative;
        margin: 0 10px 30px 10px;
        list-style: none;
        font-size: 12px;

        .jobPoplayer.el-popover[x-placement^=left] .popper__arrow{
          border-left-color: #333;
          &:after{
           border-left-color:#393e53;
         }
        }
        > li.time-label > span {
          padding: 5px;
          display: inline-block;
          border-radius: 4px;
          color: #fff;
        }
        > li:before, > li:after {
          content: '';
          position: absolute;
          top: 23px;
          bottom: 0;
          width: 3px;
          background: @border-color-base;
          left: 8px;
          border-radius: 2px;
        }
        > li {
          &:last-child:before,
          &:last-child:after{
            display: none;
          }
          &.finished:before,
          &.finished:after {
            background: @color-success;
          }
          position: relative;
          margin-right: 10px;
          padding-bottom: 10px;
          .timeline-item {
            position: relative;
            margin-left: 30px;
            border-radius: 3px;
            .time {
              float: right;
              padding: 10px;
              color: #999;
              font-size: 12px;
            }
            .timeline-header {
              margin: 0;
              padding: 0 10px 0 0;
              .single-line.stepname {
                max-width: 300px;
                word-wrap: break-word;
                word-break: normal;
                font-size:14px;
                font-weight: 500;
              }
            }
            .timeline-footer {
              padding: 0 10px 4px 0;
              i {
                font-size: 16px;
                color: @color-primary;
                margin-left: 5px;
                &:first-child {
                  margin-left: 0;
                }
                &:hover {
                  background-color: @base-color-9;
                }
              }
            }
            .timeline-body {
              padding: 4px 10px 10px 0;
              .steptime {
                height:20px;
                line-height:20px;
              }
            }
          }
          > span > .fa, > .fa {
            width: 20px;
            height: 20px;
            font-size: 10px;
            line-height: 23px;
            position: absolute;
            border-radius: 50%;
            text-align: center;
            font-size: 20px;
            color: @color-info;
            &.el-icon-ksd-good_health {
              color: @color-success;
            }
            &.el-icon-ksd-error_01 {
              color: @color-danger;
            }
            &.el-icon-loading {
              border: 1px solid @color-primary;
              color: @color-primary;
              font-size: 14px;
              width: 20px;
              height: 20px;
            }
          }
        }
        li:last-child {
          position: relative;
        }
      }
    }
    .jobs-table {
      .el-icon-ksd-filter {
        position: relative;
        left: 5px;
        &.isFilter,
        &:hover {
          color: @base-color;
        }
      }
      th .el-dropdown {
        padding: 0;
        line-height: 0;
        position: relative;
        left: 5px;
        top: 2px;
        .el-icon-ksd-filter {
          float: none;
          position: relative;
          left: 0px;
        }
      }
      .el-icon-ksd-dock_to_right_return,
      .el-icon-ksd-dock_to_right,
      .el-icon-ksd-table_delete,
      .el-icon-ksd-restart,
      .el-icon-ksd-table_resume,
      .el-icon-ksd-pause {
        &:hover {
          color: @base-color;
        }
      }
      tr.current-row2 > td{
        background: @base-color-9;
      }
      tr .el-icon-arrow-right {
        position:absolute;
        left:15px;
        top:50%;
        transform:translate(0,-50%);
        font-size:12px;
      }
    }
  }
  .jobs-dropdown {
    min-width: 80px;
    .el-dropdown-menu__item.active {
      background-color: @base-color-9;
      color: @base-color-2;
    }
  }
</style>
