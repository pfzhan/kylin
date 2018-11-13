<template>
  <div class="jobs_list ksd-mrl-20" @click.stop>
    <el-row :gutter="20" class="jobs_tools_row ksd-mt-10 ksd-mb-10">
      <el-col  :xs="12" :md="12" :lg="12">
        <el-select v-model="filter.status" @change="filterChange2" style="width:140px;" size="medium">
          <el-option
            v-for="(status, status_index) in allStatus"
            :key="status_index"
            :label="$t(status.name)"
            :value="status.value">
          </el-option>
        </el-select>
        <el-button-group class="action_groups ksd-ml-10">
          <el-button plain size="medium" :disabled="!(filter.status==8 || filter.status==32) || filter.status == ''" @click="batchResume">{{$t('jobResume')}}</el-button>
          <el-button plain size="medium" :disabled="filter.status==16 || filter.status==4 || filter.status == ''" @click="batchDiscard">{{$t('jobDiscard')}}</el-button>
          <el-button plain size="medium" :disabled="!(filter.status==2 || filter.status==1) || filter.status == ''" @click="batchPause">{{$t('jobPause')}}</el-button>
          <el-button plain size="medium" :disabled="!(filter.status==16 || filter.status==4) || filter.status == ''" @click="batchDrop">{{$t('jobDrop')}}</el-button>
        </el-button-group>
        <el-button plain size="medium" class="ksd-ml-20" icon="el-icon-refresh" @click="refreshJobs">{{$t('kylinLang.common.refresh')}}</el-button>
      </el-col>
      <el-col  :xs="24" :md="12" :lg="12">
        <el-input :placeholder="$t('kylinLang.common.pleaseFilter')" v-model="filter.jobName"  @input="filterChange" class="show-search-btn ksd-fright" size="medium" prefix-icon="el-icon-search">
        </el-input>
      </el-col>
    </el-row>
    <transition name="fade">
      <div class="selectLabel" v-if="isSelectAllShow">
        <span>{{$t('selectedJobs', {selectedNumber: selectedNumber})}}</span>
        <el-checkbox v-model="isSelectAll">{{$t('selectAll')}}</el-checkbox>
      </div>
    </transition>
    <el-table class="ksd-el-table jobs-table"
      tooltip-effect="dark"
      border
      :data="jobsList"
      highlight-current-row
      @row-click="showLineSteps"
      @sort-change="sortJobList"
      @selection-change="handleSelectionChange"
      @select="handleSelect"
      @select-all="handleSelectAll"
      :row-class-name="tableRowClassName"
      :style="{width:showStep?'70%':'100%'}"
    >
      <!-- :default-sort="{prop: 'jobname', order: 'descending'}" -->
      <el-table-column type="selection" align="center" width="55"></el-table-column>
      <el-table-column
        :label="$t('JobType')"
        sortable
        :width="200"
      >
        <template slot-scope="scope">
          <i class="el-icon-arrow-right" ></i> {{scope.row.job_name}}
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('TableModelCube')"
        sortable
        :min-width="180"
        show-overflow-tooltip
        prop="target_subject">
      </el-table-column>
       <el-table-column
        :label="$t('dataRange')"
        sortable
        :min-width="180"
        show-overflow-tooltip>
        <template slot-scope="scope">
            {{transToGmtTime(scope.row.data_range_start)}} - {{transToGmtTime(scope.row.data_range_end)}}
        </template>
      </el-table-column>
      <el-table-column
        :width="180"
        :label="$t('ProgressStatus')">
        <template slot-scope="scope">
          <kap-progress :percent="scope.row.step_ratio * 100 | number(0)" :status="scope.row.job_status"></kap-progress>
        </template>
      </el-table-column>
      <el-table-column
        :width="230"
        :label="$t('startTime')"
        show-overflow-tooltip
        sortable>
        <template slot-scope="scope">
          {{scope.row.gmtTime}}
        </template>
      </el-table-column>
      <el-table-column
        :width="140"
        :label="$t('Duration')">
        <template slot-scope="scope">
          {{scope.row.duration/60000 | number(2) }}  mins
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('Actions')"
        align="center"
        width="100">
        <template slot-scope="scope">
          <common-tip :content="$t('jobDrop')" v-if="scope.row.job_status=='DISCARDED' || scope.row.job_status=='FINISHED'">
            <i class="el-icon-delete ksd-fs-16" @click.stop="drop([scope.row.id])"></i>
          </common-tip>
          <common-tip :content="$t('jobDiscard')" v-if="scope.row.job_status=='PENDING' || scope.row.job_status=='RUNNING'">
            <i class="el-icon-ksd-table_discard ksd-fs-16" @click.stop="discard([scope.row.id])"></i>
          </common-tip>
          <common-tip :content="$t('jobResume')" v-if="scope.row.job_status=='ERROR'|| scope.row.job_status=='STOPPED'">
            <i class="el-icon-ksd-table_resure ksd-fs-16" @click.stop="resume([scope.row.id])"></i>
          </common-tip>
          <el-dropdown trigger="click">
            <span class="el-dropdown-link" @click.stop>
              <common-tip :content="$t('kylinLang.common.moreActions')">
                <i class="el-icon-ksd-table_others ksd-fs-16"></i>
              </common-tip>
            </span>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item @click.native="discard([scope.row.id])" v-if="scope.row.job_status=='NEW' || scope.row.job_status=='ERROR' || scope.row.job_status=='STOPPED'">{{$t('jobDiscard')}}</el-dropdown-item>
              <el-dropdown-item @click.native="pause([scope.row.id])" v-if="scope.row.job_status=='RUNNING' || scope.row.job_status=='NEW' || scope.row.job_status=='PENDING'">{{$t('jobPause')}}</el-dropdown-item>
              <el-dropdown-item @click.native="diagnosisJob(scope.row, scope.row.id)">{{$t('jobDiagnosis')}}</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </template>
      </el-table-column>
    </el-table>


    <pager :totalSize="jobTotal"  v-on:handleCurrentChange='currentChange' ref="jobPager" class="ksd-mb-20" ></pager>

    <el-card v-show="showStep" class="card-width job-step" id="stepList">

      <div class="timeline-item">
        <div class="timeline-body">
          <table class="table table-striped table-bordered ksd-table" cellpadding="0" cellspacing="0">
            <tr>
              <td>Job ID</td>
              <td class="single-line greyd0">
                {{selectedJob.id}}
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
              <td class="greyd0">{{selectedJob.duration/60000 | number(2)}} mins</td>
            </tr>
          </table>
        </div>
      </div>
      <p class="time-hd">
        Job Details
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
              <span class="steptime jobActivityLabel">
                <i class="el-icon-time"></i>
                {{transToGmtTime(step.exec_start_time!=0? step.exec_start_time: '')}}
              </span>

              <div v-if="step.info.hdfs_bytes_written">
                <span class="jobActivityLabel">Data Size: </span>
                <span>{{step.info.hdfs_bytes_written|dataSize}}</span>
              </div>
              <div>
                <span class="jobActivityLabel">{{$t('duration')}}: </span>
                <span>{{timerlineDuration(step)}}</span><br />
              </div>
            </div>
            <div class="timeline-footer">
              <i name="file" v-if="step.step_status!='PENDING'" class="el-icon-ksd-export" @click="clickFile(step)"></i>
              <i name="key" v-if="step.exec_cmd" class="el-icon-ksd-paramters" @click="clickKey(step)"></i>
              <a :href="step.info.yarn_application_tracking_url" target="_blank"
                 tooltip="MRJob" style="margin-left: 5px;">
                  <i name="tasks" v-if="step.info.yarn_application_tracking_url" class="el-icon-ksd-details"></i>
              </a>
              <a  target="_blank" tooltip="Monitoring">
                <i class="ace-icon fa fa-chain grey bigger-110"></i>
              </a>
            </div>
          </div>
        </li>
      </ul>
      <div class='job-btn' @click='showStep=false'><i class='el-icon-d-arrow-right' aria-hidden='true'></i>
      </div>
    </el-card>

    <el-dialog id="show-diagnos" :title="stepAttrToShow == 'cmd' ? $t('parameters') : $t('output')" :visible.sync="dialogVisible">
      <job_dialog :stepDetail="outputDetail"></job_dialog>
      <span slot="footer" class="dialog-footer">
        <el-button type="primary" plain size="medium" @click="dialogVisible = false">Close</el-button>
      </span>
    </el-dialog>

    <diagnosis :targetId="targetId" :job="selectedJob" :show="diagnosisVisible" v-on:closeModal="closeModal"></diagnosis>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import jobDialog from './job_dialog'
import TWEEN from '@tweenjs/tween.js'
import { pageCount } from '../../config'
import { transToGmtTime, kapConfirm, handleError, handleSuccess } from 'util/business'
import diagnosisXX from '../security/diagnosis'
@Component({
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      loadJobsList: 'LOAD_JOBS_LIST',
      getJobDetail: 'GET_JOB_DETAIL',
      loadStepOutputs: 'LOAD_STEP_OUTPUTS',
      removeJob: 'REMOVE_JOB',
      pauseJob: 'PAUSE_JOB',
      cancelJob: 'CANCEL_JOB',
      resumeJob: 'RESUME_JOB'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    'job_dialog': jobDialog,
    'diagnosis': diagnosisXX
  },
  locales: {
    'en': {dataRange: 'Data Range', JobType: 'Job Type', JobName: 'Job Name', TableModelCube: 'Target Subject', ProgressStatus: 'Job Status', startTime: 'Start Time', Duration: 'Duration', Actions: 'Actions', jobResume: 'Resume', jobDiscard: 'Discard', jobPause: 'Pause', jobDiagnosis: 'Diagnosis', jobDrop: 'Drop', tip_jobDiagnosis: 'Download Diagnosis Info For This Job', tip_jobResume: 'Resume the Job', tip_jobPause: 'Pause the Job', tip_jobDiscard: 'Discard the Job', cubeName: 'Cube Name', NEW: 'NEW', PENDING: 'PENDING', RUNNING: 'RUNNING', FINISHED: 'FINISHED', ERROR: 'ERROR', DISCARDED: 'DISCARDED', STOPPED: 'STOPPED', LASTONEDAY: 'LAST ONE DAY', LASTONEWEEK: 'LAST ONE WEEK', LASTONEMONTH: 'LAST ONE MONTH', LASTONEYEAR: 'LAST ONE YEAR', ALL: 'ALL', parameters: 'Parameters', output: 'Output', load: 'Loading ... ', cmdOutput: 'cmd_output', resumeJob: 'Are you sure to resume the job?', discardJob: 'Are you sure to discard the job?', pauseJob: 'Are you sure to pause the job?', dropJob: 'Are you sure to drop the job?', diagnosis: 'Generate Diagnosis Package', 'jobName': 'Job Name', 'duration': 'Duration', 'waiting': 'Waiting', noSelectJobs: 'Please check at least one job.', selectedJobs: '{selectedNumber} jobs have been selected', selectAll: 'All Select'},
    'zh-cn': {dataRange: '数据范围', JobType: 'Job 类型', JobName: '任务', TableModelCube: '任务对象', ProgressStatus: '任务状态', startTime: '任务开始时间', Duration: '耗时', Actions: '操作', jobResume: '恢复', jobDiscard: '终止', jobPause: '暂停', jobDiagnosis: '诊断', jobDrop: '删除', tip_jobDiagnosis: '下载Job诊断包', tip_jobResume: '恢复Job', tip_jobPause: '暂停Job', tip_jobDiscard: '终止Job', cubeName: 'Cube 名称', NEW: '新建', PENDING: '等待', RUNNING: '运行', FINISHED: '完成', ERROR: '错误', DISCARDED: '终止', STOPPED: '暂停', LASTONEDAY: '最近一天', LASTONEWEEK: '最近一周', LASTONEMONTH: '最近一月', LASTONEYEAR: '最近一年', ALL: '所有', parameters: '参数', output: '输出', load: '下载中 ... ', cmdOutput: 'cmd_output', resumeJob: '确定要恢复任务?', discardJob: '确定要终止任务?', pauseJob: '确定要暂停任务?', dropJob: '确定要删除任务?', diagnosis: '诊断', 'jobName': '任务名', 'duration': '持续时间', 'waiting': '等待时间', noSelectJobs: '请勾选至少一项任务。', selectedJobs: '目前已选择当页{selectedNumber}条任务。', selectAll: '全选'}
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
  isSelectAllShow = false
  isSelectAll = false
  selectedNumber = 0
  idsArr = []
  idsArrCopy = []
  filter = {
    pageOffset: 0,
    pageSize: pageCount,
    timeFilter: 4,
    jobName: this.$store.state.monitor.filter.jobName,
    sortBy: this.$store.state.monitor.filter.sortby,
    status: this.$store.state.monitor.filter.status,
    subjects: ''
  }
  allStatus = [
    {name: 'ALL', value: null},
    {name: 'PENDING', value: 1},
    {name: 'RUNNING', value: 2},
    {name: 'FINISHED', value: 4},
    {name: 'ERROR', value: 8},
    {name: 'DISCARDED', value: 16},
    {name: 'STOPPED', value: 32}
  ]
  diagnosisVisible = false
  targetId = ''
  searchLoading = false
  created () {
    this.filter.project = this.currentSelectedProject
    var autoFilter = () => {
      this.stCycle = setTimeout(() => {
        this.refreshJobs().then((res) => {
          handleSuccess(res, (data) => {
            autoFilter()
          })
        }, (res) => {
          handleError(res)
        })
      }, 30000)
    }
    autoFilter()
    this.loadJobsList(this.filter)
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
    this.$store.state.monitor.filter = {
      timeFilter: this.filter.timeFilter,
      jobName: this.filter.jobName,
      sortby: this.filter.sortby,
      status: this.filter.status
    }
  }
  get jobsList () {
    return this.$store.state.monitor.jobsList.map((m) => {
      m.gmtTime = transToGmtTime(m.exec_start_time, this)
      if (this.selectedJob) {
        if (m.id === this.selectedJob.id) {
          this.getJobDetail({project: this.currentSelectedProject, jobId: m.id}).then((res) => {
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
  }
  get jobTotal () {
    return this.$store.state.monitor.totalJobs
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
          if (sTop < 96) {
            result = 96
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
  handleSelectionChange (val) {
    this.multipleSelection = val
    this.idsArr = this.multipleSelection.map((item) => {
      return item.id
    })
  }
  handleSelectAll (val) {
    if (this.jobTotal > 10 && (this.filter.status !== '' || this.filter.status !== [])) {
      this.isSelectAllShow = !this.isSelectAllShow
      this.selectedNumber = this.animatedNum(this.jobsList.length, 0)
    }
  }
  handleSelect (val) {
    if (this.jobTotal > 10 && (this.filter.status !== '' || this.filter.status !== [])) {
      if (this.multipleSelection.length < 10) {
        this.isSelectAllShow = false
        this.isSelectAll = false
      } else {
        this.isSelectAllShow = true
      }
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
  batchResume () {
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll) {
        this.resume([])
      } else {
        const jobIds = this.getJobIds()
        this.resume(jobIds)
      }
    }
  }
  batchDiscard () {
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll) {
        this.discard([])
      } else {
        const jobIds = this.getJobIds()
        this.discard(jobIds)
      }
    }
  }
  batchPause () {
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll) {
        this.pause([])
      } else {
        const jobIds = this.getJobIds()
        this.pause(jobIds)
      }
    }
  }
  batchDrop () {
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll) {
        this.drop([])
      } else {
        const jobIds = this.getJobIds()
        this.drop(jobIds)
      }
    }
  }
  closeModal () {
    this.diagnosisVisible = false
  }
  currentChange (val) {
    this.filter.pageOffset = val - 1
    this.refreshJobs()
  }
  closeIt () {
    if (this.showStep) {
      this.showStep = false
    }
  }
  diagnosisJob (a, target) {
    this.diagnosisVisible = true
    this.targetId = target
  }
  filterChange (val) {
    this.searchLoading = true
    clearTimeout(this.lockST)
    this.lockST = setTimeout(() => {
      this.refreshJobs()
      this.showStep = false
    }, 1000)
  }
  filterChange2 () {
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
    return this.loadJobsList(this.filter).then(() => {
      this.searchLoading = false
    }, () => {
      this.searchLoading = false
    })
  }
  sortJobList (column, prop, order) {
    let _column = column.column
    if (_column.order === 'ascending') {
      this.filter.reverse = false
    } else {
      this.filter.reverse = true
    }
    if (_column.label === this.$t('JobName')) {
      this.filter.sortby = 'job_name'
    } else if (_column.label === this.$t('TableModelCube')) {
      this.filter.sortby = 'cube_name'
    } else if (_column.label === this.$t('LastModifiedTime')) {
      this.filter.sortby = 'last_modify'
    }
    this.loadJobsList(this.filter)
  }
  resume (jobIds) {
    kapConfirm(this.$t('resumeJob')).then(() => {
      this.resumeJob({jobIds: jobIds, project: this.currentSelectedProject, action: 'RESUME', status: this.filter.status}).then(() => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.actionSuccess')
        })
        this.refreshJobs()
      }).catch((res) => {
        handleError(res)
      })
    })
  }
  discard (jobIds) {
    kapConfirm(this.$t('discardJob')).then(() => {
      this.cancelJob({jobIds: jobIds, project: this.currentSelectedProject, action: 'DISCARD', status: this.filter.status}).then(() => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.actionSuccess')
        })
        this.refreshJobs()
      }).catch((res) => {
        handleError(res)
      })
    })
  }
  pause (jobIds) {
    kapConfirm(this.$t('pauseJob')).then(() => {
      this.pauseJob({jobIds: jobIds, project: this.currentSelectedProject, action: 'PAUSE', status: this.filter.status}).then(() => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.actionSuccess')
        })
        this.refreshJobs()
      }).catch((res) => {
        handleError(res)
      })
    })
  }
  drop (jobIds) {
    kapConfirm(this.$t('dropJob')).then(() => {
      this.removeJob({jobIds: jobIds, project: this.currentSelectedProject, status: this.filter.status}).then(() => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
        this.refreshJobs()
      }).catch((res) => {
        handleError(res)
      })
    })
  }
  showLineSteps (row) {
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
          if (sTop < 112) {
            result = 112
          }
          document.getElementById('stepList').style.top = result + 'px'
        })
      }, (resError) => {
        handleError(resError)
      })
    })
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
    this.loadStepOutputs({jobID: this.selectedJob.id, stepID: step.id}).then((result) => {
      this.outputDetail = result.body.data.cmd_output
    }).catch((result) => {
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
      .el-button {
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
        border-color: @text-secondary-color;
        background-color: @grey-4;
        z-index: 0;
      }
    }
    .el-dropdown-link {
      display: inline-block;
      height: 25px;
      width: 25px;
      text-align: center;
    }
    .el-progress-bar__innerText {
      top: -1px;
      position: relative;
    }
    .job-step {
      width: 30%;
      min-height: calc(~'100vh - 173px');
      box-sizing: border-box;
      z-index: 100;
      position: absolute;
      top: 0;
      right: 0;
        &.el-card {
          border-radius: 0;
          padding: 20px;
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
            width: 30%;
            text-align: right;
          }
        }
      }
      .time-hd {
        height:40px;
        line-height:40px;
        margin:20px 0 16px 10px;
        font-size: 14px;
      }
      .job-btn {
        position: absolute;
        left: 0px;
        top: 310px;
        height: 70px;
        width: 18px;
        line-height: 70px;
        padding-left: 0px;
        font-size: 12px;
        border-radius: 0;
        background-color: @modeledit-bg-color;
        border: 1px solid @border-color-base;
        cursor: pointer;
        i {
          position: relative;
          left: 3px;
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
          top: 28px;
          bottom: 0;
          width: 3px;
          background: @border-color-base;
          left: 14px;
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
          padding-bottom: 15px;
          .timeline-item {
            position: relative;
            margin-left: 45px;
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
            width: 26px;
            height: 26px;
            font-size: 10px;
            line-height: 26px;
            position: absolute;
            border-radius: 50%;
            text-align: center;
            font-size: 30px;
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
              font-size: 20px;
              width: 26px;
              height: 26px;
            }
          }
        }
        li:last-child {
          position: relative;
        }
      }
    }
    .jobs-table {
      tr.current-row2 > td{
        background: @base-color-9;
      }
      tr td:nth-child(2) .cell {
        position:relative;
        padding: 0px 10px 0px 35px;
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

</style>
