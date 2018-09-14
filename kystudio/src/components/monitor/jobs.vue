<template>
  <div class="jobs_list ksd-mrl-20" @click.stop>
    <el-row :gutter="20" class="ksd-mt-10 ksd-mb-16">
      <el-col  :xs="12" :md="4" :lg="4">
        <el-input :placeholder="$t('kylinLang.common.pleaseFilter')" v-model="filter.jobName"  @input="filterChange" class="show-search-btn" size="medium" prefix-icon="el-icon-search">
        <!--   <el-button slot="append" :icon="searchLoading? 'el-icon-loading':'el-icon-search'" ></el-button> -->
        </el-input>

      </el-col>
      <el-col  :xs="12" :md="4" :lg="4">
        <el-select v-model="filter.timeFilter" @change="filterChange2" style="width:100%;" size="medium">
          <el-option
            v-for="(item, item_index) in timeFilter"
            :key="item_index"
            :label="$t(item.name)"
            :value="item.value">
          </el-option>
        </el-select>
      </el-col>
      <el-col  :xs="24" :md="16" :lg="16">
        <el-checkbox-group v-model="filter.status" @change="filterChange2" style="float: right;" class="ksd-mt-10">
          <el-checkbox :label="status.value" v-for="(status, status_index) in allStatus" :key="status_index">{{$t(status.name)}}</el-checkbox>
        </el-checkbox-group>
      </el-col>
    </el-row>
    <el-table class="ksd-el-table jobs-table"
      tooltip-effect="dark"
      border
      :data="jobsList"
      highlight-current-row
      @row-click="showLineSteps"
      @sort-change="sortJobList"
      :row-class-name="tableRowClassName"
      :style="{width:showStep?'70%':'100%'}"
    >
      <!-- :default-sort="{prop: 'jobname', order: 'descending'}" -->
      <el-table-column
        :label="$t('JobType')"
        sortable
        :width="200"
      >
        <template slot-scope="scope">
          <i class="el-icon-arrow-right" ></i> {{scope.row.job_type}}
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
            {{transToGmtTime(scope.row.data_range_start/1000)}} - {{transToGmtTime(scope.row.data_range_end/1000)}}
        </template>
      </el-table-column>
      <el-table-column
        :width="180"
        :label="$t('ProgressStatus')">
        <template slot-scope="scope">
          <kap-progress :percent="scope.row.progress | number(0)" :status="scope.row.job_status"></kap-progress>
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
            <i class="el-icon-delete ksd-fs-16" @click.stop="drop(scope.row.uuid)"></i>
          </common-tip>
          <common-tip :content="$t('jobDiscard')" v-if="scope.row.job_status=='PENDING' || scope.row.job_status=='RUNNING'">
            <i class="el-icon-ksd-table_discard ksd-fs-16" @click.stop="discard(scope.row)"></i>
          </common-tip>
          <common-tip :content="$t('jobResume')" v-if="scope.row.job_status=='ERROR'|| scope.row.job_status=='STOPPED'">
            <i class="el-icon-ksd-table_resure ksd-fs-16" @click.stop="resume(scope.row)"></i>
          </common-tip>
          <el-dropdown trigger="click">
            <span class="el-dropdown-link" @click.stop>
              <common-tip :content="$t('kylinLang.common.moreActions')">
                <i class="el-icon-ksd-table_others ksd-fs-16"></i>
              </common-tip>
            </span>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item @click.native="discard(scope.row)" v-if="scope.row.job_status=='NEW' || scope.row.job_status=='ERROR' || scope.row.job_status=='STOPPED'">{{$t('jobDiscard')}}</el-dropdown-item>
              <el-dropdown-item @click.native="pause(scope.row)" v-if="scope.row.job_status=='RUNNING' || scope.row.job_status=='NEW' || scope.row.job_status=='PENDING'">{{$t('jobPause')}}</el-dropdown-item>
              <el-dropdown-item @click.native="diagnosisJob(scope.row, scope.row.uuid)">{{$t('jobDiagnosis')}}</el-dropdown-item>
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
                {{selectedJob.uuid}}
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
            <tr>
              <td>MapReduce {{$t('waiting')}}</td>
              <td class="greyd0">{{selectedJob.mr_waiting/60 | number(2)}} mins</td>
            </tr>
          </table>
        </div>
      </div>
      <p class="time-hd">
        Job Details
      </p>
      <ul class="timeline">

        <li v-for="(step, index) in selectedJob.steps" :class="{'finished' : step.step_status=='FINISHED'}">
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
              <li>Waiting: {{ step.exec_wait_time | tofixedTimer(2)}}</li>
              <li>Start At: {{transToGmtTime(step.exec_start_time !=0 ? step.exec_start_time:'')}}</li>
              <li>End At: {{transToGmtTime(step.exec_end_time !=0 ? step.exec_end_time :'')}}</li>
              <li v-if="step.info.hdfs_bytes_written">Data Size: <span>{{ step.info.hdfs_bytes_written | dataSize}}</span></li>
              <li v-if="step.info.mr_job_id">MR Job: {{step.info.mr_job_id}}</li>
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
                <!-- <br /> -->
              </div>
              <div>
                <span class="jobActivityLabel">{{$t('duration')}}: </span>
                <span>{{timerlineDuration(step)}}</span><br />
              </div>
              <div>
                <span class="jobActivityLabel">{{$t('waiting')}}: </span>
                <span>{{step.exec_wait_time | tofixedTimer(2)}}</span><br />
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

    <!-- <el-dialog class="kybot_diagnosis" :close-on-click-modal="false" :title="$t('diagnosis')" :visible.sync="diagnosisVisible"> -->
    <diagnosis :targetId="targetId" :job="selectedJob" :show="diagnosisVisible" v-on:closeModal="closeModal"></diagnosis>
    <!-- </el-dialog> -->
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import jobDialog from './job_dialog'
import { pageCount } from '../../config'
import { transToGmtTime, kapConfirm, handleError, handleSuccess } from 'util/business'
import diagnosisXX from '../security/diagnosis'
@Component({
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      loadJobsList: 'LOAD_JOBS_LIST',
      loadStepOutputs: 'LOAD_STEP_OUTPUTS',
      removeJob: 'REMOVE_JOB',
      pauseJob: 'PAUSE_JOB',
      cancelJob: 'CANCEL_JOB',
      resumeJob: 'RESUME_JOB'
    }),
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    'job_dialog': jobDialog,
    'diagnosis': diagnosisXX
  },
  locales: {
    'en': {dataRange: 'Data Range', JobType: 'Job Type', JobName: 'Job Name', TableModelCube: 'Target Subject', ProgressStatus: 'Job Status', startTime: 'Start Time', Duration: 'Duration', Actions: 'Actions', jobResume: 'Resume', jobDiscard: 'Discard', jobPause: 'Pause', jobDiagnosis: 'Diagnosis', jobDrop: 'Drop', tip_jobDiagnosis: 'Download Diagnosis Info For This Job', tip_jobResume: 'Resume the Job', tip_jobPause: 'Pause the Job', tip_jobDiscard: 'Discard the Job', cubeName: 'Cube Name', NEW: 'NEW', PENDING: 'PENDING', RUNNING: 'RUNNING', FINISHED: 'FINISHED', ERROR: 'ERROR', DISCARDED: 'DISCARDED', STOPPED: 'STOPPED', LASTONEDAY: 'LAST ONE DAY', LASTONEWEEK: 'LAST ONE WEEK', LASTONEMONTH: 'LAST ONE MONTH', LASTONEYEAR: 'LAST ONE YEAR', ALL: 'ALL', parameters: 'Parameters', output: 'Output', load: 'Loading ... ', cmdOutput: 'cmd_output', resumeJob: 'Are you sure to resume the job?', discardJob: 'Are you sure to discard the job?', pauseJob: 'Are you sure to pause the job?', dropJob: 'Are you sure to drop the job?', diagnosis: 'Generate Diagnosis Package', 'jobName': 'Job Name', 'duration': 'Duration', 'waiting': 'Waiting'},
    'zh-cn': {dataRange: '数据范围', JobType: 'Job 类型', JobName: '任务', TableModelCube: '任务对象', ProgressStatus: '任务状态', startTime: '任务开始时间', Duration: '耗时', Actions: '操作', jobResume: '恢复', jobDiscard: '终止', jobPause: '暂停', jobDiagnosis: '诊断', jobDrop: '删除', tip_jobDiagnosis: '下载Job诊断包', tip_jobResume: '恢复Job', tip_jobPause: '暂停Job', tip_jobDiscard: '终止Job', cubeName: 'Cube 名称', NEW: '新建', PENDING: '等待', RUNNING: '运行', FINISHED: '完成', ERROR: '错误', DISCARDED: '终止', STOPPED: '暂停', LASTONEDAY: '最近一天', LASTONEWEEK: '最近一周', LASTONEMONTH: '最近一月', LASTONEYEAR: '最近一年', ALL: '所有', parameters: '参数', output: '输出', load: '下载中 ... ', cmdOutput: 'cmd_output', resumeJob: '确定要恢复任务?', discardJob: '确定要终止任务?', pauseJob: '确定要暂停任务?', dropJob: '确定要删除任务?', diagnosis: '诊断', 'jobName': '任务名', 'duration': '持续时间', 'waiting': '等待时间'}
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
  filter = {
    pageOffset: 0,
    pageSize: pageCount,
    timeFilter: this.$store.state.monitor.filter.timeFilter,
    jobName: this.$store.state.monitor.filter.jobName,
    sortby: this.$store.state.monitor.filter.sortby,
    status: this.$store.state.monitor.filter.status,
    subjects: ''
  }
  allStatus = [
    {name: 'PENDING', value: 1},
    {name: 'RUNNING', value: 2},
    {name: 'FINISHED', value: 4},
    {name: 'ERROR', value: 8},
    {name: 'DISCARDED', value: 16},
    {name: 'STOPPED', value: 32}
  ]
  timeFilter = [
    {name: 'LASTONEDAY', value: 0},
    {name: 'LASTONEWEEK', value: 1},
    {name: 'LASTONEMONTH', value: 2},
    {name: 'LASTONEYEAR', value: 3},
    {name: 'ALL', value: 4}
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
    document.getElementById('scrollBox').addEventListener('scroll', this.scrollRightBar, false)
  }
  beforeDestroy () {
    window.clearTimeout(this.stCycle)
    window.clearTimeout(this.scrollST)
    window.removeEventListener('click', this.closeIt)
    document.getElementById('scrollBox').removeEventListener('scroll', this.scrollRightBar, false)
    this.$store.state.monitor.filter = {
      timeFilter: this.filter.timeFilter,
      jobName: this.filter.jobName,
      sortby: this.filter.sortby,
      status: this.filter.status
    }
  }
  get jobsList () {
    return this.$store.state.monitor.jobsList.map((m) => {
      m.gmtTime = transToGmtTime(m.exec_start_time / 1000, this)
      if (this.selectedJob) {
        if (m.uuid === this.selectedJob.uuid) {
          this.selectedJob = m
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
    if (row.uuid === this.selectedJob.uuid && this.showStep) {
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
  resume (job) {
    kapConfirm(this.$t('resumeJob')).then(() => {
      this.resumeJob(job.uuid).then(() => {
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
  discard (job) {
    kapConfirm(this.$t('discardJob')).then(() => {
      this.cancelJob(job.uuid).then(() => {
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
  pause (job) {
    kapConfirm(this.$t('pauseJob')).then(() => {
      this.pauseJob(job.uuid).then(() => {
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
  drop (jobId) {
    kapConfirm(this.$t('dropJob')).then(() => {
      this.removeJob(jobId).then(() => {
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
  showLineSteps (row, v1, v2) {
    var needShow = false
    // 减去滚动条的高度
    if (row.uuid !== this.selectedJob.uuid) {
      needShow = true
    } else {
      needShow = !this.showStep
    }
    this.showStep = false
    this.$nextTick(() => {
      this.showStep = needShow
      var sTop = document.getElementById('scrollBox').scrollTop
      this.beforeScrollPos = sTop
      var result = sTop
      if (sTop < 96) {
        result = 96
      }
      document.getElementById('stepList').style.top = result + 'px'
      this.selectedJob = row
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
    this.loadStepOutputs({jobID: this.selectedJob.uuid, stepID: step.id}).then((result) => {
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
    .el-dropdown-link {
      display: inline-block;
      height: 25px;
      width: 25px;
      text-align: center;
    }
    .job-step {
      width: 30%;
      z-index: 100;
      position: absolute;
      top: 0;
      right: 0;
        &.el-card {
          border-radius: 0;
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
            &.el-icon-ksd-table_others {
              // border: 1px solid @border-color-base;
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
      tr td:first-child .cell {
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
