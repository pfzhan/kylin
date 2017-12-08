<template>
  <div id="jobs_list" @click.stop :style="{width:showStep?'70%':'100%'}">
    <el-row :gutter="20" style="padding-top: 10px; margin-bottom: -8px;">
      <el-col  :xs="12" :md="4" :lg="4">
        <el-input
          icon="search"
          v-model="filter.jobName"
          :placeholder="$t('kylinLang.common.pleaseFilter')"
          @change="filterChange">
        </el-input>
      </el-col>
      <el-col  :xs="12" :md="4" :lg="4">
        <el-select v-model="filter.timeFilter" @change="refreshJobs" style="width:100%;">
          <el-option
            v-for="(item, item_index) in timeFilter"
            :key="item_index"
            :label="$t(item.name)"
            :value="item.value">
          </el-option>
        </el-select>
      </el-col>
      <el-col  :xs="24" :md="16" :lg="16">
        <el-checkbox-group v-model="filter.status" @change="refreshJobs" style="float: right;">
          <el-checkbox :label="status.value" v-for="(status, status_index) in allStatus" :key="status_index">{{$t(status.name)}}</el-checkbox>
        </el-checkbox-group>
      </el-col>
    </el-row>
    <el-table class="ksd-el-table table_margin"
      tooltip-effect="dark"
      border
      :data="jobsList"
      style="width:100%"
      highlight-current-row
      @row-click="showLineSteps"
      @sort-change="sortJobList"
    >
      <!-- :default-sort="{prop: 'jobname', order: 'descending'}" -->
      <el-table-column
        :label="$t('JobName')"
        sortable
        :width="300"
        prop="jobname"
      >
        <template scope="scope">
          <i class="el-icon-arrow-right" ></i> {{scope.row.name}}
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('TableModelCube')"
        sortable
        :min-width="180"
        show-overflow-tooltip
        prop="related_cube">
      </el-table-column>
      <el-table-column
        :width="180"
        :label="$t('ProgressStatus')">
        <template scope="scope">
          <!--  <el-progress  :percentage="scope.row.progress" v-if="scope.row.progress === 100" status="success">
           </el-progress>
           <el-progress  :percentage="scope.row.progress" v-if="scope.row.job_status === 'ERROR'" status="exception">
           </el-progress>
           <el-progress  :percentage="scope.row.progress | number(2)"  v-else>
           </el-progress> -->
          <kap-progress :percent="scope.row.progress | number(0)" :status="scope.row.job_status"></kap-progress>
        </template>
      </el-table-column>
      <el-table-column
        :width="230"
        :label="$t('LastModifiedTime')"
        show-overflow-tooltip
        sortable>
        <template scope="scope">
          {{scope.row.gmtTime}}
        </template>
      </el-table-column>
      <el-table-column
        :width="180"
        :label="$t('Duration')">
        <template scope="scope">
          {{scope.row.duration/60 | number(2) }}  mins
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('Actions')"
        width="100">
        <template scope="scope">
          <el-dropdown trigger="click">
            <el-button class="el-dropdown-link" @click.stop>
              <i class="el-icon-more"></i>
            </el-button>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item @click.native="resume(scope.row)" v-if="scope.row.job_status=='ERROR'|| scope.row.job_status=='STOPPED'">{{$t('jobResume')}}</el-dropdown-item>
              <el-dropdown-item @click.native="discard(scope.row)" v-if="scope.row.job_status=='RUNNING' || scope.row.job_status=='NEW' || scope.row.job_status=='PENDING' || scope.row.job_status=='ERROR' || scope.row.job_status=='STOPPED'">{{$t('jobDiscard')}}</el-dropdown-item>
              <el-dropdown-item @click.native="pause(scope.row)" v-if="scope.row.job_status=='RUNNING' || scope.row.job_status=='NEW' || scope.row.job_status=='PENDING'">{{$t('jobPause')}}</el-dropdown-item>
              <el-dropdown-item @click.native="diagnosisJob(scope.row, scope.row.uuid)">{{$t('jobDiagnosis')}}</el-dropdown-item>
              <el-dropdown-item @click.native="drop(scope.row.uuid)" v-if="scope.row.job_status=='FINISHED' || scope.row.job_status=='DISCARDED'">{{$t('jobDrop')}}</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </template>
      </el-table-column>
    </el-table>


    <pager :totalSize="jobTotal"  v-on:handleCurrentChange='currentChange' ref="jobPager" class="ksd-mb-20" ></pager>

    <el-card v-show="showStep" class="card-width job-step" id="stepList">

      <div class="timeline-item">
        <div class="timeline-body">
          <table class="table table-striped table-bordered" cellpadding="0" cellspacing="0">
            <tr>
              <td class="single-line"><b>{{$t('jobName')}}</b></td>
              <td style="max-width: 180px;word-wrap: break-word;word-break: normal;font-weight: normal;font-size: 12px;" class="greyd0">
                {{selected_job.name}}
              </td>
            </tr>
            <tr>
              <td>Job ID</td>
              <td class="single-line greyd0">
                {{selected_job.uuid}}
              </td>
            </tr>
            <tr>
              <td>{{$t('kylinLang.common.status')}}</td>
              <td>
                <el-tag
                  :type="getJobStatusTag">
                  {{selected_job.job_status}}
                </el-tag>
              </td>
            </tr>
            <tr>
              <td>{{$t('duration')}}</td>
              <td class="greyd0">{{selected_job.duration/60 | number(2)}} mins</td>
            </tr>
            <tr>
              <td>MapReduce {{$t('waiting')}}</td>
              <td class="greyd0">{{selected_job.mr_waiting/60 | number(2)}} mins</td>
            </tr>
          </table>
        </div>
      </div>
      </li>
      <p class="blue time-hd">
        Job Details
      </p>
      <ul class="timeline">

        <!--Start Label-->
        <!-- <li class="time-label">
            <span class="bg-blue">
                <b>Start &nbsp;&nbsp;{{(selected_job.steps[0].exec_start_time !=0 ? selected_job.steps[0].exec_start_time :'') | utcTime}}</b>
            </span>
        </li> -->

        <li v-for="(step, index) in selected_job.steps">
          <el-popover
            placement="left"
            width="300"
            trigger="hover" popper-class="jobPoplayer">
            <i slot="reference"
               :class="{
              'fa el-icon-more bg-gray' : step.step_status=='PENDING',
              'fa el-icon-loading bg-aqua' : step.step_status=='WAITING' || step.step_status=='RUNNING',
              'fa el-icon-check bg-green' : step.step_status=='FINISHED',
              'fa el-icon-warning bg-red' : step.step_status=='ERROR',
              'fa el-icon-minus bg-navy' : step.step_status=='DISCARDED'
            }">
            </i>
            <ul >
              <li>SequenceID: {{step.sequence_id}}</li>
              <li>Status: {{step.step_status}}</li>
              <li>Duration: {{timerline_duration(step)}}</li>
              <li>Waiting: {{ step.exec_wait_time | tofixedTimer(2)}}</li>
              <li>Start At: {{transToGmtTime(step.exec_start_time !=0 ? step.exec_start_time:'')}}</li>
              <li>End At: {{transToGmtTime(step.exec_end_time !=0 ? step.exec_end_time :'')}}</li>
              <li v-if="step.info.hdfs_bytes_written">Data Size: <span class="blue">{{ step.info.hdfs_bytes_written | dataSize}}</span></li>
              <li v-if="step.info.mr_job_id">MR Job: {{step.info.mr_job_id}}</li>
            </ul>
          </el-popover>

          <div class="timeline-item timer-line">
            <div class="timeline-header ">
              <p class="stepname single-line">{{step.name}}</p>
            </div>
            <div class="timeline-body">
              <!-- <span style="color: #4383B4">#{{index+1}} Step Name: </span>{{step.name}}<br> -->
              <span class="steptime jobActivityLabel">
                <i class="el-icon-time"></i>
                {{transToGmtTime(step.exec_start_time!=0? step.exec_start_time: '')}}
              </span>

              <div v-if="step.info.hdfs_bytes_written">
                <span class="jobActivityLabel">Data Size: </span>
                <span class="blue">{{step.info.hdfs_bytes_written|dataSize}}</span>
                <!-- <br /> -->
              </div>
              <div>
                <span class="jobActivityLabel">{{$t('duration')}}: </span>
                <span class="blue">{{timerline_duration(step)}}</span><br />
              </div>
              <div>
                <span class="jobActivityLabel">{{$t('waiting')}}: </span>
                <span class="blue">{{step.exec_wait_time | tofixedTimer(2)}}</span><br />
              </div>
            </div>
            <div class="timeline-footer">
              <el-button v-if="step.exec_cmd"  :plain="true" @click.native="clickKey(step)" size="mini">
                <icon name="key" class="icon-key"></icon>
              </el-button>
              <el-button v-if="step.step_status!='PENDING'"  :plain="true" @click.native="clickFile(step)" size="mini">
                <icon name="file" class="icon-file"></icon>
              </el-button>
              <a :href="step.info.yarn_application_tracking_url" target="_blank"
                 tooltip="MRJob" style="margin-left: 10px;">
                <el-button  v-if="step.info.yarn_application_tracking_url"  :plain="true"  size="mini">
                  <icon name="tasks" class="icon-task"></icon>
                </el-button>
              </a>

              <a  target="_blank" tooltip="Monitoring">
                <i class="ace-icon fa fa-chain grey bigger-110"></i>
              </a>
            </div>
          </div>
        </li>
        <!-- <li class="time-label">
          <span class="bg-blue">
            <b>End &nbsp;&nbsp; {{(selected_job.steps[selected_job.steps.length-1].exec_end_time !=0 ? (selected_job.steps[selected_job.steps.length-1].exec_end_time) :'') | utcTime }}</b>
          </span>
        </li> -->
      </ul>
      <div class='jobBtn' @click='showStep=false'><i class='el-icon-caret-right' aria-hidden='true'></i>
      </div>
    </el-card>

    <el-dialog id="show-diagnos" :title="stepAttrToShow == 'cmd' ? $t('parameters') : $t('output')" v-model="dialogVisible" size="small">
      <job_dialog :stepDetail="outputDetail"></job_dialog>
      <span slot="footer" class="dialog-footer">
    <el-button type="primary" @click="dialogVisible = false">Close</el-button>
  </span>
    </el-dialog>

    <el-dialog :title="$t('diagnosis')" v-model="diagnosisVisible">
      <diagnosis :targetId="targetId" job="selected_job" :show="diagnosisVisible"></diagnosis>
    </el-dialog>
  </div>
</template>

<script>
  import { mapActions } from 'vuex'
  import jobDialog from './job_dialog'
  import { pageCount } from '../../config'
  import { transToGmtTime, kapConfirm, handleError, handleSuccess } from 'util/business'
  import diagnosisXX from '../system/diagnosis'
  export default {
    name: 'jobslist',
    data () {
      return {
        project: localStorage.getItem('selected_project'),
        filterName: '',
        filterStatus: [],
        lockST: null,
        scrollST: null,
        stCycle: null,
        showStep: false,
        selected_job: {},
        dialogVisible: false,
        outputDetail: '',
        stepAttrToShow: '',
        beforeScrollPos: 0,
        filter: {
          pageOffset: 0,
          pageSize: pageCount,
          projectName: localStorage.getItem('selected_project'),
          timeFilter: this.$store.state.monitor.filter.timeFilter,
          jobName: this.$store.state.monitor.filter.jobName,
          sortby: this.$store.state.monitor.filter.sortby,
          status: this.$store.state.monitor.filter.status
        },
        allStatus: [
          {name: 'PENDING', value: 1},
          {name: 'RUNNING', value: 2},
          {name: 'FINISHED', value: 4},
          {name: 'ERROR', value: 8},
          {name: 'DISCARDED', value: 16},
          {name: 'STOPPED', value: 32}
        ],
        timeFilter: [
          {name: 'LASTONEDAY', value: 0},
          {name: 'LASTONEWEEK', value: 1},
          {name: 'LASTONEMONTH', value: 2},
          {name: 'LASTONEYEAR', value: 3},
          {name: 'ALL', value: 4}
        ],
        diagnosisVisible: false,
        targetId: ''
      }
    },
    components: {
      'job_dialog': jobDialog,
      'diagnosis': diagnosisXX
    },
    created () {
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
      this.loadJobsList(this.filter).then(() => {
        autoFilter()
      })
    },
    mounted () {
      window.addEventListener('click', this.closeIt)
      document.getElementById('scrollBox').addEventListener('scroll', this.scrollRightBar, false)
    },
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
    },
    computed: {
      jobsList () {
        return this.$store.state.monitor.jobsList.map((m) => {
          m.gmtTime = transToGmtTime(m.last_modified, this)
          if (this.selected_job) {
            if (m.uuid === this.selected_job.uuid) {
              this.selected_job = m
            }
          }
          return m
        })
      },
      jobTotal () {
        return this.$store.state.monitor.totalJobs
      },
      getJobStatusTag () {
        if (this.selected_job.job_status === 'PENDING') {
          return 'gray'
        }
        if (this.selected_job.job_status === 'RUNNING') {
          return 'primary'
        }
        if (this.selected_job.job_status === 'FINISHED') {
          return 'success'
        }
        if (this.selected_job.job_status === 'ERROR') {
          return 'danger'
        }
        if (this.selected_job.job_status === 'DISCARDED') {
          return ''
        }
      }
    },
    methods: {
      ...mapActions({
        loadJobsList: 'LOAD_JOBS_LIST',
        loadStepOutputs: 'LOAD_STEP_OUTPUTS',
        removeJob: 'REMOVE_JOB',
        pauseJob: 'PAUSE_JOB',
        cancelJob: 'CANCEL_JOB',
        resumeJob: 'RESUME_JOB'
      }),
      scrollRightBar () {
        clearTimeout(this.scrollST)
        this.scrollST = setTimeout(() => {
          if (this.showStep) {
            var sTop = document.getElementById('scrollBox').scrollTop
            if (sTop < this.beforeScrollPos) {
              var result = sTop - 16
              document.getElementById('stepList').style.top = result + 'px'
            }
            if (sTop === 0) {
              this.beforeScrollPos = 0
            }
          }
        }, 10)
      },
      transToGmtTime: transToGmtTime,
      currentChange: function (val) {
        this.filter.pageOffset = val - 1
        this.refreshJobs()
      },
      closeIt () {
        if (this.showStep) {
          this.showStep = false
        }
      },
      diagnosisJob: function (a, target) {
        this.diagnosisVisible = true
        this.targetId = target
      },
      filterChange () {
        clearTimeout(this.lockST)
        this.lockST = setTimeout(() => {
          this.refreshJobs()
        }, 1000)
      },
      refreshJobs: function () {
        return this.loadJobsList(this.filter)
      },
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
      },
      resume: function (job) {
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
      },
      discard: function (job) {
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
      },
      pause: function (job) {
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
      },
      drop: function (jobId) {
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
      },
      showLineSteps: function (row, v1, v2) {
        var needShow = false
        // 减去滚动条的高度
        if (row.uuid !== this.selected_job.uuid) {
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
          if (sTop > 129) {
            result = sTop - 144
          } else {
            result = -16
          }
          document.getElementById('stepList').style.top = result + 'px'
          this.selected_job = row
        })
      },
      clickKey: function (step) {
        this.stepAttrToShow = 'cmd'
        this.outputDetail = step.exec_cmd
        this.dialogVisible = true
      },
      clickFile: function (step) {
        this.stepAttrToShow = 'output'
        this.dialogVisible = true
        this.outputDetail = this.$t('load')
        this.loadStepOutputs({jobID: this.selected_job.uuid, stepID: step.id}).then((result) => {
          this.outputDetail = result.body.data.cmd_output
        }).catch((result) => {
          this.outputDetail = this.$t('cmdOutput')
        })
      },
      timerline_duration (step) {
        let min = 0
        if (!step.exec_start_time || !step.exec_end_time) {
          return '0 seconds'
        } else {
          min = (step.exec_end_time - step.exec_start_time) / 1000 / 60
          return min.toFixed(2) + ' mins'
        }
      },
      closeLoginOpenKybot () {
        this.kyBotUploadVisible = false
        this.infoKybotVisible = true
      }
    },
    locales: {
      'en': {JobName: 'Job Name', TableModelCube: 'Table/Model/Cube', ProgressStatus: 'Progress/Status', LastModifiedTime: 'Last Modified Time', Duration: 'Duration', Actions: 'Actions', jobResume: 'Resume', jobDiscard: 'Discard', jobPause: 'Pause', jobDiagnosis: 'Diagnosis', jobDrop: 'Drop', tip_jobDiagnosis: 'Download Diagnosis Info For This Job', tip_jobResume: 'Resume the Job', tip_jobPause: 'Pause the Job', tip_jobDiscard: 'Discard the Job', cubeName: 'Cube Name', NEW: 'NEW', PENDING: 'PENDING', RUNNING: 'RUNNING', FINISHED: 'FINISHED', ERROR: 'ERROR', DISCARDED: 'DISCARDED', STOPPED: 'STOPPED', LASTONEDAY: 'LAST ONE DAY', LASTONEWEEK: 'LAST ONE WEEK', LASTONEMONTH: 'LAST ONE MONTH', LASTONEYEAR: 'LAST ONE YEAR', ALL: 'ALL', parameters: 'Parameters', output: 'Output', load: 'Loading ... ', cmdOutput: 'cmd_output', resumeJob: 'Are you sure to resume the job?', discardJob: 'Are you sure to discard the job?', pauseJob: 'Are you sure to pause the job?', dropJob: 'Are you sure to drop the job?', diagnosis: 'Generate Diagnosis Package', 'jobName': 'Job Name', 'duration': 'Duration', 'waiting': 'Waiting'},
      'zh-cn': {JobName: '任务', TableModelCube: '表/模型/Cube', ProgressStatus: '进度/状态', LastModifiedTime: '最后修改时间', Duration: '耗时', Actions: '操作', jobResume: '恢复', jobDiscard: '终止', jobPause: '暂停', jobDiagnosis: '诊断', jobDrop: '删除', tip_jobDiagnosis: '下载Job诊断包', tip_jobResume: '恢复Job', tip_jobPause: '暂停Job', tip_jobDiscard: '终止Job', cubeName: 'Cube 名称', NEW: '新建', PENDING: '等待', RUNNING: '运行', FINISHED: '完成', ERROR: '错误', DISCARDED: '无效', STOPPED: '暂停', LASTONEDAY: '最近一天', LASTONEWEEK: '最近一周', LASTONEMONTH: '最近一月', LASTONEYEAR: '最近一年', ALL: '所有', parameters: '参数', output: '输出', load: '下载中 ... ', cmdOutput: 'cmd_output', resumeJob: '确定要恢复任务?', discardJob: '确定要终止任务?', pauseJob: '确定要暂停任务?', dropJob: '确定要删除任务?', diagnosis: '诊断', 'jobName': '任务名', 'duration': '持续时间', 'waiting': '等待时间'}
    }
  }
</script>
<style lang="less">
  @import '../../less/config.less';
  #jobs_list {
  border-color: @grey-color;
  .el-dialog__title{
    font-size: 14px!important;
  }
  li {
    list-style-type:none;
  }
  .timeline {
    position: relative;
    margin: 0 0 30px 0;
    padding: 0;
    list-style: none;
    font-size: 12px;
  }
  .timeline:before {
    content: '';
    position: absolute;
    top: 0px;
    bottom: 0;
    width: 3px;
    background: #71779d;
    left: 10px;
    margin: 0;
    border-radius: 2px;
  }

  .timeline > li {
    position: relative;
    margin-right: 10px;
    margin-bottom: 15px;
  }
  .timeline > li:before,
  .timeline > li:after {
    content: " ";
    display: table;
  }
  .timeline > li:after {
    clear: both;
  }
  .timeline > li > .timeline-item {
  .el-button {
    background: @grey-color;
  }
  }
  .timeline > li > .timeline-item {
    position: relative;
    margin-top: 0px;
    margin-left: 60px;
    padding: 0;
    background: @grey-color;
    color: #444;
    border-radius: 3px;
  }
  .timeline > li > .timeline-item > .time {
    float: right;
    padding: 10px;
    color: #999;
    font-size: 12px;
  }
  .timeline > li > .timeline-item > .timeline-header {
    margin: 0;
    color: #555;
    padding: 2x 10px 0;
    font-size: 16px;
    line-height: 1.1;
  }
  .timeline > li > .timeline-item > .timeline-header > a {
    font-weight: 600;
  }
  .timeline > li > .timeline-item > .timeline-body,
  .timeline > li > .timeline-item > .timeline-footer {
    padding: 4px 10px 10px 0;
  }
  .timeline > li.time-label > span {
    font-weight: 600;
    padding: 5px;
    display: inline-block;
    background-color: #fff;
    border-radius: 4px;
    color: #fff;
  }
  .timeline > li > span > .fa,
  .timeline > li > .fa
  {
    width: 22px;
    height: 22px;
    font-size: 10px;
    line-height: 22px;
    position: absolute;
    color: #fff;
    background: #d2d6de;
    border-radius: 50%;
    text-align: center;
    left: 0;
    top: 0;
  }
  .timeline li:last-child {
    position: relative;
  }
  .timeline li:last-child:before {
    display: block;
    position: absolute;
    top: 0;
    left: 10px;
    bottom: 0;
    content: '';
    width: 3px;
    background: #71779d;
  }
  .timeline li .icon-key, .timeline li .icon-file, .timeline li .icon-task{
    color:#B0BBCB
  }
  .timeline li .icon-key:hover, .timeline li .icon-file:hover, .timeline li .icon-task:hover{
    color:#fff
  }
  .bg-blue {
    background-color: #0073b7 !important;
  }

  .bg-gray {
    color: #000;
    background-color: #71779d !important;
  }
  .bg-red {
    background-color: #dd4b39 !important;
  }
  .bg-aqua {
    background-color: #00c0ef !important;
  }
  .bg-green {
    background-color: #13ce66 !important;
  }
  .bg-navy {
    background-color: #001f3f !important;
  }
  .el-progress__text {
    font-size: 15px !important;;
  }
  .table_margin {
    margin-top: 20px;
    margin-bottom: 20px;
  }
  .card-width {
    width: 30%;
  }
  .job-step {
    z-index: 100;
    position: absolute;
    top: -16px;
    right: -30px;
  }
  .job-step.el-card {border-radius: 0;}
  .job-step .el-card__body {
    padding: 20px 20px 20px 40px;
  }
  .table {
    width: 100%;
    margin-bottom: 20px;
  }
  .table-bordered {
    border-collapse: collapse;
    font-size:14px;
    border:1px solid #4b506e;
  tr:first-child {background: transparent;color:@fff;}
  th,
  td {font-size:12px;
    padding:8px 18px;
    border-bottom:1px solid #393e53;
    border-right:1px solid #393e53;
  }
  tr:last-child td{
    border-bottom:none;
  }
  tr td:last-child{
    border-right:none;
  }
  .greyd0{
    color:#d0d0d0;
  }
  }
  .jobBtn {
    position: absolute;
    left: 0px;
    top: 240px;
    height: 48px;
    padding: 5px;
    color: #000;
    border-radius: 0 4px 4px 0;
    cursor: pointer;
    /* background: rgba(228, 232, 241, 0.6); */
  }
  .jobBtn i {
    position: relative;
    top: 16px;
    color: #909eb0;
    font-size:12px;
  }
  .table_margin .el-table__body tr{
  td:first-child .cell {position:relative;padding: 10px 10px 10px 50px;}
  }
  .table_margin .el-icon-arrow-right {position:absolute;left:20px;top:50%;transform:translate(0,-50%);font-size:12px;}
  .el-checkbox-group {margin-top:7px;}
  .single-line {max-width: 140px;}
  .blue {color: #fff;}
  .time-hd {height:40px;line-height:40px;margin-bottom:16px;border-bottom: 1px solid #4b506e;font-size: 14px}
  .timeline {
  .timeline-header .single-line.stepname {max-width:none;font-size:14px;}
  .steptime {height:20px;line-height:20px;font-size:14px;color:#666;
  .el-icon-time {color:#B0BBCB;font-size: 16px}
  }
  }
  .timer-line {
  .timeline-body {
    color: #999;
  }
  }
  .footer {
    margin-top: 30px;
  }
  .agree-protocol {
    line-height:30px;
  }
  .btn-agree {
    display: block;
    margin: 20px auto;
  }
  }
  .single-line {overflow:hidden;}
  .diagnosis-wrap{
  .el-dialog__wrapper{
    overflow: visible;
  }
  }

  .job-step{
  tr{
  td:first-child{
    font-weight: bold;
    color: #fff;
    width: 40%;
  }
  }
  td{
    background: #2b2d3c;
  }
  }
  #jobs_list .timeline .jobActivityLabel{
    color:#d4d7e3!important;
    font-size: 12px;
  }
  .jobPoplayer.el-popover[x-placement^=left] .popper__arrow{
    border-left-color: #333;
    &:after{
     border-left-color:#393e53;
   }
  }

</style>
