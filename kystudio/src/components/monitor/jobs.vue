<template>
  <div id="jobListPage">
  <el-alert :title="$t('adminTips')" type="info" class="admin-tips" v-if="$store.state.user.isShowAdminTips&&isAdminRole" @close="closeTips" show-icon></el-alert>
  <div class="jobs_list ksd-mrl-20">
    <div class="ksd-title-label ksd-mt-20">{{$t('jobsList')}}</div>
    <el-row :gutter="20" class="jobs_tools_row ksd-mt-10 ksd-mb-10">
      <el-col :span="18">
        <el-dropdown class="ksd-fleft waiting-jobs" trigger="click" placement="bottom-start" @command="handleCommand">
          <el-button class="el-dropdown-link" size="medium" :disabled="!waittingJobModels.size || $store.state.project.isAllProject">
            {{waittingJobModels.size}} {{$t('waitingjobs')}}<i class="el-icon-arrow-down el-icon--right"></i>
          </el-button>
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item v-for="(item, uuid) in waittingJobModels.data" :key="item.model_alias" :command="uuid">
              {{$t('kylinLang.common.model')}} {{item.model_alias}}: {{item.size}} {{$t('jobs')}}
            </el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
        <el-button-group class="action_groups ksd-ml-10 ksd-fleft" v-if="monitorActions.includes('jobActions')">
          <el-button size="medium" icon="el-icon-ksd-table_resume" :disabled="!batchBtnsEnabled.resume" @click="batchResume">{{$t('jobResume')}}</el-button>
          <el-button size="medium" icon="el-icon-ksd-restart" :disabled="!batchBtnsEnabled.restart" @click="batchRestart">{{$t('jobRestart')}}</el-button>
          <el-button size="medium" icon="el-icon-ksd-pause" :disabled="!batchBtnsEnabled.pause" @click="batchPause">{{$t('jobPause')}}</el-button>
          <el-button size="medium" icon="el-icon-ksd-table_delete" :disabled="!batchBtnsEnabled.drop" @click="batchDrop">{{$t('jobDrop')}}</el-button>
        </el-button-group><el-button
        plain size="medium" class="ksd-ml-10 ksd-fleft" icon="el-icon-refresh" @click="manualRefreshJobs">{{$t('refreshList')}}</el-button>
      </el-col>
      <el-col :span="6">
        <el-input :placeholder="$t('pleaseSearch')" v-model="filter.key" v-global-key-event.enter.debounce="filterChange" @clear="filterChange()" class="show-search-btn ksd-fright" size="medium" prefix-icon="el-icon-search">
        </el-input>
      </el-col>
    </el-row>
    <el-row class="filter-status-list" v-show="filterTags.length">
      <div class="tag-layout">
        <el-tag size="small" closable v-for="(item, index) in filterTags" :key="index" @close="handleClose(item)">{{`${$t(item.source)}：${$t(item.label)}`}}</el-tag>
      </div>
      <span class="clear-all-tags" @click="handleClearAllTags">{{$t('clearAll')}}</span>
    </el-row>
    <transition name="fade">
      <div class="selectLabel" v-if="isSelectAllShow&&!$store.state.project.isAllProject">
        <span>{{$t('selectedJobs', {selectedNumber: selectedNumber})}}</span>
        <el-checkbox v-model="isSelectAll" @change="selectAllChange">{{$t('selectAll')}}</el-checkbox>
      </div>
    </transition>
    <el-row :gutter="10" id="listBox">
      <el-col :span="showStep?17:24" id="leftTableBox">
        <el-table class="ksd-el-table jobs-table"
          tooltip-effect="dark"
          border
          v-scroll-shadow
          ref="jobsTable"
          :data="jobsList"
          highlight-current-row
          :default-sort = "{prop: 'create_time', order: 'descending'}"
          :empty-text="emptyText"
          @sort-change="sortJobList"
          @selection-change="handleSelectionChange"
          @select="handleSelect"
          @select-all="handleSelectAll"
          @cell-click="showLineSteps"
          :row-class-name="tableRowClassName"
          :key="$store.state.project.isAllProject">
          <el-table-column type="selection" align="center" width="44"></el-table-column>
          <el-table-column align="center" width="40" prop="icon" v-if="monitorActions.includes('jobActions')">
            <template slot-scope="scope">
              <i :class="{
                'el-icon-arrow-right': scope.row.id !== selectedJob.id || !showStep,
                'el-icon-arrow-down': scope.row.id == selectedJob.id && showStep}"></i>
            </template>
          </el-table-column>
          <el-table-column :filters="jobTypeFilteArr.map(item => ({text: $t(item), value: item}))" :filtered-value="filter.job_names" :label="$t('JobType')" filter-icon="el-icon-ksd-filter" :show-multiple-footer="false" :filter-change="(v) => filterContent(v, 'job_names')" prop="job_name" width="144">
            <template slot-scope="scope">
              {{$t(scope.row.job_name)}}
            </template>
          </el-table-column>
          <el-table-column v-if="$store.state.project.isAllProject"
            :label="$t('project')"
            sortable='custom'
            :width="120"
            show-overflow-tooltip
            prop="project">
          </el-table-column>
          <el-table-column
            :label="$t('TargetSubject')"
            sortable="custom"
            min-width="140"
            show-overflow-tooltip
            prop="target_subject">
            <template slot-scope="scope">
              <span :class="{'is-disabled': scope.row.target_subject_error}" v-if="scope.row.job_name === 'TABLE_SAMPLING' || scope.row.target_subject_error">{{scope.row.target_subject}}</span>
              <a class="link" v-else @click="gotoModelList(scope.row)">{{scope.row.target_subject}}</a>
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('dataRange')"
            min-width="180"
            show-overflow-tooltip>
            <template slot-scope="scope">
              <span v-if="scope.row.data_range_end==9223372036854776000">{{$t('fullLoad')}}</span>
              <span v-else>{{scope.row.data_range_start | toServerGMTDate}} - {{scope.row.data_range_end | toServerGMTDate}}</span>
            </template>
          </el-table-column>
          <el-table-column
            width="180"
            :filters="allStatus.map(item => ({text: $t(item), value: item}))" :filtered-value="filter.status" :label="$t('ProgressStatus')" filter-icon="el-icon-ksd-filter" :show-multiple-footer="false" :filter-change="(v) => filterContent(v, 'status')">
            <template slot-scope="scope">
              <kap-progress :percent="scope.row.step_ratio * 100 | number(0)" :status="scope.row.job_status"></kap-progress>
            </template>
          </el-table-column>
          <el-table-column
            width="218"
            :label="$t('startTime')"
            show-overflow-tooltip
            prop="create_time"
            sortable="custom">
            <template slot-scope="scope">
              {{transToGmtTime(scope.row.create_time)}}
            </template>
          </el-table-column>
          <el-table-column
            width="105"
            sortable="custom"
            prop="duration"
            :label="$t('Duration')">
            <template slot-scope="scope">
              {{scope.row.duration/60/1000 | number(2) }}  mins
            </template>
          </el-table-column>
          <el-table-column
            :label="$t('Actions')"
            v-if="monitorActions.includes('jobActions')"
            class-name="job-fc-icon"
            width="96">
            <template slot-scope="scope">
              <common-tip :content="$t('diagnosis')" v-if="monitorActions.includes('diagnostic')">
                <i class="el-icon-ksd-ostin_diagnose ksd-fs-14" @click.stop="showDiagnosisDetail(scope.row.id)"></i>
              </common-tip>
              <common-tip :content="$t('jobDrop')" v-if="scope.row.job_status=='DISCARDED' || scope.row.job_status=='FINISHED'">
                <i class="el-icon-ksd-table_delete ksd-fs-14" @click.stop="drop([scope.row.id], scope.row.project)"></i>
              </common-tip>
              <common-tip :content="$t('jobRestart')" v-if="scope.row.job_status=='ERROR'|| scope.row.job_status=='STOPPED'||scope.row.job_status=='RUNNING'">
                <i class="el-icon-ksd-restart ksd-fs-14" @click.stop="restart([scope.row.id], scope.row.project)"></i>
              </common-tip>
              <common-tip :content="$t('jobResume')" v-if="scope.row.job_status=='ERROR'|| scope.row.job_status=='STOPPED'">
                <i class="el-icon-ksd-table_resume ksd-fs-14" @click.stop="resume([scope.row.id], scope.row.project)"></i>
              </common-tip>
              <common-tip :content="$t('jobPause')" v-if="scope.row.job_status=='RUNNING'|| scope.row.job_status=='PENDING'">
                <i class="el-icon-ksd-pause ksd-fs-14" @click.stop="pause([scope.row.id], scope.row.project)"></i>
              </common-tip>
            </template>
          </el-table-column>
        </el-table>
        <kap-pager :totalSize="jobTotal" :curPage="filter.page_offset+1"  v-on:handleCurrentChange='currentChange' ref="jobPager" class="ksd-mtb-10 ksd-center" ></kap-pager>
      </el-col>
      <el-col :span="7" v-if="showStep" id="rightDetail">
        <el-card v-show="showStep" class="card-width job-step" :class="{'is-admin-tips': $store.state.user.isShowAdminTips&&isAdminRole}" id="stepList">
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
                    <span v-if="selectedJob.job_name === 'TABLE_SAMPLING' || selectedJob.target_subject_error">{{selectedJob.target_subject}}</span>
                    <a class="link" v-else @click="gotoModelList(selectedJob)">{{selectedJob.target_subject}}</a>
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
                  <td>{{$t('waiting')}}</td>
                  <td>{{selectedJob.wait_time/60/1000 | number(2)}} mins</td>
                </tr>
                <tr>
                  <td>{{$t('duration')}}</td>
                  <td class="greyd0">{{selectedJob.duration/60/1000 | number(2)}} mins</td>
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
                  'el-icon-ksd-more_05' : step.step_status=='PENDING'|| step.step_status=='STOPPED',
                  'el-icon-loading' : step.step_status=='WAITING' || step.step_status=='RUNNING',
                  'el-icon-ksd-good_health' : step.step_status=='FINISHED',
                  'el-icon-ksd-error_01' : step.step_status=='ERROR',
                  'el-icon-ksd-table_discard' : step.step_status=='DISCARDED'
                }">
                </i>
                <ul >
                  <li>{{$t('sequenceId')}}: {{step.sequence_id}}</li>
                  <li>{{$t('kylinLang.common.status')}}: {{step.step_status}}</li>
                  <li>{{$t('waiting')}}: {{step.wait_time/60/1000 | number(2)}} mins</li>
                  <li>{{$t('duration')}}: {{step.duration/60/1000 | number(2)}} mins</li>
                  <li>{{$t('startTime')}}: {{transToGmtTime(step.exec_start_time !=0 ? step.exec_start_time:'')}}</li>
                  <li>{{$t('endTime')}}: {{transToGmtTime(step.exec_end_time !=0 ? step.exec_end_time :'')}}</li>
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
                    <span class="jobActivityLabel">{{$t('waiting')}}: </span>
                    <span v-if="step.wait_time">{{step.wait_time/60/1000 | number(2)}} mins</span>
                    <span v-else>0</span>
                  </div>
                  <div>
                    <span class="jobActivityLabel">{{$t('duration')}}: </span>
                    <span v-if="step.duration">{{step.duration/60/1000 | number(2)}} mins</span>
                    <span v-else>0
                      <!-- <img src="../../assets/img/dot.gif" height="12px" width="10px"/> -->
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
      </el-col>
    </el-row>
    <el-dialog :title="$t('waitingJobList')" limited-area :close-on-press-escape="false" :close-on-click-modal="false" :visible.sync="waitingJobListVisibel" width="480px">
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
        <kap-pager :totalSize="waitingJob.jobsSize" :curPage="waittingJobsFilter.offset+1" v-on:handleCurrentChange='waitingJobsCurrentChange' ref="waitingJobPager" class="ksd-mtb-10 ksd-center" ></kap-pager>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" @click="waitingJobListVisibel = false">{{$t('kylinLang.common.ok')}}</el-button>
      </span>
    </el-dialog>
    <el-dialog
      id="show-diagnos"
      limited-area
      :title="stepAttrToShow == 'cmd' ? $t('parameters') : $t('output')"
      :visible.sync="dialogVisible"
      :close-on-press-escape="false"
      :close-on-click-modal="false">
      <job_dialog :stepDetail="outputDetail" :stepId="stepId" :jobId="selectedJob.id" :targetProject="selectedJob.project"></job_dialog>
      <span slot="footer" class="dialog-footer">
        <el-button plain size="medium" @click="dialogVisible = false">{{$t('kylinLang.common.close')}}</el-button>
      </span>
    </el-dialog>
    <diagnostic v-if="showDiagnostic" @close="showDiagnostic = false" :jobId="diagnosticId"/>
  </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters, mapMutations } from 'vuex'
import jobDialog from './job_dialog'
import TWEEN from '@tweenjs/tween.js'
import $ from 'jquery'
import { pageCount } from '../../config'
import { transToGmtTime, handleError, handleSuccess } from 'util/business'
import { cacheLocalStorage, indexOfObjWithSomeKey, objectClone } from 'util/index'
import Diagnostic from 'components/admin/Diagnostic/index'
@Component({
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      loadJobsList: 'LOAD_JOBS_LIST',
      getJobDetail: 'GET_JOB_DETAIL',
      loadStepOutputs: 'LOAD_STEP_OUTPUTS',
      removeJob: 'REMOVE_JOB',
      removeJobForAll: 'ROMOVE_JOB_FOR_ALL',
      pauseJob: 'PAUSE_JOB',
      restartJob: 'RESTART_JOB',
      resumeJob: 'RESUME_JOB',
      losdWaittingJobModels: 'LOAD_WAITTING_JOB_MODELS',
      laodWaittingJobsByModel: 'LOAD_WAITTING_JOBS_BY_MODEL'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    ...mapMutations({
      setProject: 'SET_PROJECT'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'monitorActions',
      'isAdminRole'
    ])
  },
  components: {
    'job_dialog': jobDialog,
    Diagnostic
  },
  locales: {
    'en': {
      dataRange: 'Data Range',
      JobType: 'Job Type',
      JobName: 'Job Name',
      TargetSubject: 'Target Subject',
      ProgressStatus: 'Job Status',
      startTime: 'Start Time',
      endTime: 'End Time',
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
      resumeJob: 'Do you really need to resume {count} job records?',
      resumeJobTitle: 'Resume Job',
      restartJob: 'Do you really need to restart {count} job records?',
      restartJobTitle: 'Restart Job',
      pauseJob: 'Do you really need to pause {count} job records?',
      pauseJobTitle: 'Pause Job',
      dropJob: 'Do you really need to delete {count} job records?',
      dropJobTitle: 'Drop Job',
      jobName: 'Job Name',
      duration: 'Duration',
      waiting: 'Waiting',
      noSelectJobs: 'Please check at least one job.',
      selectedJobs: '{selectedNumber} jobs have been selected. ',
      selectAll: 'All Select',
      fullLoad: 'Full Load',
      jobDetails: 'Job Details',
      waitingjobs: 'initializing job(s)',
      jobs: 'Job(s)',
      waitingJobList: 'Initializing job List',
      triggerTime: 'Trigger Time',
      order: 'Order',
      jobTarget: 'Job Target: ',
      jobsList: 'Jobs List',
      sparkJobTip: 'Spark Job',
      logInfoTip: 'Log Output',
      openJobSteps: 'Open Job Steps',
      sequenceId: 'Sequence ID',
      INDEX_REFRESH: 'Refresh Data',
      INDEX_MERGE: 'Merge Data',
      INDEX_BUILD: 'Build Index',
      INC_BUILD: 'Load Data',
      TABLE_SAMPLING: 'Sample Table',
      project: 'Project',
      adminTips: 'Admin user can view all job information via Select All option in the project list.',
      clearAll: 'Clear All',
      filter: 'Filter',
      refreshList: 'Refresh List',
      pleaseSearch: 'Search Target Subject or Job ID',
      diagnosis: 'Diagnosis'
    },
    'zh-cn': {
      dataRange: '数据范围',
      JobType: 'Job 类型',
      JobName: '任务',
      TargetSubject: '任务对象',
      ProgressStatus: '任务状态',
      startTime: '任务开始时间',
      endTime: '任务结束时间',
      Duration: '耗时',
      Actions: '操作',
      jobResume: '恢复',
      jobDiscard: '终止',
      jobPause: '暂停',
      jobDrop: '删除',
      jobRestart: '重启',
      tip_jobResume: '恢复 Job',
      tip_jobPause: '暂停 Job',
      tip_jobDiscard: '终止 Job',
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
      resumeJob: '您确认要恢复 {count} 个任务记录？',
      resumeJobTitle: '恢复任务',
      restartJob: '您确认要重启 {count} 个任务记录？',
      restartJobTitle: '重启任务',
      pauseJob: '您确认要暂停 {count} 个任务记录？',
      pauseJobTitle: '暂停任务',
      dropJob: '您确认要删除 {count} 个任务记录？',
      dropJobTitle: '删除任务',
      jobName: '任务名',
      duration: '持续时间',
      waiting: '等待时间',
      noSelectJobs: '请勾选至少一项任务。',
      selectedJobs: '目前已选择当页 {selectedNumber} 条任务。',
      selectAll: '全选',
      fullLoad: '全量加载',
      jobDetails: '任务详情',
      waitingjobs: '个初始化的任务',
      jobs: '个任务',
      waitingJobList: '初始化任务列表',
      triggerTime: '触发时间',
      order: '排序',
      jobTarget: '任务目标：',
      jobsList: '任务列表',
      sparkJobTip: 'Spark任务详情',
      logInfoTip: '日志详情',
      openJobSteps: '展开任务详情',
      sequenceId: '序列号',
      INDEX_REFRESH: '刷新数据',
      INDEX_MERGE: '合并数据',
      INDEX_BUILD: '构建索引',
      INC_BUILD: '加载数据',
      TABLE_SAMPLING: '抽样表数据',
      project: '项目',
      adminTips: '系统管理员可以在项目列表中选择全部项目，查看所有项目下的任务信息。',
      clearAll: '清除所有',
      filter: '筛选',
      refreshList: '刷新列表',
      pleaseSearch: '搜索任务对象或任务 ID',
      diagnosis: '诊断包'
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
    page_offset: 0,
    page_size: pageCount,
    time_filter: 4,
    job_names: [],
    sort_by: 'create_time',
    reverse: true,
    status: [],
    key: ''
  }
  waittingJobsFilter = {
    offset: 0,
    limit: 10
  }
  jobsList = []
  jobTotal = 0
  allStatus = ['PENDING', 'RUNNING', 'FINISHED', 'ERROR', 'DISCARDED', 'STOPPED']
  jobTypeFilteArr = ['INDEX_REFRESH', 'INDEX_MERGE', 'INDEX_BUILD', 'INC_BUILD', 'TABLE_SAMPLING']
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
  stepId = ''
  filterTags = []
  showDiagnostic = false
  diagnosticId = ''

  get emptyText () {
    return this.filter.key || this.filter.job_names.length || this.filter.status.length ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  @Watch('$store.state.project.isAllProject')
  selectAllProject (curVal) {
    if (curVal) {
      delete this.filter.project
      this.jobsList = []
      this.$nextTick(() => {
        this.loadJobsList(this.filter)
      })
    }
  }
  closeTips () {
    this.$store.state.user.isShowAdminTips = false
    cacheLocalStorage('isHideAdminTips', true)
    // this.scrollRightBar(null, true)
  }
  handleCommand (uuid) {
    this.waitingJobListVisibel = true
    this.waittingJobsFilter.project = this.currentSelectedProject
    this.waittingJobsFilter.model = uuid
    this.waitingJob.modelName = this.waittingJobModels.data[uuid].model_alias
    this.getWaittingJobs()
  }
  getWaittingJobs () {
    this.laodWaittingJobsByModel(this.waittingJobsFilter).then((res) => {
      handleSuccess(res, (data) => {
        this.waitingJob.jobsList = data.value
        this.waitingJob.jobsSize = data.total_size
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
  gotoModelList (item) {
    // 暂停轮询，清掉计时器
    clearTimeout(this.stCycle)
    this.isPausePolling = true
    // 如果是全 project 模式，需要先改变当前 project 选中值
    if (this.$store.state.project.isAllProject) {
      this.setProject(item.project)
    }
    this.$router.push({name: 'ModelList', params: { modelAlias: item.target_subject }})
  }
  renderColumn (h) {
    let items = []
    for (let i = 0; i < this.jobTypeFilteArr.length; i++) {
      items.push(<el-checkbox label={this.jobTypeFilteArr[i]} key={this.jobTypeFilteArr[i]}>{this.$t(this.jobTypeFilteArr[i])}</el-checkbox>)
    }
    return (<span>
      <span>{this.$t('JobType')}</span>
      <el-popover
        ref="jobTypeFilterPopover"
        placement="bottom"
        popperClass="filter-popover">
        <el-checkbox-group class="filter-groups" value={this.filter.job_names} onInput={val => (this.filter.job_names = val)} onChange={this.filterChange2}>
          {items}
        </el-checkbox-group>
        <i class={this.filter.job_names.length ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
      </el-popover>
    </span>)
  }
  renderColumn2 (h) {
    let items = []
    for (let i = 0; i < this.allStatus.length; i++) {
      items.push(
        <el-checkbox label={this.allStatus[i]} key={this.allStatus[i]}>{this.$t(this.allStatus[i])}</el-checkbox>
      )
    }
    return (<span>
      <span>{this.$t('ProgressStatus')}</span>
      <el-popover placement="bottom" popperClass="filter-popover">
        <el-checkbox-group class="filter-groups" value={this.filter.status} onInput={val => (this.filter.status = val)} onChange={this.filterChange2}>
          {items}
        </el-checkbox-group>
        <i class={this.filter.status.length ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
      </el-popover>
    </span>)
  }
  // 清除所有的tags
  handleClearAllTags () {
    this.filter.page_offset = 0
    this.filter.job_names.splice(0, this.filter.job_names.length)
    this.filter.status.splice(0, this.filter.status.length)
    this.filterTags = []
    this.manualRefreshJobs()
  }
  autoFilter () {
    clearTimeout(this.stCycle)
    this.stCycle = setTimeout(() => {
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
    if (document.getElementById('scrollContent')) {
      document.getElementById('scrollContent').addEventListener('scroll', this.scrollRightBar, false)
    }
  }
  beforeDestroy () {
    window.clearTimeout(this.stCycle)
    window.clearTimeout(this.scrollST)
    // window.removeEventListener('click', this.closeIt)
    if (document.getElementById('scrollContent')) {
      document.getElementById('scrollContent').removeEventListener('scroll', this.scrollRightBar, false)
    }
  }
  getJobsList () {
    return new Promise((resolve, reject) => {
      if (!this.currentSelectedProject) return reject()
      let data = {}
      const statuses = this.filter.status.join(',')
      Object.keys(this.filter).forEach(key => key !== 'status' && (data[key] = this.filter[key]))
      this.loadJobsList({...data, statuses}).then((res) => {
        handleSuccess(res, (data) => {
          if (data.total_size) {
            this.jobsList = data.value
            if (this.selectedJob) {
              const selectedIndex = indexOfObjWithSomeKey(this.jobsList, 'id', this.selectedJob.id)
              if (selectedIndex !== -1) {
                this.getJobDetail({project: this.selectedJob.project, job_id: this.selectedJob.id}).then((res) => {
                  handleSuccess(res, (data) => {
                    this.selectedJob = this.jobsList[selectedIndex]
                    this.selectedJob['details'] = data
                  })
                }, (resError) => {
                  handleError(resError)
                })
              }
            }
            if (this.multipleSelection.length) {
              const cloneSelections = objectClone(this.multipleSelection)
              this.multipleSelection = []
              cloneSelections.forEach((m) => {
                const index = indexOfObjWithSomeKey(this.jobsList, 'id', m.id)
                if (index !== -1) {
                  this.$nextTick(() => {
                    this.$refs.jobsTable.toggleRowSelection(this.jobsList[index])
                  })
                }
              })
            }
            this.jobTotal = data.total_size
          } else {
            this.jobsList = []
            this.jobTotal = 0
          }
          this.searchLoading = false
        })
        resolve()
      }, (res) => {
        handleError(res)
        this.searchLoading = false
        reject()
      })
    })
  }
  getWaittingJobModels () {
    return new Promise((resolve, reject) => {
      if (!this.currentSelectedProject) return reject()
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
      return ''
    }
    if (this.selectedJob.job_status === 'FINISHED') {
      return 'success'
    }
    if (this.selectedJob.job_status === 'ERROR') {
      return 'danger'
    }
    if (this.selectedJob.job_status === 'DISCARDED') {
      return 'info'
    }
    if (this.selectedJob.job_status === 'STOPPED') {
      return ''
    }
  }
  setRightBarTop () {
    // 默认右侧详情的位移为 0
    let result = 0
    // 左边列表区域的高度
    let leftTableH = document.getElementById('leftTableBox').clientHeight
    // 右侧详情的高度
    let rightStepDetailH = document.getElementById('stepList').clientHeight
    // 可视区剔除掉导航头后的高度
    let screenH = document.documentElement.clientHeight - 52
    // 当前滚动距离
    let sTop = document.getElementById('scrollContent').scrollTop
    // this.beforeScrollPos = sTop
    // 整个列表区距离顶部的位移
    let listBoxOffsetTop = document.getElementById('listBox').offsetTop
    /*
      1、列表在一屏内，详情也在一屏，相当于没有滚动条，不做啥处理
      2、列表在一屏内，详情超出一屏幕，让详情跟着滚，其实同1
      3、列表超出一屏，详情在一屏内，让详情始终顶边即可
      4、列表超出一屏，详情也超出一屏，列表高度比详情高度小，这种只能跟着滚，不做顶边
      5、列表超出一屏，详情也超出一屏，列表高度比详情高度大，详情不断改位置，一直到，详情底部位置和列表一致了，位置就保持不变了
    */
    // 列表在一屏幕以内的，都是跟着滚，保持 0 即可，其余情况开始判断
    if (leftTableH > screenH) {
      if (rightStepDetailH <= screenH) { // 详情在一屏幕内的，滚动后超出界面了，保持顶边，其余时候也是 0
        if (sTop > listBoxOffsetTop) {
          result = sTop - listBoxOffsetTop
        }
      } else {
        // 列表超出一屏，详情也超出一屏，列表高度比详情高度大，详情不断改位置，一直到，详情底部位置和列表一致了，位置就保持不变了，其余情况就还是跟着滚的 0
        if (leftTableH > rightStepDetailH) {
          let temp = leftTableH - rightStepDetailH
          if (sTop > listBoxOffsetTop) {
            result = sTop - listBoxOffsetTop > temp ? temp : sTop - listBoxOffsetTop
          }
        }
      }
    }
    if (document.getElementById('stepList')) {
      document.getElementById('stepList').style.top = result + 'px'
    }
  }
  scrollRightBar (e, needRizeTop) {
    clearTimeout(this.scrollST)
    this.scrollST = setTimeout(() => {
      if (this.showStep) {
        this.setRightBarTop()
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
    this.loadList()
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
      this.isPausePolling = false
      this.multipleSelection = []
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
    if (!this.batchBtnsEnabled.resume) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.resume([], this.currentSelectedProject, 'batchAll')
      } else {
        const jobIds = this.getJobIds()
        this.resume(jobIds, this.currentSelectedProject, 'batch')
      }
    }
  }
  batchRestart () {
    if (!this.batchBtnsEnabled.restart) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.restart([], this.currentSelectedProject, 'batchAll')
      } else {
        const jobIds = this.getJobIds()
        this.restart(jobIds, this.currentSelectedProject, 'batch')
      }
    }
  }
  batchPause () {
    if (!this.batchBtnsEnabled.pause) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.pause([], this.currentSelectedProject, 'batchAll')
      } else {
        const jobIds = this.getJobIds()
        this.pause(jobIds, this.currentSelectedProject, 'batch')
      }
    }
  }
  batchDrop () {
    if (!this.batchBtnsEnabled.drop) return
    if (!this.multipleSelection.length) {
      this.$message.warning(this.$t('noSelectJobs'))
    } else {
      if (this.isSelectAll && this.isSelectAllShow) {
        this.drop([], this.currentSelectedProject, 'batchAll')
      } else {
        const jobIds = this.getJobIds()
        this.drop(jobIds, this.currentSelectedProject, 'batch')
      }
    }
  }
  resetSelection () {
    this.isSelectAllShow = false
    this.isSelectAll = false
    this.multipleSelection = []
    this.$refs.jobsTable && this.$refs.jobsTable.clearSelection()
    this.idsArrCopy = []
    this.idsArr = []
  }
  currentChange (size, count) {
    this.filter.page_offset = size
    this.filter.page_size = count
    this.resetSelection()
    this.getJobsList()
    this.closeIt()
  }
  waitingJobsCurrentChange (size, count) {
    this.waittingJobsFilter.offset = size
    this.waittingJobsFilter.limit = count
    this.getWaittingJobs()
  }
  closeIt () {
    if (this.showStep) {
      this.showStep = false
    }
  }
  filterChange (val) {
    this.searchLoading = true
    this.filter.page_offset = 0
    this.manualRefreshJobs()
    this.showStep = false
  }
  filterChange2 (status) {
    this.filter.page_offset = 0
    this.manualRefreshJobs()
    this.showStep = false
  }
  tableRowClassName ({row, rowIndex}) {
    if (row.id === this.selectedJob.id && this.showStep) {
      return 'current-row2'
    }
  }
  loadList () {
    if (this.$store.state.project.isAllProject) {
      delete this.filter.project
      return this.getJobsList()
    } else {
      this.filter.project = this.currentSelectedProject
      return Promise.all([this.getJobsList(), this.getWaittingJobModels()])
    }
  }
  manualRefreshJobs () {
    this.resetSelection()
    this.loadList()
  }
  refreshJobs () {
    if (!this.isPausePolling) {
      return this.loadList()
    } else {
      return new Promise((resolve) => {
        resolve()
      })
    }
  }
  sortJobList ({ column, prop, order }) {
    if (order === 'ascending') {
      this.filter.reverse = false
    } else {
      this.filter.reverse = true
    }
    this.filter.sort_by = prop
    this.filter.page_offset = 0
    this.manualRefreshJobs()
  }
  async resume (jobIds, project, isBatch) {
    await this.callGlobalDetailDialog({
      msg: this.$t('resumeJob', {count: (isBatch && isBatch === 'batchAll') ? this.selectedNumber : jobIds.length}),
      title: this.$t('resumeJobTitle'),
      details: jobIds,
      dialogType: 'tip',
      showDetailBtn: false
    })
    const resumeData = {job_ids: jobIds, project: project, action: 'RESUME'}
    if (this.$store.state.project.isAllProject && isBatch) {
      delete resumeData.project
    }
    this.resumeJob(resumeData).then(() => {
      if (isBatch) {
        if (isBatch === 'batchAll') {
          this.filter.status = ''
        }
      }
      this.manualRefreshJobs()
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.actionSuccess')
      })
    }).catch((res) => {
      handleError(res)
    })
  }
  async restart (jobIds, project, isBatch) {
    await this.callGlobalDetailDialog({
      msg: this.$t('restartJob', {count: (isBatch && isBatch === 'batchAll') ? this.selectedNumber : jobIds.length}),
      title: this.$t('restartJobTitle'),
      details: jobIds,
      dialogType: 'tip',
      showDetailBtn: false
    })
    const restartData = {job_ids: jobIds, project: project, action: 'RESTART'}
    if (this.$store.state.project.isAllProject && isBatch) {
      delete restartData.project
    }
    this.restartJob(restartData).then(() => {
      if (isBatch) {
        if (isBatch === 'batchAll') {
          this.filter.status = ''
        }
      }
      this.manualRefreshJobs()
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.actionSuccess')
      })
    }).catch((res) => {
      handleError(res)
    })
  }
  async pause (jobIds, project, isBatch) {
    await this.callGlobalDetailDialog({
      msg: this.$t('pauseJob', {count: (isBatch && isBatch === 'batchAll') ? this.selectedNumber : jobIds.length}),
      title: this.$t('pauseJobTitle'),
      details: jobIds,
      dialogType: 'tip',
      showDetailBtn: false
    })
    const pauseData = {job_ids: jobIds, project: project, action: 'PAUSE'}
    if (this.$store.state.project.isAllProject && isBatch) {
      delete pauseData.project
    }
    this.pauseJob(pauseData).then(() => {
      if (isBatch) {
        if (isBatch === 'batchAll') {
          this.filter.status = ''
        }
      }
      this.manualRefreshJobs()
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.actionSuccess')
      })
    }).catch((res) => {
      handleError(res)
    })
  }
  async drop (jobIds, project, isBatch) {
    await this.callGlobalDetailDialog({
      msg: this.$t('dropJob', {count: (isBatch && isBatch === 'batchAll') ? this.selectedNumber : jobIds.length}),
      title: this.$t('dropJobTitle'),
      details: jobIds,
      dialogType: 'warning',
      showDetailBtn: false
    })
    const dropData = {job_ids: jobIds, project: project}
    let removeJobType = 'removeJob'
    if (this.$store.state.project.isAllProject && isBatch) {
      delete dropData.project
      removeJobType = 'removeJobForAll'
    }
    this[removeJobType](dropData).then(() => {
      if (isBatch) {
        if (isBatch === 'batchAll') {
          this.filter.status = ''
        }
      }
      this.manualRefreshJobs()
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.delSuccess')
      })
    }).catch((res) => {
      handleError(res)
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
      this.getJobDetail({project: this.selectedJob.project, job_id: row.id}).then((res) => {
        handleSuccess(res, (data) => {
          this.$nextTick(() => {
            this.$set(this.selectedJob, 'details', data)
            this.setRightBarTop()
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
    this.loadStepOutputs({jobId: this.selectedJob.id, stepId: step.id, project: this.selectedJob.project}).then((res) => {
      handleSuccess(res, (data) => {
        this.outputDetail = data.cmd_output
        this.stepId = step.id
      })
    }, (resError) => {
      this.outputDetail = this.$t('cmdOutput')
    })
  }
  // timerlineDuration (step) {
  //   let min = 0
  //   if (!step.exec_start_time || !step.exec_end_time) {
  //     return '0 seconds'
  //   } else {
  //     min = (step.exec_end_time - step.exec_start_time) / 1000 / 60
  //     return min.toFixed(2) + ' mins'
  //   }
  // }
  closeLoginOpenKybot () {
    this.kyBotUploadVisible = false
    this.infoKybotVisible = true
  }
  // 查询状态过滤回调函数
  filterContent (val, type) {
    const maps = {
      job_names: 'JobType',
      status: 'ProgressStatus'
    }

    this.filterTags = this.filterTags.filter((item, index) => item.key !== type || item.key === type && val.includes(item.label))
    const list = this.filterTags.filter(it => it.key === type).map(it => it.label)
    val.length && val.forEach(item => {
      if (!list.includes(item)) {
        this.filterTags.push({label: item, source: maps[type], key: type})
      }
    })
    this.filter[type] = val
    this.filter.page_offset = 0
    this.manualRefreshJobs()
  }
  // 删除单个筛选条件
  handleClose (tag) {
    const index = this.filter[tag.key].indexOf(tag.label)
    index > -1 && this.filter[tag.key].splice(index, 1)
    this.filterTags = this.filterTags.filter(item => item.key !== tag.key || item.key === tag.key && tag.label !== item.label)
    this.filter.page_offset = 0
    this.manualRefreshJobs()
  }
  showDiagnosisDetail (id) {
    this.diagnosticId = id
    this.showDiagnostic = true
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
      background-color: @base-color-9;
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
    }
    .waiting-jobs {
      color: @text-title-color;
      &:hover {
        .el-icon-arrow-down {
          color: @base-color;
        }
      }
      .el-button {
        &:hover {
          background-color: @fff;
        }
        &.is-disabled {
          .el-icon-arrow-down {
            color: inherit;
            cursor: not-allowed;
          }
          &:hover {
            background-color: @background-disabled-color;
            .el-icon-arrow-down {
              color: inherit;
            }
          }
        }
      }
    }
    .el-progress-bar__innerText {
      top: -1px;
      position: relative;
    }
    .job-step {
      // width: 30%;
      min-height: calc(~'100vh - 167px');
      box-sizing: border-box;
      position: relative;
      // z-index: 100;
      // position: absolute;
      // top: 0;
      // right: 0;
      &.is-admin-tips {
        min-height: calc(~'100vh - 181px');
      }
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
              a {
                color: @base-color;
                &.link{
                  text-decoration: underline;
                }
              }
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
            font-weight: @font-medium;
          }
        }
      }
      .time-hd {
        height:20px;
        line-height:20px;
        margin:15px 0 10px 0;
        font-size: 14px;
        font-weight: @font-medium;
      }
      .job-btn {
        position: absolute;
        left: -1px;
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
        &:hover i {
          color: @base-color;
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
                font-weight: @font-medium;
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
      span.is-disabled {
        color: @text-disabled-color;
      }
      .link{
        text-decoration: underline;
        color:@base-color;
      }
      .el-icon-ksd-filter {
        position: relative;
        font-size: 17px;
        top: 2px;
        left: 5px;
        &:hover,
        &.filter-open {
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
      tr .el-icon-arrow-right, tr .el-icon-arrow-down {
        position:absolute;
        left:15px;
        top:50%;
        transform:translate(0,-50%);
        font-size:12px;
        &:hover{
          color:@base-color;
        }
      }
      tr .el-icon-arrow-down{
        color:@base-color;
      }
      .job-fc-icon {
        .tip_box {
          margin-left: 10px;
          &:first-child {
            margin-left: 0;
          }
        }
      }
    }
  }
  .jobs-dropdown {
    min-width: 80px;
    .el-dropdown-menu__item {
      &:focus {
        background-color: inherit;
        color: inherit;
      }
      &.active {
        background-color: @base-color-9;
        color: @base-color-2;
      }
    }
  }
  .filter-status-list {
    background: @background-disabled-color;
    margin-bottom: 10px;
    padding: 0px 5px 5px;
    box-sizing: border-box;
    .tag-layout {
      width: calc(~'100% - 100px');
      display: inline-block;
      .el-tag {
        margin-left: 5px;
        margin-top: 5px;
      }
    }
    .clear-all-tags {
      position: absolute;
      top: 8px;
      right: 10px;
      font-size: 14px;
      color: @base-color;
      cursor: pointer;
    }
  }
</style>
