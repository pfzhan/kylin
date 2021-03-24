import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import Vuex from 'vuex'
import * as business from '../../../util/business'
import * as utils from '../../../util/index'
import JobsList from '../jobs.vue'
import JobDialog from '../job_dialog.vue'
import Diagnostic from '../../admin/Diagnostic/store.js'
import kapPager from 'components/common/kap_pager.vue'
import commonTip from 'components/common/common_tip.vue'

jest.useFakeTimers()

const loadJobsList = jest.fn().mockImplementation(() => {
  return {
    then: (callback, errorCallback) => {
      callback({total_size: 1, value: [{id: 'job1'}]})
      errorCallback()
    }
  }
})
const getJobDetail = jest.fn().mockImplementation(() => {
  return {
    then: (callback, errorCallback) => {
      callback({id: 'job1', info: {}})
      errorCallback && errorCallback()
    }
  }
})
const loadStepOutputs = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const removeJob = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const removeJobForAll = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const pauseJob = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const restartJob = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const resumeJob = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const discardJob = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})

const setProject = jest.fn()

let handleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback, errorCallback) => {
  callback(res)
  errorCallback && errorCallback()
})
const handleError = jest.spyOn(business, 'handleError').mockImplementation(() => {})

const mockKapConfirm = jest.spyOn(business, 'kapConfirm').mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const mockKapWarn = jest.spyOn(business, 'kapWarn').mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
let mockGetQueryString = jest.spyOn(utils, 'getQueryString').mockImplementation(() => 'pc')
let mockPostCloudUrlMessage = jest.spyOn(business, 'postCloudUrlMessage').mockImplementation()

const $message = {
  success: jest.fn(),
  warning: jest.fn(),
  error: jest.fn()
}
const $alert = jest.fn()

global.clearTimeout = jest.fn()
global.setTimeout = jest.fn()

const mockApi = {
  mockCallGlobalDetailDialog: jest.fn().mockImplementation(() => {
    return new Promise((resolve, reject) => {
      resolve()
    })
  })
}
const DetailDialogModal = {
  namespaced: true,
  actions: {
    'CALL_MODAL': mockApi.mockCallGlobalDetailDialog
  }
}

const store = new Vuex.Store({
  state: {
    system: {lang: 'en', isShowGlobalAlter: false},
    user: {isShowAdminTips: true},
    config: {platform: ''},
    project: {isAllProject: false}
  },
  getters: {
    currentSelectedProject () {
      return 'learn_kylin'
    },
    isAdminRole () {
      return true
    },
    monitorActions () {
      return ['jobActions', 'diagnostic']
    }
  },
  actions: {
    LOAD_JOBS_LIST: loadJobsList,
    GET_JOB_DETAIL: getJobDetail,
    LOAD_STEP_OUTPUTS: loadStepOutputs,
    REMOVE_JOB: removeJob,
    ROMOVE_JOB_FOR_ALL: removeJobForAll,
    PAUSE_JOB: pauseJob,
    RESTART_JOB: restartJob,
    RESUME_JOB: resumeJob,
    DISCARD_JOB: discardJob

  },
  mutations: {
    SET_PROJECT: setProject
  },
  modules: {
    Diagnostic,
    DetailDialogModal
  }
})

// const modelJob = shallowMount(JobDialog, { localVue, store, propsData: {stepDetail: {}, stepId: 'stepId1', jobId: 'jobId1', targetProject: 'learn_kylin'} })
// const modelDiagnostic = shallowMount(Diagnostic, { localVue, store, propsData: {jobId: 'jobId1'}, mocks: {$route: {name: 'Job'}} })
const mockGlobalConfirm = jest.fn().mockResolvedValue('')

const mockPush = jest.fn().mockImplementation((item) => {
  wrapper.vm.$route = item
})

const mockCallGlobalDetail = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})

const wrapper = shallowMount(JobsList, {
  localVue,
  store,
  mocks: {
    handleSuccess: handleSuccess,
    handleError: handleError,
    kapConfirm: mockKapConfirm,
    kapWarn: mockKapWarn,
    $message: $message,
    $alert: $alert,
    $confirm: mockGlobalConfirm,
    $route: {query: {modelAlias: null, jobStatus: null}},
    $router: {push: mockPush},
    postCloudUrlMessage: mockPostCloudUrlMessage,
    getQueryString: mockGetQueryString
  },
  components: {
    JobDialog,
    Diagnostic,
    kapPager,
    commonTip
  }
})

jest.useFakeTimers()

describe('Component Monitor', () => {
  beforeEach(() => {
    wrapper.vm.$refs.jobsTable = {
      clearSelection: jest.fn(),
      toggleRowSelection: jest.fn()
    }
  })
  it('computed events', async () => {
    expect(wrapper.vm.emptyText).toEqual('No data')
    await wrapper.setData({ filter: {...wrapper.vm.filter, key: 'job1', job_names: [], status: []} })
    // await wrapper.update()
    expect(wrapper.vm.emptyText).toEqual('No Results')

    expect(wrapper.vm.isShowAdminTips).toBeTruthy()

    await wrapper.setData({ selectedJob: {job_status: 'PENDING'} })
    // await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('gray')
    await wrapper.setData({ selectedJob: {job_status: 'RUNNING'} })
    // await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('')
    await wrapper.setData({ selectedJob: {job_status: 'FINISHED'} })
    // await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('success')
    await wrapper.setData({ selectedJob: {job_status: 'ERROR'} })
    // await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('danger')
    await wrapper.setData({ selectedJob: {job_status: 'DISCARDED'} })
    // await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('info')
    await wrapper.setData({ selectedJob: {job_status: 'STOPPED'} })
    // await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('')

    wrapper.setData({ filter: {project: 'learn_kylin', key: '', job_names: [], status: []} })
    wrapper.vm.$store.state.project.isAllProject = true
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.filter.project).toBeUndefined()
  })
  it('methods events', async () => {
    wrapper.vm.closeTips()
    expect(wrapper.vm.$store.state.user.isShowAdminTips).toBeFalsy()
    expect(localStorage.getItem('isHideAdminTips')).toBeTruthy()

    await wrapper.setData({ waittingJobModels: {data: {uuid: {model_alias: 'model_alias'}}} })
    // await wrapper.update()
    wrapper.vm.handleCommand('uuid')
    expect(wrapper.vm.waitingJobListVisibel).toBeTruthy()
    expect(wrapper.vm.waittingJobsFilter.project).toEqual('learn_kylin')
    expect(wrapper.vm.waittingJobsFilter.model).toEqual('uuid')
    expect(wrapper.vm.waitingJob.modelName).toEqual('model_alias')

    wrapper.vm.getBatchBtnStatus(['PENDING'])
    expect(wrapper.vm.batchBtnsEnabled).toEqual({resume: false, restart: false, pause: true, discard: true, drop: false})
    wrapper.vm.getBatchBtnStatus(['STOPPED'])
    expect(wrapper.vm.batchBtnsEnabled).toEqual({resume: true, restart: true, pause: false, discard: true, drop: false})

    let isContain = wrapper.vm.isContain(['PENDING', 'RUNNING', 'ERROR', 'STOPPED'], ['ERROR', 'STOPPED'])
    expect(isContain).toBeTruthy()
    isContain = wrapper.vm.isContain(['PENDING', 'RUNNING', 'ERROR', 'STOPPED'], ['ERROR', 'FINISHED'])
    expect(isContain).toBeFalsy()

    expect(wrapper.vm.getTargetSubject({target_subject: 'The snapshot is deleted'})).toBe('The snapshot is deleted')
    expect(wrapper.vm.getTargetSubject({target_subject: 'The model is deleted'})).toBe('The model is deleted')
    expect(wrapper.vm.getTargetSubject({target_subject: ''})).toBe('')


    wrapper.vm.$store.state.project.isAllProject = true
    await wrapper.vm.$nextTick()
    wrapper.vm.gotoModelList({ project: 'learn_kylin', target_subject: 'target_subject'})
    expect(clearTimeout).toBeCalled()
    expect(wrapper.vm.isPausePolling).toBeTruthy()
    expect(setProject).toBeCalled()
    expect(wrapper.vm.$route.name).toEqual('ModelList')

    await wrapper.setData({ filter: {page_offset: 1, key: '', job_names: ['job_names'], status: ['status']}, filterTags: ['filterTags'] })
    // await wrapper.update()

    wrapper.vm.gotoSnapshotList({project: 'kyligence'})
    expect(setProject.mock.calls[0][1]).toBe('learn_kylin')
    expect(wrapper.vm.$route.name).toEqual('Snapshot')

    mockGetQueryString = jest.spyOn(utils, 'getQueryString').mockImplementation(() => 'iframe')
    await wrapper.vm.$nextTick()
    wrapper.vm.gotoSnapshotList({project: 'kyligence'})
    expect(setProject.mock.calls[1][1]).toBe('kyligence')
    expect(mockPostCloudUrlMessage.mock.calls[0][1]).toEqual({"name": "Snapshot", "params": {"table": undefined}})

    wrapper.vm.gotoModelList({ project: 'learn_kylin', target_subject: 'target_subject'})
    expect(clearTimeout).toBeCalled()
    expect(wrapper.vm.isPausePolling).toBeTruthy()
    expect(setProject).toBeCalled()
    expect(mockPostCloudUrlMessage.mock.calls[1][1]).toEqual({"name": "ModelList", "params": {"modelAlias": "target_subject"}})

    wrapper.vm.handleClearAllTags()
    expect(wrapper.vm.filter.page_offset).toEqual(0)
    expect(wrapper.vm.filter.job_names).toEqual([])
    expect(wrapper.vm.filter.status).toEqual([])
    expect(wrapper.vm.filterTags).toEqual([])

    const options = {
      stCycle: 1,
      _isDestroyed: false,
      handleSuccess,
      handleError,
      refreshJobs: jest.fn().mockImplementation(() => {
        return {
          then: (callback, errorCallback) => {
            callback()
            errorCallback()
          }
        }
      }),
      autoFilter: jest.fn(),
      filter: {
        isAuto: false
      }
    }
    await JobsList.options.methods.autoFilter.call(options)
    jest.runAllTimers()
    expect(handleError).toBeCalled()
    expect(options.autoFilter).toBeCalled()
    expect(options.filter.isAuto).toBeTruthy()
    
    options._isDestroyed = true
    await JobsList.options.methods.autoFilter.call(options)
    jest.runAllTimers()
    expect(handleError).toBeCalled()
    expect(options.filter.isAuto).toBeTruthy()

    jest.clearAllTimers()

    wrapper.vm.getJobsList()
    expect(handleSuccess).toBeCalled()
    expect(wrapper.vm.jobsList).toEqual([{id: 'job1'}])
    expect(wrapper.vm.jobTotal).toEqual(1)

    await wrapper.setData({ selectedJob: {id: 'job1', info: {}}, multipleSelection: [{id: 'job1'}] })
    // await wrapper.update()
    await wrapper.vm.getJobsList()
    expect(handleSuccess).toBeCalled()
    expect(wrapper.vm.selectedJob.details).toEqual({id: 'job1', info: {}})
    expect(wrapper.vm.$refs.jobsTable.toggleRowSelection).toBeCalled()

    wrapper.vm.$store._actions.LOAD_JOBS_LIST = [jest.fn().mockImplementation(() => {
      return {
        then: (callback, errorCallback) => {
        callback({total_size: 0, value: []})
        errorCallback()
        }
      }
    })]
    await wrapper.vm.$nextTick()
    await wrapper.vm.getJobsList()
    expect(handleSuccess).toBeCalled()
    expect(wrapper.vm.jobsList).toEqual([])
    expect(wrapper.vm.jobTotal).toEqual(0)

    wrapper.vm.animatedNum(0, 10)
    expect(wrapper.vm.selectedNumber).toEqual('10')

    wrapper.vm.reCallPolling()
    expect(wrapper.vm.isPausePolling).toBeFalsy()

    wrapper.vm.handleSelectionChange([{id: 'job1'}])
    expect(wrapper.vm.isPausePolling).toBeTruthy()
    expect(wrapper.vm.multipleSelection).toEqual([{id: 'job1'}])
    expect(wrapper.vm.idsArr).toEqual(['job1'])
    wrapper.vm.handleSelectionChange([])
    expect(wrapper.vm.isPausePolling).toBeFalsy()
    expect(wrapper.vm.multipleSelection).toEqual([])
    expect(wrapper.vm.idsArr).toEqual([])
    expect(wrapper.vm.batchBtnsEnabled).toEqual({resume: false, discard: false, pause: false, drop: false})

    await wrapper.setData({ jobTotal: 20, filter: {page_size: 10, key: '', status: ['RUNNING'], job_names: []}, isSelectAllShow: false, jobsList: [{id: 'job1'}]})
    // await wrapper.update()
    wrapper.vm.handleSelectAll()
    expect(wrapper.vm.isSelectAllShow).toBeTruthy()
    expect(wrapper.vm.isSelectAll).toBeFalsy()
    expect(wrapper.vm.selectedNumber).toEqual('0')

    await wrapper.setData({ jobTotal: 20, filter: {page_size: 10, key: '', status: ['RUNNING'], job_names: []}, multipleSelection: [{id: 'job1'}]})
    // await wrapper.update()
    wrapper.vm.handleSelect()
    expect(wrapper.vm.isSelectAllShow).toBeFalsy()
    await wrapper.setData({ jobTotal: 20, filter: {page_size: 1, key: '', status: ['RUNNING'], job_names: []}, multipleSelection: [{id: 'job1'}]})
    // await wrapper.update()
    wrapper.vm.handleSelect()
    expect(wrapper.vm.isSelectAllShow).toBeTruthy()

    await wrapper.setData({ jobTotal: 20, idsArr: ['job1'], jobsList: [{id: 'job1'}]})
    // await wrapper.update()
    wrapper.vm.selectAll()
    expect(wrapper.vm.idsArrCopy).toEqual(['job1'])
    expect(wrapper.vm.idsArr).toEqual([])
    expect(wrapper.vm.selectedNumber).toEqual('1')

    await wrapper.setData({ jobTotal: 20, idsArr: ['job1'], jobsList: [{id: 'job1'}] })
    // await wrapper.update()
    wrapper.vm.selectAllChange(1)
    expect(wrapper.vm.idsArrCopy).toEqual(['job1'])
    expect(wrapper.vm.idsArr).toEqual([])
    expect(wrapper.vm.selectedNumber).toEqual('1')

    await wrapper.setData({ idsArrCopy: ['job1'], jobsList: [{id: 'job1'}] })
    // await wrapper.update()
    wrapper.vm.cancelSelectAll()
    expect(wrapper.vm.idsArr).toEqual(['job1'])
    expect(wrapper.vm.selectedNumber).toEqual('20')

    await wrapper.setData({ idsArrCopy: ['job1'], jobsList: [{id: 'job1'}] })
    // await wrapper.update()
    wrapper.vm.selectAllChange()
    expect(wrapper.vm.idsArr).toEqual(['job1'])
    expect(wrapper.vm.selectedNumber).toEqual('20')

    await wrapper.setData({ multipleSelection: [{id: 'job1'}] })
    // await wrapper.update()
    const ids = wrapper.vm.getJobIds()
    expect(ids).toEqual(['job1'])

    await wrapper.setData({ multipleSelection: [{name: 'jobName'}] })
    // await wrapper.update()
    const jobNames = wrapper.vm.getJobNames()
    expect(jobNames).toEqual(['jobName'])

    await wrapper.setData({ batchBtnsEnabled: {resume: true, restart: true, pause: true, discard: true, drop: true}, multipleSelection: [] })
    wrapper.vm.batchResume()
    expect($message.warning).toBeCalledWith('Please select at least one job.')
    wrapper.vm.batchRestart()
    expect($message.warning).toBeCalledWith('Please select at least one job.')
    wrapper.vm.batchPause()
    expect($message.warning).toBeCalledWith('Please select at least one job.')
    wrapper.vm.batchDiscard()
    expect($message.warning).toBeCalledWith('Please select at least one job.')
    wrapper.vm.batchDrop()
    expect($message.warning).toBeCalledWith('Please select at least one job.')

    await wrapper.setData({ multipleSelection: [{id: 'job1'}], isSelectAll: true, isSelectAllShow: true })
    wrapper.vm.batchResume()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()
    wrapper.vm.batchRestart()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()
    wrapper.vm.batchPause()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()
    wrapper.vm.batchDiscard()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()
    wrapper.vm.batchDrop()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()
    

    await wrapper.setData({ multipleSelection: [{id: 'job1'}], isSelectAll: false, isSelectAllShow: false })
    wrapper.vm.batchResume()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()
    wrapper.vm.batchRestart()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()
    wrapper.vm.batchPause()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()
    wrapper.vm.batchDiscard()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()
    wrapper.vm.batchDrop()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()


    await wrapper.setData({isSelectAll: false})
    await wrapper.vm.$nextTick()

    // wrapper.vm.$refs = {
    //   jobsTable: {
    //     clearSelection: jest.fn().mockResolvedValue(true)
    //   }
    // }
    wrapper.vm.resetSelection()
    expect(wrapper.vm.isSelectAllShow).toBeFalsy()
    expect(wrapper.vm.isSelectAll).toBeFalsy()
    expect(wrapper.vm.multipleSelection).toEqual([])
    expect(wrapper.vm.$refs.jobsTable.clearSelection).toBeCalled()
    expect(wrapper.vm.idsArrCopy).toEqual([])
    expect(wrapper.vm.idsArr).toEqual([])

    await wrapper.setData({ showStep: true })
    // await wrapper.update()
    wrapper.vm.currentChange(5, 10)
    expect(wrapper.vm.filter.page_offset).toEqual(5)
    expect(wrapper.vm.filter.page_size).toEqual(10)
    expect(wrapper.vm.showStep).toBeFalsy()

    await wrapper.setData({ showStep: true })
    // await wrapper.update()
    wrapper.vm.closeIt()
    expect(wrapper.vm.showStep).toBeFalsy()

    wrapper.vm.filterChange()
    expect(wrapper.vm.searchLoading).toBeFalsy()
    expect(wrapper.vm.filter.page_offset).toEqual(0)
    expect(wrapper.vm.showStep).toBeFalsy()

    await wrapper.setData({ showStep: true, selectedJob: {id: 'job1'} })
    // await wrapper.update()
    const calssName = wrapper.vm.tableRowClassName({row: {id: 'job1'}})
    expect(calssName).toEqual('current-row2')

    wrapper.vm.$store.state.project.isAllProject = true
    await wrapper.vm.$nextTick()
    wrapper.vm.loadList()
    expect(wrapper.vm.filter.project).toBeUndefined()
    wrapper.vm.$store.state.project.isAllProject = false
    await wrapper.vm.$nextTick()
    wrapper.vm.loadList()
    expect(wrapper.vm.filter.project).toEqual('learn_kylin')

    wrapper.vm.manualRefreshJobs()
    expect(wrapper.vm.filter.isAuto).toBeFalsy()

    wrapper.vm.$store.state.project.isAllProject = true
    await wrapper.setData({ isPausePolling: false })
    // await wrapper.update()
    wrapper.vm.refreshJobs()
    expect(wrapper.vm.filter.project).toBeUndefined()

    await wrapper.setData({isPausePolling: true})
    expect(wrapper.vm.refreshJobs().toString()).toEqual('[object Promise]')

    wrapper.vm.sortJobList({ column: {name: 'jobName'}, prop: 'jobName', order: 'ascending' })
    expect(wrapper.vm.filter.reverse).toBeFalsy()
    expect(wrapper.vm.filter.sort_by).toEqual('jobName')
    expect(wrapper.vm.filter.page_offset).toEqual(0)
    
    wrapper.vm.sortJobList({ column: {name: 'jobName'}, prop: 'jobName', order: 'descending' })
    expect(wrapper.vm.filter.reverse).toBeTruthy()
    expect(wrapper.vm.filter.sort_by).toEqual('jobName')
    expect(wrapper.vm.filter.page_offset).toEqual(0)

    wrapper.vm.callGlobalDetail([{id: 'job1'}], 'msg', 'title', 'submitText', false)
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()

    wrapper.vm.callGlobalDetail = mockCallGlobalDetail
    wrapper.vm.$store.state.project.isAllProject = true
    await wrapper.vm.$nextTick()
    await wrapper.vm.resume(['job1'], 'learn_kylin', 'batch', [{id: 'job1'}], [])
    expect(mockCallGlobalDetail.mock.calls[0]).toEqual([[[{"id": "job1"}]], "Are you sure you want to resume 1 job record(s)?", "Resume Job", "tip", "Resume"])
    expect(resumeJob.mock.calls[1][1]).toEqual({job_ids: ['job1'], action: 'RESUME'})

    await wrapper.vm.resume('job1', 'learn_kylin', 'batchAll', '', [])
    expect(mockCallGlobalDetail.mock.calls[1]).toEqual([[], "Are you sure you want to resume 20 job record(s)?", "Resume Job", "tip", "Resume"])
    expect(resumeJob.mock.calls[1][1]).toEqual({job_ids: ['job1'], action: 'RESUME'})

    await wrapper.vm.restart(['job1'], 'learn_kylin', 'batch', [{id: 'job1'}], [])
    expect(mockCallGlobalDetail.mock.calls[2]).toEqual([[[{"id": "job1"}]], "Are you sure you want to restart 1 job record(s)?", "Restart Job", "tip", "Restart"])
    expect(restartJob.mock.calls[1][1]).toEqual({"action": "RESTART", "job_ids": ["job1"]})

    await wrapper.vm.restart('job1', 'learn_kylin', 'batchAll', '', [])
    expect(mockCallGlobalDetail.mock.calls[3]).toEqual([[], "Are you sure you want to restart 20 job record(s)?", "Restart Job", "tip", "Restart"])
    expect(restartJob.mock.calls[1][1]).toEqual({"action": "RESTART", "job_ids": ["job1"]})

    await wrapper.vm.pause(['job1'], 'learn_kylin', 'batch', [{id: 'job1'}], [])
    expect(mockCallGlobalDetail.mock.calls[4]).toEqual([[[{"id": "job1"}]], "Are you sure you want to pause 1 job record(s)?", "Pause Job", "tip", "Pause"])
    expect(pauseJob.mock.calls[1][1]).toEqual({job_ids: ['job1'], action: 'PAUSE'})

    await wrapper.vm.pause(['job1'], 'learn_kylin', 'batchAll', '', [])
    expect(mockCallGlobalDetail.mock.calls[5]).toEqual([[], "Are you sure you want to pause 20 job record(s)?", "Pause Job", "tip", "Pause"])
    expect(pauseJob.mock.calls[1][1]).toEqual({job_ids: ['job1'], action: 'PAUSE'})

    await wrapper.vm.discard(['job1'], 'learn_kylin', 'batch', [{id: 'job1'}], [])
    expect(mockCallGlobalDetail.mock.calls[6]).toEqual([[[{"id": "job1"}]], "Are you sure you want to discard the following job(s)? Discarding the highlighted job(s) might result in gaps between segments. The query results would be empty for those data ranges. Please note that the discarded jobs couldn’t be recovered.", "Discard Job", "warning", "Discard", true])
    expect(discardJob.mock.calls[1][1]).toEqual({"action": "DISCARD", "job_ids": ["job1"]})

    await wrapper.vm.discard('job1', 'learn_kylin', 'batchAll', '', [])
    expect(mockCallGlobalDetail.mock.calls[7]).toEqual([[], "Are you sure you want to discard the following job(s)? Please note that it couldn't be recovered.", "Discard Job", "warning", "Discard", true])
    expect(discardJob.mock.calls[1][1]).toEqual({"action": "DISCARD", "job_ids": ["job1"]})
    expect(wrapper.vm.filter.status).toEqual([])

    await wrapper.vm.drop(['job1'], 'learn_kylin', 'batch', [{id: 'job1'}], [])
    expect(mockCallGlobalDetail.mock.calls[8]).toEqual([[[{"id": "job1"}]], "Are you sure you want to delete 1 job record(s)?", "Delete Job", "warning", "Delete"])
    expect(removeJobForAll.mock.calls[1][1]).toEqual({job_ids: ['job1']})

    await wrapper.vm.drop('job1', 'learn_kylin', 'batchAll', '', [])
    expect(mockCallGlobalDetail.mock.calls[9]).toEqual([[], "Are you sure you want to delete 20 job record(s)?", "Delete Job", "warning", "Delete"])
    expect(removeJobForAll.mock.calls[1][1]).toEqual({job_ids: ['job1']})

    await wrapper.setData({ selectedJob: {id: 'job1'}, showStep: false })
    // await wrapper.update()
    wrapper.vm.showLineSteps({id: 'job1'}, {property: 'icon'})
    expect(wrapper.vm.showStep).toBeTruthy()
    expect(wrapper.vm.selectedJob).toEqual({id: 'job1'})
    expect(getJobDetail).toBeCalled()

    wrapper.vm.showLineSteps({id: 'job2'}, {property: 'icon'})
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.showStep).toBeTruthy()
    expect(wrapper.vm.selectedJob).toEqual({"details": {"id": "job1", "info": {}}, "id": "job2"})
    expect(getJobDetail).toBeCalled()
    expect(wrapper.vm.selectedJob.details).toEqual({"id": "job1", "info": {}})
    expect(handleError).toBeCalled()

    wrapper.vm.clickFile({id: 'step1'})
    expect(wrapper.vm.stepAttrToShow).toEqual('output')
    expect(wrapper.vm.dialogVisible).toBeTruthy()
    expect(wrapper.vm.outputDetail).toEqual('Loading ... ')
    expect(loadStepOutputs).toBeCalled()

    wrapper.vm.$store._actions.LOAD_STEP_OUTPUTS = [jest.fn().mockRejectedValue(false)]
    await wrapper.vm.$nextTick()
    await wrapper.vm.clickFile({id: 'step1'})
    expect(wrapper.vm.stepAttrToShow).toEqual('output')
    expect(wrapper.vm.dialogVisible).toBeTruthy()
    expect(wrapper.vm.stepId).toBe('step1')
    expect(wrapper.vm.outputDetail).toEqual('cmd_output')

    wrapper.vm.filterContent(['PENDING'], 'status')
    expect(wrapper.vm.filterTags).toEqual([{key: 'status', label: 'PENDING', source: 'ProgressStatus'}])
    expect(wrapper.vm.filter.status).toEqual(['PENDING'])
    expect(wrapper.vm.filter.page_offset).toEqual(0)

    wrapper.vm.handleClose({key: 'status', label: 'PENDING'})
    expect(wrapper.vm.filter.status).toEqual([])
    expect(wrapper.vm.filter.page_offset).toEqual(0)

    wrapper.vm.showDiagnosisDetail('job1')
    expect(wrapper.vm.diagnosticId).toEqual('job1')
    expect(wrapper.vm.showDiagnostic).toBeTruthy()

    wrapper.destroy()
    expect(setTimeout).toBeCalled()
  })
})