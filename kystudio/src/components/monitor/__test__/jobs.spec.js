import { shallow } from 'vue-test-utils'
import { localVue } from '../../../../test/common/spec_common'
import Vuex from 'vuex'
import * as business from '../../../util/business'
import JobsList from '../jobs.vue'
import JobDialog from '../job_dialog.vue'
import Diagnostic from '../../admin/Diagnostic/store.js'

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
    then: (callback) => {
    callback({id: 'job1', info: {}})
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

let handleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback) => {
  callback(res)
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
const $message = {
  success: jest.fn(),
  warning: jest.fn(),
  error: jest.fn()
}
const $alert = jest.fn()

const mockPostCloudUrlMessage = jest.spyOn(business, 'postCloudUrlMessage').mockImplementation()

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

const modelJob = shallow(JobDialog, { localVue, store, propsData: {stepDetail: {}, stepId: 'stepId1', jobId: 'jobId1', targetProject: 'learn_kylin'} })
const modelDiagnostic = shallow(Diagnostic, { localVue, store, propsData: {jobId: 'jobId1'}, mocks: {$route: {name: 'Job'}} })
const mockGlobalConfirm = jest.fn().mockResolvedValue('')

const mockPush = jest.fn().mockImplementation((item) => {
  wrapper.vm.$route = item
})

const mockCallGlobalDetail = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})

const wrapper = shallow(JobsList, {
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
    postCloudUrlMessage: mockPostCloudUrlMessage
  },
  components: {
    JobDialog: modelJob.vm,
    Diagnostic: modelDiagnostic.vm
  }
})

jest.useFakeTimers()

describe('Component Monitor', () => {
  it('computed events', async () => {
    expect(wrapper.vm.emptyText).toEqual('No data')
    wrapper.setData({ filter: {key: 'job1', job_names: [], status: []} })
    await wrapper.update()
    expect(wrapper.vm.emptyText).toEqual('No Results')

    expect(wrapper.vm.isShowAdminTips).toBeTruthy()

    wrapper.setData({ selectedJob: {job_status: 'PENDING'} })
    await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('gray')
    wrapper.setData({ selectedJob: {job_status: 'RUNNING'} })
    await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('')
    wrapper.setData({ selectedJob: {job_status: 'FINISHED'} })
    await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('success')
    wrapper.setData({ selectedJob: {job_status: 'ERROR'} })
    await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('danger')
    wrapper.setData({ selectedJob: {job_status: 'DISCARDED'} })
    await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('info')
    wrapper.setData({ selectedJob: {job_status: 'STOPPED'} })
    await wrapper.update()
    expect(wrapper.vm.getJobStatusTag).toEqual('')

    wrapper.setData({ filter: {project: 'learn_kylin', job_names: [], status: []} })
    wrapper.vm.$store.state.project.isAllProject = true
    await wrapper.update()
    expect(wrapper.vm.filter.project).toBeUndefined()
  })
  it('methods events', async () => {
    wrapper.vm.closeTips()
    expect(wrapper.vm.$store.state.user.isShowAdminTips).toBeFalsy()
    expect(localStorage.getItem('isHideAdminTips')).toBeTruthy()

    wrapper.setData({ waittingJobModels: {data: {uuid: {model_alias: 'model_alias'}}} })
    await wrapper.update()
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

    wrapper.vm.$store.state.project.isAllProject = true
    await wrapper.update()
    wrapper.vm.gotoModelList({ project: 'learn_kylin', target_subject: 'target_subject'})
    expect(clearTimeout).toBeCalled()
    expect(wrapper.vm.isPausePolling).toBeTruthy()
    expect(setProject).toBeCalled()
    expect(wrapper.vm.$route.name).toEqual('ModelList')

    wrapper.setData({ filter: {page_offset: 1, job_names: ['job_names'], status: ['status']}, filterTags: ['filterTags'] })
    await wrapper.update()
    wrapper.vm.handleClearAllTags()
    expect(wrapper.vm.filter.page_offset).toEqual(0)
    expect(wrapper.vm.filter.job_names).toEqual([])
    expect(wrapper.vm.filter.status).toEqual([])
    expect(wrapper.vm.filterTags).toEqual([])

    wrapper.vm.autoFilter()
    expect(clearTimeout).toBeCalled()
    expect(setTimeout).toBeCalled()

    wrapper.vm.getJobsList()
    expect(handleSuccess).toBeCalled()
    expect(wrapper.vm.jobsList).toEqual([{id: 'job1'}])
    expect(wrapper.vm.jobTotal).toEqual(1)
    wrapper.vm.$refs = {
      jobsTable: {
        toggleRowSelection: jest.fn()
      }
    }
    wrapper.setData({ selectedJob: {id: 'job1', info: {}}, multipleSelection: [{id: 'job1'}] })
    await wrapper.update()
    await wrapper.vm.getJobsList()
    expect(handleSuccess).toBeCalled()
    expect(wrapper.vm.selectedJob.details).toEqual({id: 'job1', info: {}})
    expect(wrapper.vm.$refs.jobsTable.toggleRowSelection).toBeCalled()

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

    wrapper.setData({ jobTotal: 20, filter: {page_size: 10, status: ['RUNNING'], job_names: []}, isSelectAllShow: false, jobsList: [{id: 'job1'}]})
    await wrapper.update()
    wrapper.vm.handleSelectAll()
    expect(wrapper.vm.isSelectAllShow).toBeTruthy()
    expect(wrapper.vm.isSelectAll).toBeFalsy()
    expect(wrapper.vm.selectedNumber).toEqual('0')

    wrapper.setData({ jobTotal: 20, filter: {page_size: 10, status: ['RUNNING'], job_names: []}, multipleSelection: [{id: 'job1'}]})
    await wrapper.update()
    wrapper.vm.handleSelect()
    expect(wrapper.vm.isSelectAllShow).toBeFalsy()
    wrapper.setData({ jobTotal: 20, filter: {page_size: 1, status: ['RUNNING'], job_names: []}, multipleSelection: [{id: 'job1'}]})
    await wrapper.update()
    wrapper.vm.handleSelect()
    expect(wrapper.vm.isSelectAllShow).toBeTruthy()

    wrapper.setData({ jobTotal: 20, idsArr: ['job1'], jobsList: [{id: 'job1'}]})
    await wrapper.update()
    wrapper.vm.selectAll()
    expect(wrapper.vm.idsArrCopy).toEqual(['job1'])
    expect(wrapper.vm.idsArr).toEqual([])
    expect(wrapper.vm.selectedNumber).toEqual('1')

    wrapper.setData({ jobTotal: 20, idsArr: ['job1'], jobsList: [{id: 'job1'}] })
    await wrapper.update()
    wrapper.vm.selectAllChange(1)
    expect(wrapper.vm.idsArrCopy).toEqual(['job1'])
    expect(wrapper.vm.idsArr).toEqual([])
    expect(wrapper.vm.selectedNumber).toEqual('1')

    wrapper.setData({ idsArrCopy: ['job1'], jobsList: [{id: 'job1'}] })
    await wrapper.update()
    wrapper.vm.cancelSelectAll()
    expect(wrapper.vm.idsArr).toEqual(['job1'])
    expect(wrapper.vm.selectedNumber).toEqual('20')

    wrapper.setData({ idsArrCopy: ['job1'], jobsList: [{id: 'job1'}] })
    await wrapper.update()
    wrapper.vm.selectAllChange()
    expect(wrapper.vm.idsArr).toEqual(['job1'])
    expect(wrapper.vm.selectedNumber).toEqual('20')

    wrapper.setData({ multipleSelection: [{id: 'job1'}] })
    await wrapper.update()
    const ids = wrapper.vm.getJobIds()
    expect(ids).toEqual(['job1'])

    wrapper.setData({ multipleSelection: [{name: 'jobName'}] })
    await wrapper.update()
    const jobNames = wrapper.vm.getJobNames()
    expect(jobNames).toEqual(['jobName'])

    wrapper.setData({ batchBtnsEnabled: {resume: true, restart: false, pause: false, discard: false, drop: false}, multipleSelection: [] })
    wrapper.vm.batchResume()
    expect($message.warning).toBeCalled()
    wrapper.setData({ batchBtnsEnabled: {resume: true, restart: false, pause: false, discard: false, drop: false}, multipleSelection: [{id: 'job1'}] })
    wrapper.vm.batchResume()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()
    // expect(resumeJob).toBeCalledWith({job_ids: ['job1'], project: 'learn_kylin', action: 'RESUME'})

    wrapper.setData({ batchBtnsEnabled: {resume: false, restart: true, pause: false, discard: false, drop: false}, multipleSelection: [] })
    wrapper.vm.batchRestart()
    expect($message.warning).toBeCalled()
    wrapper.setData({ batchBtnsEnabled: {resume: true, restart: true, pause: false, discard: false, drop: false}, multipleSelection: [{id: 'job1'}] })
    wrapper.vm.batchRestart()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()

    wrapper.setData({ batchBtnsEnabled: {resume: false, restart: false, pause: true, discard: false, drop: false}, multipleSelection: [] })
    wrapper.vm.batchPause()
    expect($message.warning).toBeCalled()
    wrapper.setData({ batchBtnsEnabled: {resume: true, restart: false, pause: true, discard: false, drop: false}, multipleSelection: [{id: 'job1'}] })
    wrapper.vm.batchPause()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()

    wrapper.setData({ batchBtnsEnabled: {resume: false, restart: false, pause: false, discard: true, drop: false}, multipleSelection: [] })
    wrapper.vm.batchDiscard()
    expect($message.warning).toBeCalled()
    wrapper.setData({ batchBtnsEnabled: {resume: true, restart: false, pause: false, discard: true, drop: false}, multipleSelection: [{id: 'job1'}] })
    wrapper.vm.batchDiscard()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()

    wrapper.setData({ batchBtnsEnabled: {resume: false, restart: false, pause: false, discard: false, drop: true}, multipleSelection: [] })
    wrapper.vm.batchDrop()
    expect($message.warning).toBeCalled()
    wrapper.setData({ batchBtnsEnabled: {resume: true, restart: false, pause: false, discard: false, drop: true}, multipleSelection: [{id: 'job1'}] })
    wrapper.vm.batchDrop()
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()

    wrapper.vm.$refs = {
      jobsTable: {
        clearSelection: jest.fn()
      }
    }
    wrapper.vm.resetSelection()
    expect(wrapper.vm.isSelectAllShow).toBeFalsy()
    expect(wrapper.vm.isSelectAll).toBeFalsy()
    expect(wrapper.vm.multipleSelection).toEqual([])
    expect(wrapper.vm.$refs.jobsTable.clearSelection).toBeCalled()
    expect(wrapper.vm.idsArrCopy).toEqual([])
    expect(wrapper.vm.idsArr).toEqual([])

    wrapper.setData({ showStep: true })
    await wrapper.update()
    wrapper.vm.currentChange(5, 10)
    expect(wrapper.vm.filter.page_offset).toEqual(5)
    expect(wrapper.vm.filter.page_size).toEqual(10)
    expect(wrapper.vm.showStep).toBeFalsy()

    wrapper.setData({ showStep: true })
    await wrapper.update()
    wrapper.vm.closeIt()
    expect(wrapper.vm.showStep).toBeFalsy()

    wrapper.vm.filterChange()
    expect(wrapper.vm.searchLoading).toBeFalsy()
    expect(wrapper.vm.filter.page_offset).toEqual(0)
    expect(wrapper.vm.showStep).toBeFalsy()

    wrapper.setData({ showStep: true, selectedJob: {id: 'job1'} })
    await wrapper.update()
    const calssName = wrapper.vm.tableRowClassName({row: {id: 'job1'}})
    expect(calssName).toEqual('current-row2')

    wrapper.vm.$store.state.project.isAllProject = true
    await wrapper.update()
    wrapper.vm.loadList()
    expect(wrapper.vm.filter.project).toBeUndefined()
    wrapper.vm.$store.state.project.isAllProject = false
    await wrapper.update()
    wrapper.vm.loadList()
    expect(wrapper.vm.filter.project).toEqual('learn_kylin')

    wrapper.vm.manualRefreshJobs()
    expect(wrapper.vm.filter.isAuto).toBeFalsy()

    wrapper.vm.$store.state.project.isAllProject = true
    wrapper.setData({ isPausePolling: false })
    await wrapper.update()
    wrapper.vm.refreshJobs()
    expect(wrapper.vm.filter.project).toBeUndefined()

    wrapper.vm.sortJobList({ column: {name: 'jobName'}, prop: 'jobName', order: 'ascending' })
    expect(wrapper.vm.filter.reverse).toBeFalsy()
    expect(wrapper.vm.filter.sort_by).toEqual('jobName')
    expect(wrapper.vm.filter.page_offset).toEqual(0)

    wrapper.vm.callGlobalDetail([{id: 'job1'}], 'msg', 'title', 'submitText', false)
    expect(mockApi.mockCallGlobalDetailDialog).toBeCalled()

    wrapper.vm.callGlobalDetail = mockCallGlobalDetail
    wrapper.vm.$store.state.project.isAllProject = false
    await wrapper.update()
    wrapper.vm.resume(['job1'], 'learn_kylin', 'batch', [{id: 'job1'}], [])
    expect(mockCallGlobalDetail).toBeCalled()
    expect(resumeJob.mock.calls[0][1]).toEqual({job_ids: ['job1'], action: 'RESUME'})

    wrapper.vm.restart(['job1'], 'learn_kylin', 'batch', [{id: 'job1'}], [])
    expect(mockCallGlobalDetail).toBeCalled()
    expect(restartJob.mock.calls[0][1]).toEqual({job_ids: ['job1'], action: 'RESTART'})

    wrapper.vm.pause(['job1'], 'learn_kylin', 'batch', [{id: 'job1'}], [])
    expect(mockCallGlobalDetail).toBeCalled()
    expect(pauseJob.mock.calls[0][1]).toEqual({job_ids: ['job1'], action: 'PAUSE'})

    wrapper.vm.discard(['job1'], 'learn_kylin', 'batch', [{id: 'job1'}], [])
    expect(mockCallGlobalDetail).toBeCalled()
    expect(discardJob.mock.calls[0][1]).toEqual({job_ids: ['job1'], action: 'DISCARD'})

    wrapper.vm.drop(['job1'], 'learn_kylin', 'batch', [{id: 'job1'}], [])
    expect(mockCallGlobalDetail).toBeCalled()
    expect(removeJobForAll.mock.calls[0][1]).toEqual({job_ids: ['job1']})

    wrapper.setData({ selectedJob: {id: 'job1'}, showStep: false })
    await wrapper.update()
    wrapper.vm.showLineSteps({id: 'job1'}, {property: 'icon'})
    expect(wrapper.vm.showStep).toBeTruthy()
    expect(wrapper.vm.selectedJob).toEqual({id: 'job1'})
    expect(getJobDetail).toBeCalled()

    wrapper.vm.clickFile({id: 'step1'})
    expect(wrapper.vm.stepAttrToShow).toEqual('output')
    expect(wrapper.vm.dialogVisible).toBeTruthy()
    expect(wrapper.vm.outputDetail).toEqual('Loading ... ')
    expect(loadStepOutputs).toBeCalled()

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