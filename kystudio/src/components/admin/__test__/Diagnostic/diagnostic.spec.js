import Vuex from 'vuex'
import { mount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import Diagnostic from '../../Diagnostic/index.vue'
import * as util from '../../../../util'
import * as business from '../../../../util/business'

// const mockGetQueryString = jest.spyOn(util, 'getQueryString').mockImplementation(() => 'iframe')
const mockPostCloudUrlMessage = jest.spyOn(business, 'postCloudUrlMessage').mockImplementation()

const mockApis = {
  getDumpRemote: jest.fn().mockImplementation(() => {
    return new Promise((resolve, reject) => reject(false))
  }),
  getServers: jest.fn().mockImplementation(() => {
    return new Promise((resolve) => {
      resolve([{host: 'sandbox.com', mode: 'ALL'}])
    })
  }),
  downloadDump: jest.fn(),
  removeDiagnosticTask: jest.fn(),
  updateCheckType: jest.fn(),
  delDumpIdList: jest.fn(),
  resetDumpData: jest.fn(),
  stopInterfaceCall: jest.fn()
}

const diagnosticModel = {
  namespaced: true,
  state: {
    host: 'http://kyligence',
    diagDumpIds: {
      front_2020_08_04_07_35_49_494298: {
        status: '000',
        stage: 'DONE',
        progress: 0,
        error: '',
        showErrorDetail: false,
        isCheck: true,
        running: false,
        host: 'http://sandbox.com'
      }
    }
  },
  actions: {
    'GET_DUMP_REMOTE': mockApis.getDumpRemote,
    'GET_SERVERS': mockApis.getServers,
    'DOWNLOAD_DUMP_DIAG': mockApis.downloadDump,
    'REMOVE_DIAGNOSTIC_TASK': mockApis.removeDiagnosticTask
  },
  mutations: {
    'UPDATE_CHECK_TYPE': mockApis.updateCheckType,
    'DEL_DUMP_ID_LIST': mockApis.delDumpIdList,
    'RESET_DUMP_DATA': mockApis.resetDumpData,
    'STOP_INTERFACE_CALL': mockApis.stopInterfaceCall
  }
}

const store = new Vuex.Store({
  state: {
    config: {
      platform: 'pc'
    }
  },
  modules: {
    diagnosticModel
  },
  getters: {
    isAdminRole: () => {
      return true
    }
  }
})

const wrapper = mount(Diagnostic, {
  localVue,
  store,
  mocks: {
    $route: {
      name: '',
      query: {
        previousPage: 'StreamingJob'
      }
    },
    $router: {
      push: jest.fn()
    }
  },
  data () {
    return {
      isRunning: true
    }
  }
})

describe('Component Diagnostic', () => {
  it('init', () => {
    expect(mockApis.getServers).toBeCalled()
    expect(wrapper.vm.serverOptions).toEqual([{'label': 'sandbox.com(ALL)', 'value': 'sandbox.com'}])
    expect(wrapper.vm.servers).toEqual(['sandbox.com'])
    expect(mockApis.stopInterfaceCall).toBeCalled()
  })
  it('computed', () => {
    expect(wrapper.vm.timeRange).toEqual([{'label': 'lastHour', 'text': 'Last 1 Hour'}, {'label': 'lastDay', 'text': 'Last 1 Day'}, {'label': 'lastThreeDay', 'text': 'Last 3 Days'}, {'label': 'lastMonth', 'text': 'Last 1 Month'}, {'label': 'custom', 'text': 'Customize'}])
    expect(wrapper.vm.getDownloadNum).toBe('1/1')
    expect(wrapper.vm.getDateTimeValid).toBeFalsy()
    expect(wrapper.vm.getTitle({host: 'http://sandbox.com'})).toBe('sandbox.com(ALL)')
    expect(wrapper.vm.getTimes()).not.toBeNaN()
    expect(wrapper.vm.getTimes('2020-08-04')).toBe(1596499200000)
  })
  it('methods', async () => {
    expect(wrapper.vm.setProgressColor({progress: 0, status: '000'})).toEqual({'color': '#0988DE'})
    expect(wrapper.vm.setProgressColor({progress: 0.5, status: '000'})).toEqual({'color': '#0988DE'})
    expect(wrapper.vm.setProgressColor({progress: 1, status: '000'})).toEqual({'status': 'success'})
    expect(wrapper.vm.setProgressColor({progress: 1, status: '999'})).toEqual({'status': 'exception'})
    wrapper.vm.onBlur()
    expect(wrapper.vm.validDateTime).toBeTruthy()
    wrapper.vm.changeTimeRange('lastHour')
    expect(wrapper.vm.dateTime.prev).not.toBe('')
    expect(wrapper.vm.dateTime.next).not.toBe('')
    wrapper.vm.changeTimeRange('lastThreeDay')
    expect(wrapper.vm.dateTime.prev).not.toBe('')
    expect(wrapper.vm.dateTime.next).not.toBe('')
    wrapper.vm.changeTimeRange('lastMonth')
    expect(wrapper.vm.dateTime.prev).not.toBe('')
    expect(wrapper.vm.dateTime.next).not.toBe('')
    wrapper.vm.changeTimeRange('custom')
    expect(wrapper.vm.dateTime.prev).toBe('')
    expect(wrapper.vm.dateTime.next).not.toBe('')
    wrapper.vm.changeTimeRange('')
    expect(wrapper.vm.dateTime.prev).toBe('')
    expect(wrapper.vm.dateTime.next).toBe('')

    await wrapper.setData({ isRunning: false })
    wrapper.vm.handleClose('')
    // wrapper.vm.$nextTick(() => {
    // expect(wrapper.vm.closeFromHeader).toBeTruthy()
    expect(mockApis.removeDiagnosticTask).toBeCalled()
    expect(mockApis.resetDumpData).toBeCalled()
    expect(mockApis.stopInterfaceCall).toBeCalled()
    expect(wrapper.emitted().close).toEqual([[]])
    // })

    // wrapper.vm.generateDiagnostic()
    await wrapper.setData({dateTime: {prev: new Date(), next: ''}})
    Diagnostic.options.methods.generateDiagnostic.call(wrapper.vm)
    // wrapper.vm.generateDiagnostic()
    expect(mockApis.resetDumpData).toBeCalled()
    // expect(mockApis.getDumpRemote).toBeCalled()
    expect(wrapper.vm.isRunning).toBeFalsy()

    wrapper.vm.retryJob({id: 'front_2020_08_04_07_35_49_494298', host: 'sandbox.com'})
    expect(mockApis.delDumpIdList).toBeCalled()
    expect(mockApis.getDumpRemote).toBeCalled()
    expect(wrapper.vm.isRunning).toBeFalsy()

    wrapper.vm.changeCheckAllType(false)
    expect(wrapper.vm.indeterminate).toBeFalsy()
    expect(mockApis.updateCheckType).toBeCalled()

    wrapper.vm.cancelManualDownload()
    expect(wrapper.vm.isManualDownload).toBeFalsy()
    expect(wrapper.vm.checkAll).toBeFalsy()
    expect(wrapper.vm.indeterminate).toBeFalsy()
    expect(mockApis.updateCheckType).toBeCalled()

    const option = {
      $t: jest.fn().mockImplementation(res => res),
      $message: {
        success: jest.fn(),
        error: jest.fn()
      }
    }
    Diagnostic.options.methods.onCopy.call(option)
    expect(option.$message.success).toHaveBeenCalledWith('kylinLang.common.copySuccess')
    Diagnostic.options.methods.onError.call(option)
    expect(option.$message.error).toHaveBeenCalledWith('kylinLang.common.copyfail')

    wrapper.vm.changeCheckItems()
    expect(wrapper.vm.indeterminate).toBeFalsy()
    expect(wrapper.vm.checkAll).toBeTruthy()

    wrapper.vm.downloadEvent({host: 'sandbox.com', id: 'front_2020_08_04_07_35_49_494298'})
    expect(mockApis.downloadDump).toBeCalled()

    wrapper.vm.goto('admin')
    expect(wrapper.vm.jumpPage).toBe('admin')
    expect(wrapper.emitted().close).toEqual([[], []])
    expect(wrapper.vm.$router.push).toBeCalledWith('/admin/project?previousPage=undefined')

    await wrapper.setData({isRunning: true})
    wrapper.vm.goto('job')
    expect(wrapper.vm.popoverCallback).not.toEqual()

    wrapper.vm.gotoEventCallback()
    expect(wrapper.vm.$router.push).toBeCalledWith(`/admin/project?previousPage=undefined`)
    // expect(mockPostCloudUrlMessage.mock.calls[0][1]).toEqual([])

    jest.spyOn(util, 'getQueryString').mockImplementation(() => 'pc')
    wrapper.vm.gotoEventCallback()
    expect(wrapper.vm.$router.push).toBeCalledWith('/monitor/streamingJob')

    wrapper.vm.$route.query.previousPage = 'job'
    wrapper.vm.gotoEventCallback()
    expect(wrapper.vm.$router.push).toBeCalledWith('/monitor/job')

    wrapper.vm.gotoWorkspaceList()
    expect(mockPostCloudUrlMessage.mock.calls[0][1]).toEqual({'name': 'Stack'})

    await wrapper.setData({dateTime: {prev: new Date('2021-03-01 11:13:00'), next: new Date('2021-03-01 14:41:09')}})
    wrapper.vm.$store._actions.GET_DUMP_REMOTE = [jest.fn().mockRejectedValue(false)]
    await wrapper.vm.generateDiagnostic()
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.isShowDiagnosticProcess).toBeFalsy()
    expect(wrapper.vm.isRunning).toBeFalsy()

    wrapper.vm.$route.name = 'Job'
    // await wrapper.setProps({jobId: '550fafd0-4043-43be-9033-d1d9033e430b'})
    await wrapper.vm.generateDiagnostic()
    expect(wrapper.vm.diagDumpIds).not.toEqual({})

    await wrapper.setProps({jobId: '550fafd0-4043-43be-9033-d1d9033e430b'})
    await wrapper.vm.generateDiagnostic()
    await wrapper.vm.$nextTick()
    expect(mockApis.getDumpRemote.mock.calls[2][1].job_id).toBe('550fafd0-4043-43be-9033-d1d9033e430b')
  })
})
