import Vuex from 'vuex'
import { mount } from 'vue-test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import Diagnostic from '../../Diagnostic/index.vue'
import KyligenceUI from 'kyligence-ui'

localVue.use(KyligenceUI)

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
        running: false
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
      name: ''
    }
  },
  data: {
    isRunning: true
  }
})

describe('Component Diagnostic', () => {
  it('init', () => {
    expect(mockApis.getServers).toBeCalled()
    expect(wrapper.vm.$data.serverOptions).toEqual([{"label": "sandbox.com(ALL)", "value": "sandbox.com"}])
    expect(wrapper.vm.$data.servers).toEqual(["sandbox.com"])
    expect(mockApis.stopInterfaceCall).toBeCalled()
  })
  it('computed', () => {
    expect(wrapper.vm.timeRange).toEqual([{"label": "lastHour", "text": "Last one hour"}, {"label": "lastDay", "text": "Last one day"}, {"label": "lastThreeDay", "text": "Last three days"}, {"label": "lastMonth", "text": "Last one month"}, {"label": "custom", "text": "Customize"}])
    expect(wrapper.vm.getDownloadNum).toBe('1/1')
    expect(wrapper.vm.getDateTimeValid).toBeFalsy()
    expect(wrapper.vm.showManualDownloadLayout).toBeTruthy()
    expect(wrapper.vm.getTitle({host: 'http://sandbox.com'})).toBe('http://sandbox.com(ALL)')
    expect(wrapper.vm.getTimes()).not.toBeNaN()
    expect(wrapper.vm.getTimes('2020-08-04')).toBe(1596499200000)
  })
  it('methods', async () => {
    expect(wrapper.vm.setProgressColor({progress: 0, status: '000'})).toEqual({"color": "#0988DE"})
    expect(wrapper.vm.setProgressColor({progress: 0.5, status: '000'})).toEqual({"color": "#0988DE"})
    expect(wrapper.vm.setProgressColor({progress: 1, status: '000'})).toEqual({"status": "success"})
    expect(wrapper.vm.setProgressColor({progress: 1, status: '999'})).toEqual({"status": "exception"})
    wrapper.vm.onBlur()
    expect(wrapper.vm.$data.validDateTime).toBeTruthy()
    wrapper.vm.changeTimeRange('lastHour')
    expect(wrapper.vm.$data.dateTime.prev).not.toBe('')
    expect(wrapper.vm.$data.dateTime.next).not.toBe('')
    wrapper.vm.changeTimeRange('lastThreeDay')
    expect(wrapper.vm.$data.dateTime.prev).not.toBe('')
    expect(wrapper.vm.$data.dateTime.next).not.toBe('')
    wrapper.vm.changeTimeRange('lastMonth')
    expect(wrapper.vm.$data.dateTime.prev).not.toBe('')
    expect(wrapper.vm.$data.dateTime.next).not.toBe('')
    wrapper.vm.changeTimeRange('custom')
    expect(wrapper.vm.$data.dateTime.prev).toBe('')
    expect(wrapper.vm.$data.dateTime.next).not.toBe('')
    wrapper.vm.changeTimeRange('')
    expect(wrapper.vm.$data.dateTime.prev).toBe('')
    expect(wrapper.vm.$data.dateTime.next).toBe('')

    wrapper.vm.handleClose('header')
    // wrapper.vm.$nextTick(() => {
    expect(wrapper.vm.$data.showPopoverTip).toBeTruthy()
    expect(wrapper.vm.$data.closeFromHeader).toBeTruthy()
    // })
    wrapper.setData({ isRunning: false })
    await wrapper.vm.$nextTick()
    wrapper.vm.handleClose('')
    // wrapper.vm.$nextTick(() => {
    expect(wrapper.vm.$data.showPopoverTip).toBeTruthy()
    expect(wrapper.vm.$data.closeFromHeader).toBeTruthy()
    expect(mockApis.removeDiagnosticTask).toBeCalled()
    expect(mockApis.resetDumpData).toBeCalled()
    expect(mockApis.stopInterfaceCall).toBeCalled()
    expect(wrapper.emitted().close).toEqual([[]])
    // })

    // wrapper.vm.generateDiagnostic()
    wrapper.setData({dateTime: {prev: new Date(), next: ''}})
    await wrapper.vm.$nextTick()
    Diagnostic.options.methods.generateDiagnostic.call(wrapper.vm)
    // wrapper.vm.generateDiagnostic()
    expect(mockApis.resetDumpData).toBeCalled()
    // expect(mockApis.getDumpRemote).toBeCalled()
    expect(wrapper.vm.$data.isRunning).toBeFalsy()

    wrapper.vm.retryJob({id: 'front_2020_08_04_07_35_49_494298', host: 'sandbox.com'})
    expect(mockApis.delDumpIdList).toBeCalled()
    expect(mockApis.getDumpRemote).toBeCalled()

    wrapper.vm.changeCheckAllType(false)
    expect(wrapper.vm.$data.indeterminate).toBeFalsy()
    expect(mockApis.updateCheckType).toBeCalled()

    wrapper.vm.cancelManualDownload()
    expect(wrapper.vm.$data.isManualDownload).toBeFalsy()
    expect(wrapper.vm.$data.checkAll).toBeFalsy()
    expect(wrapper.vm.$data.indeterminate).toBeFalsy()
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
    expect(wrapper.vm.$data.indeterminate).toBeFalsy()
    expect(wrapper.vm.$data.checkAll).toBeTruthy()

    wrapper.vm.downloadEvent()
    expect(mockApis.downloadDump).toBeCalled()
  })
})
