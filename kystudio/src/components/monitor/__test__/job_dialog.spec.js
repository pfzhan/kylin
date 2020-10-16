import { mount, shallow } from 'vue-test-utils'
import { localVue } from '../../../../test/common/spec_common'
import Vuex from 'vuex'
import jobDialog from '../job_dialog'

jest.useFakeTimers()

const mockApi = {
  downloadLogs: jest.fn().mockImplementation(() => {
    return Promise.resolve({
      status: 200,
      body: 'd_datekey,d_date,d_dayofwee'
    })
  })
}

const store = new Vuex.Store({
  state: {
    config: {
      platform: 'iframe'
    }
  },
  actions: {
    DOWNLOAD_LOGS: mockApi.downloadLogs
  }
})

const wrapper = mount(jobDialog, {
  localVue,
  store,
  propsData: {
    stepDetail: '下载中...',
    stepId: '6728fe21-9535-4728-9e41-1e99b7f9387e_01',
    jobId: '6728fe21-9535-4728-9e41-1e99b7f9387e',
    targetProject: 'xm_test'
  }
})

const mockSaveBlob = jest.fn()
const mockRevokeObjectURL = jest.fn().mockImplementation()
global.window.navigator.msSaveOrOpenBlob = true
global.window.URL = {
  revokeObjectURL: mockRevokeObjectURL,
  createObjectURL: jest.fn().mockImplementation(() => {
    return 'blob:1234567890'
  })
}
global.navigator.msSaveBlob = mockSaveBlob

describe('Component Job_dialog', () => {
  it('computed', () => {
    expect(wrapper.vm.actionUrl).toBe('/kylin/api/jobs/6728fe21-9535-4728-9e41-1e99b7f9387e/steps/6728fe21-9535-4728-9e41-1e99b7f9387e_01/log')
  })
  it('methods', async () => {
    await wrapper.vm.downloadLogs()
    expect(mockApi.downloadLogs.mock.calls[0][1]).toEqual({"jobId": "6728fe21-9535-4728-9e41-1e99b7f9387e", "project": "xm_test", "stepId": "6728fe21-9535-4728-9e41-1e99b7f9387e_01"})
    expect(mockSaveBlob).toBeCalled()

    global.window.navigator.msSaveOrOpenBlob = false
    await wrapper.vm.downloadLogs()
    expect(mockApi.downloadLogs.mock.calls[0][1]).toEqual({"jobId": "6728fe21-9535-4728-9e41-1e99b7f9387e", "project": "xm_test", "stepId": "6728fe21-9535-4728-9e41-1e99b7f9387e_01"})
    expect(mockRevokeObjectURL).toBeCalled()

    wrapper.vm.$el.querySelectorAll = jest.fn().mockReturnValue([])
    wrapper.vm.$store.state.config.platform = ''
    await wrapper.update()
    await wrapper.vm.downloadLogs()
    expect(wrapper.vm.hasClickDownloadLogBtn).toBeFalsy()

    wrapper.vm.$store._actions.DOWNLOAD_LOGS = [jest.fn().mockImplementation(() => {
      return new Error('error')
    })]
    await wrapper.vm.downloadLogs()
    jest.runAllTimers()
    expect(wrapper.vm.hasClickDownloadLogBtn).toBeFalsy()

    wrapper.setData({hasClickDownloadLogBtn: true})
    await wrapper.update()
    expect(await wrapper.vm.downloadLogs()).toBeFalsy()
  })
})
