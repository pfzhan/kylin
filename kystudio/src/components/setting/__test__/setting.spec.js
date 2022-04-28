import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import Setting from '../setting.vue'
import kapEmptyData from 'components/common/EmptyData/EmptyData.vue'
import Vuex from 'vuex'
import * as utils from '../../../util/index'
import * as business from '../../../util/business'

jest.useFakeTimers()

const mockApi = {
  mockFetchProjectSettings: jest.fn().mockImplementation(() => {
    return Promise.resolve({
      auto_merge_enabled: true,
      auto_merge_time_ranges: ['WEEK', 'MONTH', 'QUARTER', 'YEAR'],
      converter_class_names: 'org.apache.kylin.query.util.PowerBIConverter,io.kyligence.kap.query.util.SparkSQLFunctionConverter,io.kyligence.kap.query.util.RestoreFromComputedColumn,io.kyligence.kap.query.security.RowFilter,io.kyligence.kap.query.security.HackSelectStarWithColumnACL,org.apache.kylin.source.adhocquery.HivePushDownConverter',
      data_load_empty_notification_enabled: false,
      default_database: 'DEFAULT',
      description: '',
      expose_computed_column: true,
      frequency_time_window: 'MONTH',
      job_error_notification_enabled: false,
      job_notification_emails: [],
      kerberos_project_level_enabled: false,
      low_frequency_threshold: 5,
      principal: null,
      project: 'xm_test_1',
      push_down_enabled: true,
      retention_range: {retention_range_number: 1, retention_range_enabled: false, retention_range_type: 'YEAR'},
      runner_class_name: 'io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl',
      semi_automatic_mode: true,
      storage_quota_size: 10995116277760,
      threshold: 20,
      tips_enabled: true,
      volatile_range: {volatile_range_number: 5, volatile_range_enabled: true, volatile_range_type: 'QUARTER'},
      yarn_queue: 'kylin'
    })
  }),
  mockUserAccess: jest.fn().mockResolvedValue(),
  mockSetProject: jest.fn().mockResolvedValue()
}

const mockHandleSuccessAsync = jest.spyOn(utils, 'handleSuccessAsync').mockImplementation((res) => {
  return Promise.resolve(res)
})
const mockCacheSessionStorage = jest.spyOn(utils, 'cacheSessionStorage').mockImplementation(res => res)
const mockHandleError = jest.spyOn(business, 'handleError').mockImplementation()
let mockConfirm = jest.fn().mockImplementation(() => {
  return Promise.resolve()
})
const mockRouter = jest.fn()

const route = {
  to: {
    name: 'refresh',
    params: {
      ignoreIntercept: false
    }
  },
  from: {
    name: 'GroupDetail'
  },
  next: jest.fn().mockImplementation(func => func && func(wrapper.vm))
}

const store = new Vuex.Store({
  state: {
    project: {
      isSemiAutomatic: true
    },
    config: {
      platform: 'pc'
    }
  },
  getters: {
    currentProjectData: () => {
      return {
        name: 'xm_test_1',
        uuid: 'c47daf3b-816b-4965-94de-c6f3262874d2',
        default_database: 'DEFAULT',
        owner: 'ADMIN'
      }
    },
    isAutoProject: () => {
      return true
    }
  },
  actions: {
    'FETCH_PROJECT_SETTINGS': mockApi.mockFetchProjectSettings,
    'USER_ACCESS': mockApi.mockUserAccess
  },
  mutations: {
    'SET_PROJECT': mockApi.mockSetProject
  }
})

const wrapper = shallowMount(Setting, {
  localVue,
  store,
  mocks: {
    handleSuccessAsync: mockHandleSuccessAsync,
    handleError: mockHandleError,
    $confirm: mockConfirm,
    cacheSessionStorage: mockCacheSessionStorage,
    $router: {
      replace: mockRouter
    }
  },
  components: {
    kapEmptyData
  }
})

global.window.kapVm = wrapper.vm

describe('Component Setting', () => {
  it('init', async () => {
    expect(mockApi.mockFetchProjectSettings.mock.calls[0][1]).toEqual({'projectName': 'xm_test_1'})
    await wrapper.vm.getCurrentSettings()
    expect(mockApi.mockFetchProjectSettings.mock.calls[0][1]).toEqual({'projectName': 'xm_test_1'})
    expect(mockHandleSuccessAsync).toBeCalled()
    expect(wrapper.vm.$data.projectSettings).toEqual({'auto_merge_enabled': true, 'auto_merge_time_ranges': ['WEEK', 'MONTH', 'QUARTER', 'YEAR'], 'converter_class_names': 'org.apache.kylin.query.util.PowerBIConverter,io.kyligence.kap.query.util.SparkSQLFunctionConverter,io.kyligence.kap.query.util.RestoreFromComputedColumn,io.kyligence.kap.query.security.RowFilter,io.kyligence.kap.query.security.HackSelectStarWithColumnACL,org.apache.kylin.source.adhocquery.HivePushDownConverter', 'data_load_empty_notification_enabled': false, 'default_database': 'DEFAULT', 'description': '', 'expose_computed_column': true, 'frequency_time_window': 'MONTH', 'job_error_notification_enabled': false, 'job_notification_emails': [], 'kerberos_project_level_enabled': false, 'low_frequency_threshold': 5, 'principal': null, 'project': 'xm_test_1', 'push_down_enabled': true, 'retention_range': {'retention_range_enabled': false, 'retention_range_number': 1, 'retention_range_type': 'YEAR'}, 'runner_class_name': 'io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl', 'semi_automatic_mode': true, 'storage_quota_size': 10995116277760, 'storage_quota_tb_size': '10.00', 'threshold': 20, 'tips_enabled': true, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': 5, 'volatile_range_type': 'QUARTER'}, 'yarn_queue': 'kylin'})
  })
  it('computed', () => {
    expect(wrapper.vm.modelSetting).toBe('Index Group Rewrite Settings')
  })
  it('methods', async () => {
    wrapper.vm._showLoading()
    expect(wrapper.vm.$data.isLoading).toBeTruthy()
    wrapper.vm._hideLoading()
    expect(wrapper.vm.$data.isLoading).toBeFalsy()
    wrapper.vm.handleFormChanged({isAdvanceSettingChange: true})
    expect(wrapper.vm.$data.changedForm).toEqual({'isAdvanceSettingChange': true, 'isBasicSettingChange': false})

    await wrapper.vm.beforeRouteLeave(route.to, route.from, route.next)
    jest.runAllTimers()
    expect(mockConfirm).toBeCalledWith('Exit the page will lose unsaved content.', 'Notification', {'cancelButtonText': 'Cancel', 'closeOnClickModal': false, 'closeOnPressEscape': false, 'confirmButtonText': 'Exit', 'type': 'warning', 'width': '400px'})
    // expect(mockRouter).toBeCalledWith()
    expect(route.next).toBeCalled()

    route.to.name = 'Admin'
    await wrapper.vm.beforeRouteLeave(route.to, route.from, route.next)
    jest.runAllTimers()
    expect(mockConfirm).toBeCalled()
    expect(route.next).toBeCalled()

    route.to.name = 'refresh'
    await wrapper.vm.beforeRouteLeave(route.to, route.from, route.next)
    expect(wrapper.vm.viewType).toBe('basicSetting')
    expect(route.next).toBeCalledWith(false)

    route.to.params.ignoreIntercept = true
    route.to.name = 'refresh'
    await wrapper.vm.beforeRouteLeave(route.to, route.from, route.next)
    expect(route.next).toBeCalled()

    wrapper.vm.$confirm = jest.fn().mockImplementation(() => {
      return new Promise((resolve, reject) => {
        reject(false)
      })
    })

    route.to.params.ignoreIntercept = false
    route.to.name = 'refresh'
    await wrapper.vm.beforeRouteLeave(route.to, route.from, route.next)
    jest.runAllTimers()
    expect(mockConfirm).toBeCalled()
    expect(wrapper.vm.viewType).toBe('basicSetting')
    // expect(mockApi.mockSetProject).toBeCalledWith()
    // expect(mockApi.mockUserAccess).toBeCalledWith()
    expect(route.next).toBeCalled()

    route.to.name = 'Admin'
    await wrapper.vm.beforeRouteLeave(route.to, route.from, route.next)
    jest.runAllTimers()
    expect(mockConfirm).toBeCalled()
    expect(wrapper.vm.viewType).toBe('basicSetting')
    expect(route.next).toBeCalled()

    wrapper.vm.$store._actions.FETCH_PROJECT_SETTINGS = jest.fn().mockRejectedValue(false)
    await wrapper.vm.$nextTick()
    await wrapper.vm.getCurrentSettings()
    expect(mockHandleError).toBeCalled()

    jest.clearAllTimers()
    wrapper.destroy()
  })
})

