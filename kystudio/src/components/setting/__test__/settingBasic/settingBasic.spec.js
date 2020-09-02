import { shallow } from 'vue-test-utils'
import Vuex from 'vuex'
import Vue from 'vue'
import VueI18n from 'vue-i18n'
import SettingBasic from '../../SettingBasic/SettingBasic.vue'
import { localVue } from '../../../../../test/common/spec_common'
import { project, favoriteRules, groupAndUser } from '../mock'
import * as utils from '../../../../util/index'
import * as business from '../../../../util/business'
import * as handler from '../../SettingAdvanced/handler'
import EditableBlock from '../../../common/EditableBlock/EditableBlock'

Vue.use(VueI18n)

const mockHandleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback) => {
  callback(res)
})
const mockHandleSuccessAsync = jest.spyOn(utils, 'handleSuccessAsync').mockImplementation(res => {
  return Promise.resolve(res)
})
const mockHandleError = jest.spyOn(business, 'handleError').mockImplementation()
const mockApi = {
  mockUpdateProjectGeneralInfo: jest.fn().mockImplementation(),
  mockUpdateSegmentConfig: jest.fn().mockImplementation(),
  mockUpdatePushdownConfig: jest.fn().mockImplementation(),
  mockUpdateStorageQuota: jest.fn().mockImplementation(),
  mockUpdateIndexOptimization: jest.fn().mockImplementation(),
  mockResetConfig: jest.fn().mockImplementation(),
  mockGetFavoriteRules: jest.fn().mockImplementation(() => {
    return Promise.resolve(favoriteRules)
  }),
  mockGetUserAndGroups: jest.fn().mockResolvedValue(groupAndUser),
  mockUpdateFavoriteRules: jest.fn().mockImplementation(),
  mockCallGlobalDetailDialog: jest.fn().mockImplementation()
}
const DetailDialogModal = {
  namespaced: true,
  actions: {
    'CALL_MODAL': mockApi.mockCallGlobalDetailDialog
  }
}
const store = new Vuex.Store({
  state: {
    config: {
      platform: 'PC'
    },
    project: {
      isSemiAutomatic: true
    }
  },
  actions: {
    'UPDATE_PROJECT_GENERAL_INFO': mockApi.mockUpdateProjectGeneralInfo,
    'UPDATE_SEGMENT_CONFIG': mockApi.mockUpdateSegmentConfig,
    'UPDATE_PUSHDOWN_CONFIG': mockApi.mockUpdatePushdownConfig,
    'UPDATE_STORAGE_QUOTA': mockApi.mockUpdateStorageQuota,
    'UPDATE_INDEX_OPTIMIZATION': mockApi.mockUpdateIndexOptimization,
    'RESET_PROJECT_CONFIG': mockApi.mockResetConfig,
    'GET_FAVORITE_RULES': mockApi.mockGetFavoriteRules,
    'GET_USER_AND_GROUPS': mockApi.mockGetUserAndGroups,
    'UPDATE_FAVORITE_RULES': mockApi.mockUpdateFavoriteRules
  },
  getters: {
    currentSelectedProject: () => {
      return 'Kyligence'
    }
  },
  modules: {
    DetailDialogModal
  }
})

const mockRoute = {
  query: {
    moveTo: 'index-suggest-setting'
  }
}
const mockMessage = jest.fn().mockImplementation()
const _EditableBlock = shallow(EditableBlock)
const wrapper = shallow(SettingBasic, {
  store,
  localVue,
  mocks: {
    $route: mockRoute,
    handleSuccess: mockHandleSuccess,
    handleError: mockHandleError,
    handleSuccessAsync: mockHandleSuccessAsync,
    $message: mockMessage
  },
  propsData: {
    project
  },
  components: {
    EditableBlock: _EditableBlock
  }
})
wrapper.vm.$refs = {
  acclerationRuleSettings: {
    $el: {
      scrollIntoView: jest.fn()
    }
  },
  'segment-setting-form': {
    validate: jest.fn(),
    clearValidate: jest.fn()
  },
  'setting-storage-quota': {
    validate: jest.fn().mockResolvedValue(true)
  },
  'setting-index-optimization': {
    validate: jest.fn()
  },
  'rulesForm': {
    validate: jest.fn()
  }
}

describe('Component SettingBasic', () => {
  it('init', async () => {
    expect(wrapper.vm.rulesAccerationDefault).toEqual({"count_enable": true, "count_value": 0, "duration_enable": false, "max_duration": 0, "min_duration": 0, "recommendation_enable": true, "recommendations_value": 20, "submitter_enable": true, "user_groups": [], "users": []})
    await wrapper.vm.$options.mounted[0].call(wrapper.vm)
    expect(wrapper.vm.form).toEqual({"alias": "xm_test_1", "auto_merge_enabled": true, "auto_merge_time_ranges": ["WEEK", "MONTH"], "description": undefined, "frequency_time_window": "MONTH", "low_frequency_threshold": 0, "maintain_model_type": "MANUAL_MAINTAIN", "project": "xm_test_1", "push_down_enabled": true, "push_down_range_limited": undefined, "retention_range": {"retention_range_enabled": false, "retention_range_number": 1, "retention_range_type": "MONTH"}, "semi_automatic_mode": true, "storage_garbage": true, "storage_quota_size": 14293651161088, "storage_quota_tb_size": "13.00", "volatile_range": {"volatile_range_enabled": true, "volatile_range_number": 0, "volatile_range_type": "DAY"}})
    expect(wrapper.vm.$refs.acclerationRuleSettings.$el.scrollIntoView).toBeCalled()
    expect(mockApi.mockGetFavoriteRules.mock.calls[0][1]).toEqual({"project": "Kyligence"})
    expect(wrapper.vm.rulesObj).toEqual(favoriteRules)
    expect(wrapper.vm.rulesAccerationDefault).toEqual(favoriteRules)
    expect(mockApi.mockGetUserAndGroups.mock.calls[0][1]).toEqual()
    expect(wrapper.vm.allSubmittersOptions).toEqual(groupAndUser)
    expect(wrapper.vm.filterUsers).toEqual(groupAndUser.user)
  })
  it('computed', () => {
    expect(wrapper.vm.projectIcon).toBe('el-icon-ksd-expert_mode_small')
    expect(wrapper.vm.retentionRangeScale).toBe('month')
    expect(wrapper.vm.rules['volatile_range.volatile_range_number'][0].trigger).toBe('change')
    expect(wrapper.vm.rules['retention_range.retention_range_number'][0].trigger).toBe('change')
    expect(wrapper.vm.storageQuota['storage_quota_tb_size'][0].trigger).toBe('change')
    expect(wrapper.vm.indexOptimization['low_frequency_threshold'][0].trigger).toBe('change')
  })
  it('methods', async () => {
    const callback = jest.fn()
    const error = new Error(null)
    wrapper.vm.validatePass({field: 'duration'}, '', callback)
    expect(callback).toBeCalledWith()

    wrapper.vm.handleCheckMergeRanges(['MONTH', 'WEEK'])
    expect(wrapper.vm.form.auto_merge_time_ranges).toEqual(['MONTH', 'WEEK'])

    await wrapper.vm.handleSwitch('auto-merge', false)
    expect(mockApi.mockUpdateSegmentConfig.mock.calls[0][1]).toEqual({"auto_merge_enabled": false, "auto_merge_time_ranges": ["WEEK", "MONTH"], "project": "xm_test_1", "retention_range": {"retention_range_enabled": false, "retention_range_number": 1, "retention_range_type": "MONTH"}, "volatile_range": {"volatile_range_enabled": true, "volatile_range_number": 0, "volatile_range_type": "DAY"}})
    expect(wrapper.emitted()['reload-setting'].length).toBe(1)
    expect(mockMessage).toBeCalledWith({"message": "Updated successfully.", "type": "success"})

    await wrapper.vm.handleSwitch('auto-retention', false)
    expect(mockApi.mockUpdateSegmentConfig.mock.calls[0][1]).toEqual({"auto_merge_enabled": false, "auto_merge_time_ranges": ["WEEK", "MONTH"], "project": "xm_test_1", "retention_range": {"retention_range_enabled": false, "retention_range_number": 1, "retention_range_type": "MONTH"}, "volatile_range": {"volatile_range_enabled": true, "volatile_range_number": 0, "volatile_range_type": "DAY"}})
    expect(wrapper.emitted()['reload-setting'].length).toBe(2)
    expect(mockMessage).toBeCalledWith({"message": "Updated successfully.", "type": "success"})

    await wrapper.vm.handleSwitch('pushdown-range', 100)
    expect(mockApi.mockUpdateSegmentConfig.mock.calls[0][1]).toEqual({"auto_merge_enabled": false, "auto_merge_time_ranges": ["WEEK", "MONTH"], "project": "xm_test_1", "retention_range": {"retention_range_enabled": false, "retention_range_number": 1, "retention_range_type": "MONTH"}, "volatile_range": {"volatile_range_enabled": true, "volatile_range_number": 0, "volatile_range_type": "DAY"}})
    expect(wrapper.emitted()['reload-setting'].length).toBe(3)
    expect(mockMessage).toBeCalledWith({"message": "Updated successfully.", "type": "success"})

    await wrapper.vm.handleSwitch('pushdown-engine', false)
    expect(mockApi.mockUpdateSegmentConfig.mock.calls[0][1]).toEqual({"auto_merge_enabled": false, "auto_merge_time_ranges": ["WEEK", "MONTH"], "project": "xm_test_1", "retention_range": {"retention_range_enabled": false, "retention_range_number": 1, "retention_range_type": "MONTH"}, "volatile_range": {"volatile_range_enabled": true, "volatile_range_number": 0, "volatile_range_type": "DAY"}})
    expect(wrapper.emitted()['reload-setting'].length).toBe(4)
    expect(mockMessage).toBeCalledWith({"message": "Updated successfully.", "type": "success"})

    const callbackFnc = {
      success: jest.fn(),
      error: jest.fn()
    }
    await wrapper.vm.handleSubmit('basic-info', callbackFnc.success, callbackFnc.error)
    expect(mockApi.mockCallGlobalDetailDialog.mock.calls[0][1]).toEqual({"dangerouslyUseHTMLString": true, "dialogType": "warning", "isBeta": true, "msg": "Please note that this feature is still in BETA phase. Potential risks or known limitations might exist. Check <a class=\"ky-a-like\" href=\"https://docs.kyligence.io/books/v4.2/en/acceleration/\" target=\"_blank\">user manual</a> for details.<br/>Do you want to continue?", "needConcelReject": true, "showDetailBtn": false, "submitText": "Turn On", "title": "Turn On Recommendation"})
    expect(mockApi.mockUpdateProjectGeneralInfo.mock.calls[0][1]).toEqual({"alias": "xm_test_1", "description": undefined, "maintain_model_type": "MANUAL_MAINTAIN", "project": "xm_test_1", "semi_automatic_mode": true})
    expect(callbackFnc.success).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(5)
    expect(mockMessage).toBeCalledWith({"message": "Updated successfully.", "type": "success"})

    await wrapper.vm.handleSubmit('segment-settings', callbackFnc.success, callbackFnc.error)
    expect(wrapper.vm.$refs['segment-setting-form'].validate).toBeCalled()
    expect(mockApi.mockUpdateSegmentConfig.mock.calls[0][1]).toEqual({"auto_merge_enabled": false, "auto_merge_time_ranges": ["WEEK", "MONTH"], "project": "xm_test_1", "retention_range": {"retention_range_enabled": false, "retention_range_number": 1, "retention_range_type": "MONTH"}, "volatile_range": {"volatile_range_enabled": true, "volatile_range_number": 0, "volatile_range_type": "DAY"}})
    expect(callbackFnc.success).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(5)
    expect(mockMessage).toBeCalledWith({"message": "Updated successfully.", "type": "success"})

    await wrapper.vm.handleSubmit('storage-quota', callbackFnc.success, callbackFnc.error)
    expect(wrapper.vm.$refs['setting-storage-quota'].validate).toBeCalled()
    expect(wrapper.vm.form.storage_quota_size).toBe(14293651161088)
    expect(mockApi.mockUpdateStorageQuota.mock.calls[0][1]).toEqual({"project": "xm_test_1", "storage_quota_size": 14293651161088, "storage_quota_tb_size": "13.00"})
    expect(callbackFnc.success).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(6)
    expect(mockMessage).toBeCalledWith({"message": "Updated successfully.", "type": "success"})
    // console.log(wrapper.vm.$store._actions)
    // wrapper.vm.$store._actions.GET_FAVORITE_RULES = jest.fn().mockImplementation(() => {
    //   return Promise.reject()
    // })
    // await wrapper.update()
    // await wrapper.vm.getAccelerationRules()
    // expect(mockHandleError).toBeCalled()
  })
})
