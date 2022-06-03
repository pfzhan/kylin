import { shallowMount } from '@vue/test-utils'
import Vuex from 'vuex'
import Vue from 'vue'
import VueI18n from 'vue-i18n'
import SettingBasic from '../../SettingBasic/SettingBasic.vue'
import { localVue } from '../../../../../test/common/spec_common'
import { project, favoriteRules, groupAndUser } from '../mock'
import * as utils from '../../../../util/index'
import * as business from '../../../../util/business'
import * as handler from '../../SettingBasic/handler'
import EditableBlock from '../../../common/EditableBlock/EditableBlock'

Vue.use(VueI18n)
jest.useFakeTimers()

const mockHandleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback, errorCallback) => {
  callback(res)
  errorCallback && errorCallback()
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
  mockResetConfig: jest.fn().mockImplementation((store, {project, reset_item}) => {
    if (reset_item === 'segment_config') {
      return Promise.resolve({ ...wrapper.vm.form, auto_merge_enabled: false })
    } else if (reset_item === 'storage_quota_config') {
      return Promise.resolve({ ...wrapper.vm.form, storage_quota_size: 100000000 })
    } else if (reset_item === 'garbage_cleanup_config') {
      return Promise.resolve({ ...wrapper.vm.form, low_frequency_threshold: 10 })
    } else if (reset_item === 'favorite_rule_config') {
      return Promise.resolve({ ...wrapper.vm.rulesObj, min_duration: 10, max_duration: 20, favorite_rules: {} })
    }
  }),
  mockGetFavoriteRules: jest.fn().mockImplementation(() => {
    return Promise.resolve(favoriteRules)
  }),
  mockGetUserAndGroups: jest.fn().mockResolvedValue(groupAndUser),
  mockUpdateFavoriteRules: jest.fn().mockImplementation(() => {
    return Promise.resolve()
  }),
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
    },
    currentProjectData: () => {
      return {
        name: 'Kyligence',
        override_kylin_properties: {
          'kylin.metadata.semi-automatic-mode': "true",
          'kylin.query.metadata.expose-computed-column': "true",
          'kylin.snapshot.manual-management-enabled': "true",
          'kylin.source.default': "9"
        }
      }
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
// const EditableBlock = shallowMount(EditableBlock)
const wrapper = shallowMount(SettingBasic, {
  store,
  localVue,
  mocks: {
    $route: mockRoute,
    handleSuccess: mockHandleSuccess,
    handleError: mockHandleError,
    handleSuccessAsync: mockHandleSuccessAsync,
    $message: mockMessage,
    $refs: {
      acclerationRuleSettings: {
        $el: {
          scrollIntoView: jest.fn()
        }
      }
    }
  },
  propsData: {
    project
  },
  components: {
    EditableBlock
  }
})
wrapper.vm.$refs = {
  acclerationRuleSettings: {
    $el: {
      scrollIntoView: jest.fn()
    }
  },
  'segment-setting-form': {
    validate: jest.fn().mockResolvedValue(true),
    clearValidate: jest.fn()
  },
  'setting-storage-quota': {
    validate: jest.fn().mockResolvedValue(true),
    clearValidate: jest.fn()
  },
  'setting-index-optimization': {
    validate: jest.fn().mockResolvedValue(true),
    clearValidate: jest.fn()
  },
  'rulesForm': {
    validate: jest.fn().mockImplementation((callback) => {
      return new Promise(resolve => {
        callback && callback(true)
        resolve(true)
      })
    }),
    clearValidate: jest.fn()
  }
}

describe('Component SettingBasic', () => {
  it('init', async () => {
    expect(wrapper.vm.rulesAccerationDefault).toEqual({'count_enable': true, 'count_value': 0, 'duration_enable': false, 'effective_days': 2, 'excluded_tables': [], 'excluded_tables_enable': false, 'max_duration': 0, 'min_duration': 0, 'min_hit_count': 30, 'recommendation_enable': true, 'recommendations_value': 20, 'submitter_enable': true, 'update_frequency': 2, 'user_groups': [], 'users': []})
    await wrapper.vm.$options.mounted[0].call(wrapper.vm)
    expect(wrapper.vm.form).toEqual({'JDBCConnectSetting': [], 'alias': 'xm_test_1', 'auto_merge_enabled': true, 'auto_merge_time_ranges': ['WEEK', 'MONTH'], 'jdbc_datasource_enabled': false, 'create_empty_segment_enabled': undefined, 'description': undefined, 'frequency_time_window': 'MONTH', 'low_frequency_threshold': 0, 'project': 'xm_test_1', 'push_down_enabled': true, 'push_down_range_limited': undefined, 'retention_range': {'retention_range_enabled': false, 'retention_range_number': '1', 'retention_range_type': 'MONTH'}, 'semi_automatic_mode': true, 'storage_garbage': true, 'storage_quota_size': 14293651161088, 'storage_quota_tb_size': '13.00', 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': '0', 'volatile_range_type': 'DAY'}})
    expect(wrapper.vm.$refs.acclerationRuleSettings.$el.scrollIntoView).toBeCalled()
    expect(mockApi.mockGetFavoriteRules.mock.calls[0][1]).toEqual({'project': 'Kyligence'})
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
    expect(wrapper.vm.filterSubmitterUserOptions).toEqual(['ADMIN', 'ANALYST', 'fanfan', 'fengys', 'gaoyuan'])
  })
  it('methods', async (done) => {
    const callback = jest.fn()
    // const error = new Error(null)
    wrapper.vm.validatePass({field: 'duration'}, '', callback)
    expect(callback).toBeCalledWith()

    wrapper.vm.handleCheckMergeRanges(['MONTH', 'WEEK'])
    expect(wrapper.vm.form.auto_merge_time_ranges).toEqual(['MONTH', 'WEEK'])

    await wrapper.vm.handleSwitch('auto-merge', false)
    expect(mockApi.mockUpdateSegmentConfig.mock.calls[0][1]).toEqual({'auto_merge_enabled': false, 'auto_merge_time_ranges': ['WEEK', 'MONTH'], 'create_empty_segment_enabled': undefined, 'project': 'xm_test_1', 'retention_range': {'retention_range_enabled': false, 'retention_range_number': '1', 'retention_range_type': 'MONTH'}, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': '0', 'volatile_range_type': 'DAY'}})
    expect(wrapper.emitted()['reload-setting'].length).toBe(1)
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})

    await wrapper.vm.handleSwitch('auto-retention', false)
    expect(mockApi.mockUpdateSegmentConfig.mock.calls[0][1]).toEqual({'auto_merge_enabled': false, 'auto_merge_time_ranges': ['WEEK', 'MONTH'], 'create_empty_segment_enabled': undefined, 'project': 'xm_test_1', 'retention_range': {'retention_range_enabled': false, 'retention_range_number': '1', 'retention_range_type': 'MONTH'}, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': '0', 'volatile_range_type': 'DAY'}})
    expect(wrapper.emitted()['reload-setting'].length).toBe(2)
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})

    await wrapper.vm.handleSwitch('pushdown-range', 100)
    expect(mockApi.mockUpdateSegmentConfig.mock.calls[0][1]).toEqual({'auto_merge_enabled': false, 'auto_merge_time_ranges': ['WEEK', 'MONTH'], 'create_empty_segment_enabled': undefined, 'project': 'xm_test_1', 'retention_range': {'retention_range_enabled': false, 'retention_range_number': '1', 'retention_range_type': 'MONTH'}, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': '0', 'volatile_range_type': 'DAY'}})
    expect(wrapper.emitted()['reload-setting'].length).toBe(3)
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})

    await wrapper.vm.handleSwitch('pushdown-engine', false)
    expect(mockApi.mockUpdateSegmentConfig.mock.calls[0][1]).toEqual({'auto_merge_enabled': false, 'auto_merge_time_ranges': ['WEEK', 'MONTH'], 'create_empty_segment_enabled': undefined, 'project': 'xm_test_1', 'retention_range': {'retention_range_enabled': false, 'retention_range_number': '1', 'retention_range_type': 'MONTH'}, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': '0', 'volatile_range_type': 'DAY'}})
    expect(wrapper.emitted()['reload-setting'].length).toBe(4)
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})

    const callbackFnc = {
      success: jest.fn(),
      error: jest.fn()
    }
    await wrapper.vm.handleSubmit('basic-info', callbackFnc.success, callbackFnc.error)
    expect(mockApi.mockCallGlobalDetailDialog.mock.calls[0][1]).toEqual({'dangerouslyUseHTMLString': true, 'dialogType': 'warning', 'isBeta': false, 'isCenterBtn': true, 'msg': 'With recommendation mode turned on, the system will generate recommendations for existing models by analyzing the query history and model usage. You may set the related preferences in settings.<br/><br/>Do you want to continue?', 'needConcelReject': true, 'showDetailBtn': false, 'submitText': 'Turn On', 'title': 'Turn On Recommendation', 'wid': '400px'})
    expect(mockApi.mockUpdateProjectGeneralInfo.mock.calls[0][1]).toEqual({'alias': 'xm_test_1', 'description': undefined, 'project': 'xm_test_1', 'semi_automatic_mode': true})
    expect(callbackFnc.success).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(5)
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})

    await wrapper.vm.handleSubmit('segment-settings', callbackFnc.success, callbackFnc.error)
    expect(wrapper.vm.$refs['segment-setting-form'].validate).toBeCalled()
    expect(mockApi.mockUpdateSegmentConfig.mock.calls[0][1]).toEqual({'auto_merge_enabled': false, 'auto_merge_time_ranges': ['WEEK', 'MONTH'], 'create_empty_segment_enabled': undefined, 'project': 'xm_test_1', 'retention_range': {'retention_range_enabled': false, 'retention_range_number': '1', 'retention_range_type': 'MONTH'}, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': '0', 'volatile_range_type': 'DAY'}})
    expect(callbackFnc.success).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(6)
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})

    await wrapper.vm.handleSubmit('storage-quota', callbackFnc.success, callbackFnc.error)
    expect(wrapper.vm.$refs['setting-storage-quota'].validate).toBeCalled()
    expect(wrapper.vm.form.storage_quota_size).toBe(14293651161088)
    expect(mockApi.mockUpdateStorageQuota.mock.calls[0][1]).toEqual({'project': 'xm_test_1', 'storage_quota_size': 14293651161088, 'storage_quota_tb_size': '13.00'})
    expect(callbackFnc.success).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(7)
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})

    await wrapper.vm.handleSubmit('index-optimization', callbackFnc.success, callbackFnc.error)
    expect(wrapper.vm.$refs['setting-index-optimization'].validate).toBeCalled()
    expect(mockApi.mockUpdateIndexOptimization.mock.calls[0][1]).toEqual({'frequency_time_window': 'MONTH', 'low_frequency_threshold': 0, 'project': 'xm_test_1'})
    expect(callbackFnc.success).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(8)
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})

    await wrapper.vm.handleSubmit('accleration-rule-settings', callbackFnc.success, callbackFnc.error)
    expect(wrapper.vm.$refs['rulesForm'].validate).toBeCalled()
    expect(callbackFnc.success).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(9)
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})

    await wrapper.vm.handleResetForm('segment-settings', callbackFnc.success, callbackFnc.error)
    expect(mockApi.mockResetConfig.mock.calls[0][1]).toEqual({'project': 'Kyligence', 'reset_item': 'segment_config'})
    expect(mockHandleSuccessAsync).toBeCalled()
    expect(wrapper.vm.form.auto_merge_enabled).toBeFalsy()
    expect(wrapper.vm.$refs['segment-setting-form'].clearValidate).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(10)
    expect(mockMessage).toBeCalledWith({'message': 'Reset successfully.', 'type': 'success'})

    await wrapper.vm.handleResetForm('storage-quota', callbackFnc.success, callbackFnc.error)
    expect(mockApi.mockResetConfig.mock.calls[1][1]).toEqual({'project': 'Kyligence', 'reset_item': 'storage_quota_config'})
    expect(mockHandleSuccessAsync).toBeCalled()
    expect(wrapper.vm.form.storage_quota_size).toBe(100000000)
    expect(wrapper.vm.form.storage_quota_tb_size).toBe('0.00')
    expect(wrapper.vm.$refs['setting-storage-quota'].clearValidate).toBeCalled()
    expect(callbackFnc.success).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(11)
    expect(mockMessage).toBeCalledWith({'message': 'Reset successfully.', 'type': 'success'})

    await wrapper.vm.handleResetForm('index-optimization', callbackFnc.success, callbackFnc.error)
    expect(mockApi.mockResetConfig.mock.calls[2][1]).toEqual({'project': 'Kyligence', 'reset_item': 'garbage_cleanup_config'})
    expect(mockHandleSuccessAsync).toBeCalled()
    expect(wrapper.vm.form.low_frequency_threshold).toBe(10)
    expect(wrapper.vm.$refs['setting-index-optimization'].clearValidate).toBeCalled()
    expect(callbackFnc.success).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(12)
    expect(mockMessage).toBeCalledWith({'message': 'Reset successfully.', 'type': 'success'})

    await wrapper.vm.handleResetForm('accleration-rule-settings', callbackFnc.success, callbackFnc.error)
    expect(mockApi.mockResetConfig.mock.calls[3][1]).toEqual({'project': 'Kyligence', 'reset_item': 'favorite_rule_config'})
    expect(mockHandleSuccessAsync).toBeCalled()
    expect(wrapper.vm.rulesObj.min_duration).toBe(0)
    expect(wrapper.vm.rulesObj.max_duration).toBe(0)
    expect(callbackFnc.success).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(12)
    expect(mockMessage).toBeCalledWith({'message': 'Reset successfully.', 'type': 'success'})

    expect(wrapper.vm.isFormEdited(wrapper.vm.form, 'basic-info')).toBeFalsy()
    expect(wrapper.vm.isFormEdited(wrapper.vm.form, 'segment-settings')).toBeTruthy()
    expect(wrapper.vm.isFormEdited(wrapper.vm.form, 'storage-quota')).toBeTruthy()
    expect(wrapper.vm.isFormEdited(wrapper.vm.form, 'index-optimization')).toBeTruthy()
    expect(wrapper.vm.isFormEdited(wrapper.vm.form, 'accleration-rule-settings')).toBeFalsy()

    wrapper.vm.remoteMethod('')
    expect(wrapper.vm.filterUsers).toEqual(['ADMIN', 'ANALYST', 'fanfan', 'fengys', 'gaoyuan'])
    wrapper.vm.remoteMethod('A')
    jest.runAllTimers()
    expect(wrapper.vm.filterUsers).toEqual(['ADMIN', 'ANALYST', 'fanfan', 'gaoyuan'])

    wrapper.vm.saveAcclerationRule()
    expect(mockApi.mockUpdateFavoriteRules.mock.calls[0][1]).toEqual({'count_enable': true, 'count_value': 0, 'duration_enable': false, 'effective_days': 2, 'excluded_tables': '', 'excluded_tables_enable': false, 'max_duration': 0, 'min_duration': 0, 'min_hit_count': 30, 'project': 'Kyligence', 'recommendation_enable': true, 'recommendations_value': 20, 'submitter_enable': true, 'update_frequency': 2, 'user_groups': [], 'users': []})
    expect(mockHandleSuccess).toBeCalled()
    expect(wrapper.vm.rulesAccerationDefault === wrapper.vm.rulesObj).toBeFalsy()

    wrapper.vm.$store._actions.UPDATE_FAVORITE_RULES = [jest.fn().mockImplementation(() => {
      return {
        then: (successCallback, errorCallback) => {
          errorCallback()
        }
      }
    })]
    wrapper.vm.saveAcclerationRule()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.$store._actions.GET_FAVORITE_RULES = [jest.fn().mockRejectedValue(false)]
    await wrapper.vm.getAccelerationRules()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.$store._actions.GET_USER_AND_GROUPS = [jest.fn().mockRejectedValue(false)]
    await wrapper.vm.getAccelerationRules()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.$store._actions.RESET_PROJECT_CONFIG = [jest.fn().mockRejectedValue(false)]
    await wrapper.vm.handleResetForm('segment-settings', callbackFnc.success, callbackFnc.error)
    expect(callbackFnc.error).toBeCalled()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.$store._actions.UPDATE_PROJECT_GENERAL_INFO = [jest.fn().mockRejectedValue(false)]
    await wrapper.vm.handleSubmit('basic-info', callbackFnc.success, callbackFnc.error)
    expect(callbackFnc.error).toBeCalled()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.$store._actions.UPDATE_SEGMENT_CONFIG = [jest.fn().mockRejectedValue(false)]
    await wrapper.vm.handleSwitch('auto-merge', false)
    expect(mockHandleError).toBeCalled()

    // wrapper.setData({
    //   rulesObj: {
    //     ...wrapper.vm.rulesObj,
    //     min_duration: 50
    //   }
    // })
    // await wrapper.update()
    wrapper.vm.rulesObj.min_duration = 50
    wrapper.vm.$nextTick(() => {
      expect(wrapper.vm.durationError).toBeFalsy()
      done()
    })

    // console.log(wrapper.vm.$store._actions)
    // wrapper.vm.$store._actions.GET_FAVORITE_RULES = jest.fn().mockImplementation(() => {
    //   return Promise.reject()
    // })
    // await wrapper.update()
    // await wrapper.vm.getAccelerationRules()
    // expect(mockHandleError).toBeCalled()
  })
})

describe('SettingBasic handler', () => {
  it('event', () => {
    const callbackValidate = jest.fn()
    global.window.kapVm = wrapper.vm
    handler.validate.storageQuotaSize.call(wrapper.vm, null, '', callbackValidate)
    expect(callbackValidate).not.toBe()
    handler.validate.storageQuotaSize.call(wrapper.vm, null, 10, callbackValidate)
    expect(callbackValidate).toBeCalledWith()

    handler.validate.positiveNumber.call(wrapper.vm, null, 'ab123', callbackValidate)
    expect(callbackValidate.mock.calls[0][0]).toBeTruthy()
    handler.validate.positiveNumber.call(wrapper.vm, null, '10.00', callbackValidate)
    expect(callbackValidate).toBeCalledWith()

    handler.validate.storageQuotaNum.call(wrapper.vm, null, '', callbackValidate)
    expect(callbackValidate.mock.calls[0][0]).toBeTruthy()
    handler.validate.storageQuotaNum.call(wrapper.vm, null, 100, callbackValidate)
    expect(callbackValidate).toBeCalledWith()
  })
})
