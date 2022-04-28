import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import EditProjectConfigDialog from '../../EditProjectConfigDialog/index.vue'
import Vuex from 'vuex'
import EditProjectConfigDialogStore from '../../EditProjectConfigDialog/store'
import * as business from '../../../../util/business'

const mockHandleError = jest.spyOn(business, 'handleError').mockRejectedValue(false)
const mockMessage = jest.fn()
const mockApi = {
  mockLoadConfigByProject: jest.fn().mockImplementation(),
  mockUpdateProjectConfig: jest.fn().mockImplementation()
}

const store = new Vuex.Store({
  state: {
    project: {
      defaultConfigList: ['kylin.source.default']
    }
  },
  actions: {
    'LOAD_CONFIG_BY_PROJEECT': mockApi.mockLoadConfigByProject,
    'UPDATE_PROJECT_CONFIG': mockApi.mockUpdateProjectConfig
  },
  modules: {
    'EditProjectConfigDialog': EditProjectConfigDialogStore
  },
  getters: {
    currentProjectData () {
      return {
        create_time: 1606709290609,
        create_time_utc: 1606709290609,
        default_database: "DEFAULT",
        description: "",
        keytab: null,
        last_modified: 1611233129785,
        mvcc: 16,
        name: "xm_test",
        override_kylin_properties: {'kylin.metadata.semi-automatic-mode': "true", 'kylin.query.metadata.expose-computed-column': "true", 'kylin.source.default': "9", 'kylin.snapshot.manual-management-enabled': "true", 'kylin.model.multi-partition-enabled': "true"},
        owner: "ADMIN",
        permission: "ADMINISTRATION",
        principal: null,
        segment_config: {auto_merge_enabled: false, auto_merge_time_ranges: [], volatile_range: {
          volatile_range_enabled: true,
          volatile_range_number: 0,
          volatile_range_type: "DAY"
        }, retention_range: {
          retention_range_enabled: false,
          retention_range_number: 1,
          retention_range_type: "MONTH"
        }, create_empty_segment_enabled: true},
        status: "ENABLED",
        uuid: "98c88a35-6a88-442f-8e74-167a8244049d",
        version: "4.0.0.0"
      }
    },
    configList () {
      return [{key: 'kylin.model.multi-partition-enabled'}]
    }
  }
})

const wrapper = shallowMount(EditProjectConfigDialog, {
  store,
  localVue,
  mocks: {
    handleError: mockHandleError,
    $message: mockMessage
  }
})

describe('Component EditProjectConfigDialog', () => {
  it('computed', () => {
    expect(wrapper.vm.modalTitle).toBe('Add Custom Project Configuration')
  })
  it('methods', async () => {
    const callbackEvent = jest.fn()
    wrapper.vm.validateConfigKey(null, '', callbackEvent)
    expect(callbackEvent.mock.calls[0].toString()).toBe('Error: Configuration is required')

    wrapper.vm.validateConfigKey(null, 'kylin.model.multi-partition-enabled', callbackEvent)
    expect(callbackEvent.mock.calls[1].toString()).toBe('Error: The key is already exists')

    wrapper.vm.validateConfigKey(null, 'kylin.source.default', callbackEvent)
    expect(callbackEvent.mock.calls[2].toString()).toBe('Error: The configuration does not support add')

    wrapper.vm.validateConfigKey(null, 'kylin.snapshot.manual-management-enabled', callbackEvent)
    expect(callbackEvent.mock.calls[3].toString()).toBe('')

    wrapper.vm.validateConfigValue(null, '', callbackEvent)
    expect(callbackEvent.mock.calls[4].toString()).toBe('Error: Value is required')

    wrapper.vm.validateConfigValue(null, true, callbackEvent)
    expect(callbackEvent.mock.calls[5].toString()).toBe('')

    wrapper.vm.$refs = {
      form: {
        resetFields: jest.fn(),
        validate: jest.fn().mockResolvedValue(true)
      }
    }

    wrapper.vm.handlerClose()
    expect(wrapper.vm.$refs.form.resetFields).toBeCalled()
    expect(wrapper.vm.$store.state.EditProjectConfigDialog.form).toEqual({"key": "", "value": ""})
    expect(wrapper.vm.$store.state.EditProjectConfigDialog.isShow).toBeFalsy()

    wrapper.vm.handlerInput('key', 'kylin.snapshot.manual-management-enabled')
    wrapper.vm.handlerInput('value', 'false')
    expect(wrapper.vm.$store.state.EditProjectConfigDialog.form).toEqual({"key": "kylin.snapshot.manual-management-enabled", "value": 'false'})

    await wrapper.vm.submit()
    expect(mockApi.mockUpdateProjectConfig.mock.calls[0][1]).toEqual({"data": {"kylin.snapshot.manual-management-enabled": "false"}, "project": "xm_test"})
    expect(mockMessage).toBeCalledWith({"message": "The operation is successfully", "type": "success"})
    expect(wrapper.vm.loadingSubmit).toBeFalsy()
    expect(wrapper.vm.$refs.form.resetFields).toBeCalled()
    expect(mockApi.mockLoadConfigByProject.mock.calls[0][1]).toEqual({"exact": true, "page_offset": 0, "page_size": 1, "permission": "ADMINISTRATION", "project": "xm_test"})

    wrapper.vm.$store.state.EditProjectConfigDialog.originForm = {
      key: 'kylin.snapshot.manual-management-enabled',
      value: 'false'
    }

    await wrapper.vm.submit()
    expect(wrapper.vm.$store.state.EditProjectConfigDialog.form).toEqual({"key": "", "value": ""})
    expect(wrapper.vm.$store.state.EditProjectConfigDialog.isShow).toBeFalsy()

    wrapper.vm.$refs.form.validate = jest.fn().mockRejectedValue('false')
    await wrapper.vm.submit()
    expect(mockHandleError).toBeCalled()
    expect(wrapper.vm.loadingSubmit).toBeFalsy()

    wrapper.vm.$store._actions.LOAD_CONFIG_BY_PROJEECT = [jest.fn().mockRejectedValue(false)]
    await wrapper.vm.getConfigList()
    expect(mockHandleError).toBeCalled()
  })
  it('store', async () => {
    EditProjectConfigDialogStore.actions.CALL_MODAL({commit: (name, payloads) => EditProjectConfigDialogStore.mutations[name](EditProjectConfigDialogStore.state, payloads)}, {isShow: true, form: {key: 'kylin.project.num', value: '100'}, callback: null})
    expect(EditProjectConfigDialogStore.state.isShow).toBeTruthy()
    expect(EditProjectConfigDialogStore.state.form).toEqual({"key": "kylin.project.num", "value": "100"})
  })
})
