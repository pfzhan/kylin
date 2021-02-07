import Vue from 'vue'
import { shallow } from 'vue-test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import SettingAdvanced from '../../SettingAdvanced/SettingAdvanced.vue'
import EditableBlock from '../../../common/EditableBlock/EditableBlock'
import { project } from '../mock'
import Vuex from 'vuex'
import VueI18n from 'vue-i18n'
import * as utils from '../../../../util/index'
import * as business from '../../../../util/business'
import * as handler from '../../SettingAdvanced/handler'

Vue.use(VueI18n)

jest.useFakeTimers()

const mockHandleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback) => {
  callback && callback()
})
const mockHandleSuccessAsync = jest.spyOn(utils, 'handleSuccessAsync').mockImplementation((res) => {
  return new Promise((resolve) => {
    resolve(res)
  })
})
const mockHandleError = jest.spyOn(business, 'handleError').mockImplementation()
const mockKapConfirm = jest.spyOn(business, 'kapConfirm').mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const mockMessage = jest.fn()

const mockApi = {
  mockUpdateAccelerationSettings: jest.fn().mockImplementation(),
  mockUpdateJobAlertSettings: jest.fn().mockImplementation(),
  mockResetProjectConfig: jest.fn().mockResolvedValue(project),
  mockUpdateDefaultDBSettings: jest.fn().mockImplementation(() => {
    return new Promise((resolve) => {
      resolve(true)
    })
  }),
  mockFetchDatabases: jest.fn().mockImplementation(),
  mockUpdateYarnQueue: jest.fn().mockImplementation(),
  mockUpdateExposeCCConfig: jest.fn().mockImplementation(),
  mockUpdateKerberosConfig: jest.fn().mockImplementation(),
  mockReloadHiveDBTables: jest.fn().mockImplementation(),
  mockToggleEnableSCD: jest.fn().mockImplementation(() => {
    return Promise.resolve(true)
  }),
  mockGetSCD2Model: jest.fn().mockImplementation(() => {
    return Promise.resolve(['SCD_MODELS'])
  }),
  mockUpdateSCD2Enable: jest.fn().mockImplementation()
}

const store = new Vuex.Store({
  state: {
    user: {
      currentUser: {
        authorities: [{authority: 'ROLE_ADMIN'}],
        create_time: 1581666816489,
        defaultPassword: false,
        disabled: false,
        first_login_failed_time: 0,
        last_modified: 1598647737984,
        locked: false,
        locked_time: 0,
        mvcc: 201,
        username: 'ADMIN',
        uuid: '79db7b2c-9d07-4b76-b29e-573049b028d8',
        version: '4.0.0.0',
        wrong_time: 0
      }
    },
    system: {
      kerberosEnabled: 'true'
    },
    project: {
      projectDefaultDB: 'DEFAULT'
    },
    config: {
      platform: 'iframe'
    }
  },
  actions: {
    'UPDATE_ACCELERATION_SETTINGS': mockApi.mockUpdateAccelerationSettings,
    'UPDATE_JOB_ALERT_SETTINGS': mockApi.mockUpdateJobAlertSettings,
    'RESET_PROJECT_CONFIG': mockApi.mockResetProjectConfig,
    'UPDATE_DEFAULT_DB_SETTINGS': mockApi.mockUpdateDefaultDBSettings,
    'FETCH_DATABASES': mockApi.mockFetchDatabases,
    'UPDATE_YARN_QUEUE': mockApi.mockUpdateYarnQueue,
    'UPDATE_EXPOSE_CC_CONFIG': mockApi.mockUpdateExposeCCConfig,
    'UPDATE_KERBEROS_CONFIG': mockApi.mockUpdateKerberosConfig,
    'RELOAD_HIVE_DB_TABLES': mockApi.mockReloadHiveDBTables,
    'TOGGLE_ENABLE_SCD': mockApi.mockToggleEnableSCD,
    'GET_SCD2_MODEL': mockApi.mockGetSCD2Model
  },
  mutations: {
    'UPDATE_SCD2_ENABLE': mockApi.mockUpdateSCD2Enable
  },
  getters: {
    'currentSelectedProject': () => {
      return 'kyligence'
    },
    'currentProjectData': () => {
      return {
        name: 'xm_test_1',
        uuid: 'c47daf3b-816b-4965-94de-c6f3262874d2',
        default_database: 'DEFAULT',
        owner: 'ADMIN',
        override_kylin_properties: {
          'kap.metadata.semi-automatic-mode': 'true',
          'kap.query.metadata.expose-computed-column': 'true',
          'kylin.metadata.semi-automatic-mode': 'true',
          'kylin.query.metadata.expose-computed-column': 'true',
          'kylin.query.non-equi-join-model-enabled': 'false',
          'kylin.source.default': '9',
          'kylin.storage.quota-in-giga-bytes': '13312.0'
        },
        segment_config: {
          auto_merge_enabled: true,
          auto_merge_time_ranges: ['WEEK', 'MONTH', 'QUARTER', 'YEAR'],
          retention_range: {
            retention_range_enabled: false,
            retention_range_number: 1,
            retention_range_type: 'MONTH'
          },
          volatile_range: {
            volatile_range_enabled: false,
            volatile_range_number: 0,
            volatile_range_type: 'DAY'
          }
        }
      }
    },
    'isAutoProject': () => {
      return true
    },
    'settingActions': () => {
      return ['kerberosAcc', 'yarnQueue']
    }
  }
})

const _EditableBlock = shallow(EditableBlock)

const mockClearValidate = jest.fn()
const mockMsgBox = jest.fn().mockResolvedValue(true)

const wrapper = shallow(SettingAdvanced, {
  localVue,
  store,
  propsData: {
    project
  },
  components: {
    EditableBlock: _EditableBlock
  },
  mocks: {
    kapConfirm: mockKapConfirm,
    handleError: mockHandleError,
    handleSuccess: mockHandleSuccess,
    handleSuccessAsync: mockHandleSuccessAsync,
    $message: mockMessage,
    $confirm: mockKapConfirm,
    $createElement: document.createElement,
    $msgbox: mockMsgBox
  }
})

wrapper.vm.$refs = {
  'kerberos-setting-form': {
    clearValidate: mockClearValidate
  },
  'setDefaultDB': {
    validate: jest.fn().mockResolvedValue(true)
  }
}

// wrapper.vm.$refs = {
//   'kerberos-setting-form': {
//     clearValidate: mockClearValidate
//   }
// }

describe('Component SettingAdvanced', () => {
  it('init', () => {
    expect(mockApi.mockFetchDatabases.mock.calls[0][1]).toEqual({'projectName': 'kyligence', 'sourceType': 9})
    expect(wrapper.vm.form).toEqual({'data_load_empty_notification_enabled': false, 'defaultDatabase': 'SSB', 'default_database': 'DEFAULT', 'expose_computed_column': true, 'file': null, 'fileList': [], 'job_error_notification_enabled': false, 'job_notification_emails': [''], 'principal': null, 'project': 'xm_test_1', 'scd2_enabled': false, 'yarn_queue': 'default'})
    // expect(mockClearValidate).toBeCalled()
  })
  it('computed', async () => {
    expect(wrapper.vm.ifShowYarn).toBeFalsy()
    wrapper.vm.$store.state.config.platform = ''
    await wrapper.update()
    expect(wrapper.vm.ifShowYarn).toBeTruthy()
    expect('threshold' in wrapper.vm.accelerateRules).toBeTruthy()
    expect(wrapper.vm.setDefaultDBRules).toEqual({'default_database': {'message': 'Please select', 'required': true, 'trigger': 'change'}})
    expect(wrapper.vm.emailRules).toEqual([{'message': 'Please enter email', 'required': true, 'trigger': 'blur'}, {'message': 'Please enter vaild email.', 'trigger': 'blur', 'type': 'email'}])
    expect(wrapper.vm.yarnQueueRules['yarn_queue'][0].message).toEqual(['The queue name is required', 'Incorrect format'])
    expect(wrapper.vm.userType).toBeTruthy()
    expect('principal' in wrapper.vm.kerberosRules).toBeTruthy()
    expect(wrapper.vm.kerberosActionUrl).toBe('/kylin/api/projects/kyligence/project_kerberos_info')
  })
  it('methods', async () => {
    await wrapper.vm.handleSwitch()
    expect(mockKapConfirm).toBeCalledWith('If you turn off this option, when the BI or others system are connected to Kyligence Enterprise, Kyligence Enterprise will not expose the computed columns defined in Kyligence Enterprise to it. This operation may make the system connected to Kyligence Enterprise unusable.')
    expect(mockApi.mockUpdateExposeCCConfig.mock.calls[0][1]).toEqual({'expose_computed_column': undefined, 'project': 'xm_test_1'})
    expect(wrapper.emitted()['reload-setting']).toEqual([[]])
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})
    await wrapper.vm.handleSwitch(true)
    expect(mockApi.mockUpdateExposeCCConfig.mock.calls[1][1]).toEqual({'expose_computed_column': true, 'project': 'xm_test_1'})
    expect(wrapper.emitted()['reload-setting']).toEqual([[], []])
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})
    wrapper.vm.$store._actions.UPDATE_EXPOSE_CC_CONFIG = jest.fn().mockImplementation(() => {
      return Promise.reject()
    })
    await wrapper.update()
    await wrapper.vm.handleSwitch()
    expect(mockHandleError).toBeCalled()
    await wrapper.vm.handleSwitch(true)
    expect(mockHandleError).toBeCalled()

    const callback = {
      successCallback: jest.fn().mockImplementationOnce(),
      errorCallback: jest.fn().mockImplementationOnce()
    }
    const mockValidate = jest.fn().mockResolvedValue(true)
    const mockErrorValidate = jest.fn().mockResolvedValue(false)
    wrapper.vm.$refs = {
      'setDefaultDB': {
        validate: mockValidate,
      },
      'job-alert': {
        validate: mockValidate,
      },
      'yarn-setting-form': {
        validate: mockValidate
      },
      'kerberos-setting-form': {
        validate: mockValidate,
        clearValidate: jest.fn()
      }
    }
    await wrapper.update()
    // console.log(wrapper.vm.$refs)
    await wrapper.vm.handleSubmit('defaultDB-settings', callback.successCallback, callback.errorCallback)
    jest.runAllTimers()
    expect(mockKapConfirm).toBeCalledWith('Modifying the default database may result in saved queries or SQL files being unavailable. Please confirm whether to modify the default database to DEFAULT ?', 'Modify Default Database', {'cancelButtonText': 'Cancel', 'confirmButtonText': 'Submit', 'type': 'warning'})
    expect(mockValidate).toBeCalled()
    // expect(mockApi.mockUpdateDefaultDBSettings).toBeCalled()
    // expect(callback.successCallback).toBeCalled()
    expect(wrapper.emitted()['reload-setting']).toEqual([[], [], []])
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})

    await wrapper.vm.handleSubmit('job-alert', callback.successCallback, callback.errorCallback)
    expect(wrapper.vm.$refs['job-alert'].validate).toBeCalled()
    expect(mockApi.mockUpdateJobAlertSettings.mock.calls[0][1]).toEqual({'data_load_empty_notification_enabled': false, 'job_error_notification_enabled': false, 'job_notification_emails': [''], 'project': 'xm_test_1'})

    await wrapper.vm.handleSubmit('yarn-name', callback.successCallback, callback.errorCallback)
    expect(wrapper.vm.$refs['yarn-setting-form'].validate).toBeCalled()
    expect(mockApi.mockUpdateYarnQueue.mock.calls[0][1]).toEqual({'project': 'xm_test_1', 'queue_name': 'default'})
    expect(mockMessage).toBeCalledWith({'message': 'Updated successfully.', 'type': 'success'})

    await wrapper.vm.handleSubmit('kerberos-acc', callback.successCallback, callback.errorCallback)
    expect(mockMessage).toBeCalledWith({'message': 'Please upload keytab file', 'type': 'error'})
    expect(callback.errorCallback).toBeCalled()

    wrapper.setData({
      form: {
        ...wrapper.vm.form,
        fileList: [{ 
          name: 'test.keytab',
          percentage: 0,
          raw: {
            name: 'test.keytab',
            size: 8,
            type: '',
            uid: 1598852854585,
            webkitRelativePath: ''
          },
          size: 8,
          status: 'ready',
          uid: 1598852854585,
          url: 'blob:http://localhost:8080/94465053-2696-4432-9faf-e8f5cb7a4f35'
        }],
        file: {
          name: 'test.keytab',
          size: 8,
          type: '',
          uid: 1598852854585,
          webkitRelativePath: ''
        },
        principal: 'cccc',
        project: 'SSB_TEST'
      }
    })

    await wrapper.vm.handleSubmit('kerberos-acc', callback.successCallback, callback.errorCallback)
    expect(wrapper.vm.$refs['kerberos-setting-form'].validate).toBeCalled()
    const params = mockApi.mockUpdateKerberosConfig.mock.calls[0][1]
    expect(params.body).not.toBeUndefined()
    expect(params.project).toBe('SSB_TEST')
    expect(wrapper.vm.form.fileList).toEqual([])
    expect(wrapper.vm.form.file).toBeNull()
    expect(wrapper.vm.$refs['kerberos-setting-form'].clearValidate).toBeCalled()
    expect(mockKapConfirm).toBeCalledWith('Update successfully. The configuration will take effect after refreshing the datasource cache. Please confirm whether to refresh now. </br> Note: It will take a long time to refresh the cache. If you need to configure multiple projects, it is recommended to refresh when configuring the last project.', 'Refresh DataSource', {'cancelButtonText': 'Refresh Later', 'confirmButtonText': 'Refresh Now', 'dangerouslyUseHTMLString': true, 'type': 'warning'})
    expect(mockApi.mockReloadHiveDBTables.mock.calls[0][1]).toEqual({'force': true, 'project': 'kyligence'})
    expect(callback.successCallback).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(8)

    wrapper.vm.$refs = {
      'setDefaultDB': {
        validate: mockErrorValidate,
      },
      'job-alert': {
        validate: mockErrorValidate,
        clearValidate: jest.fn()
      },
      'yarn-setting-form': {
        validate: mockErrorValidate
      },
      'kerberos-setting-form': {
        validate: mockErrorValidate,
        clearValidate: jest.fn()
      }
    }

    await wrapper.update()
    await wrapper.vm.handleSubmit('defaultDB-settings', callback.successCallback, callback.errorCallback)
    expect(callback.errorCallback).toHaveBeenCalledTimes(1)
    await wrapper.vm.handleSubmit('job-alert', callback.successCallback, callback.errorCallback)
    expect(callback.errorCallback).toHaveBeenCalledTimes(3)

    await wrapper.vm.handleSubmit('yarn-name', callback.successCallback, callback.errorCallback)
    expect(callback.errorCallback).toHaveBeenCalledTimes(4)

    await wrapper.vm.handleSubmit('kerberos-acc', callback.successCallback, callback.errorCallback)
    expect(callback.errorCallback).toHaveBeenCalledTimes(5)

    await wrapper.vm.handleResetForm('job-alert', callback.successCallback, callback.errorCallback)
    expect(mockApi.mockResetProjectConfig.mock.calls[0][1]).toEqual({'project': 'kyligence', 'reset_item': 'job_notification_config'})
    expect(wrapper.vm.$refs['job-alert'].clearValidate).toBeCalled()
    expect(callback.successCallback).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(11)
    expect(mockMessage).toBeCalledWith({'message': 'Reset successfully.', 'type': 'success'})

    await wrapper.vm.handleResetForm('kerberos-acc', callback.successCallback, callback.errorCallback)
    expect(mockApi.mockResetProjectConfig.mock.calls[1][1]).toEqual({'project': 'kyligence', 'reset_item': 'kerberos_project_level_config'})
    expect(wrapper.vm.form.fileList).toEqual([])
    expect(wrapper.vm.form.file).toBeNull()
    expect(wrapper.vm.$refs['kerberos-setting-form'].clearValidate).toBeCalled()
    expect(callback.successCallback).toBeCalled()
    expect(wrapper.emitted()['reload-setting'].length).toBe(12)

    wrapper.vm.$store._actions.RESET_PROJECT_CONFIG = jest.fn().mockRejectedValue(false)
    await wrapper.update()
    await wrapper.vm.handleResetForm('job-alert', callback.successCallback, callback.errorCallback)
    expect(callback.errorCallback).toBeCalled()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.handleAddItem('job_notification_emails', 0)
    expect(wrapper.vm.form.job_notification_emails.length).toBe(2)

    wrapper.vm.handleRemoveItem('job_notification_emails', 1)
    expect(wrapper.vm.form.job_notification_emails.length).toBe(1)

    wrapper.vm.handleRemove()
    expect(wrapper.vm.form.fileList).toEqual([])

    const file = {
      name: 'test.keytab',
      size: 8,
      type: '',
      uid: 1598852854585,
      webkitRelativePath: ''
    }
    const fileList = [{ 
      name: 'test.keytab',
      percentage: 0,
      raw: {
        name: 'test.keytab',
        size: 8,
        type: '',
        uid: 1598852854585,
        webkitRelativePath: ''
      },
      size: 8,
      status: 'ready',
      uid: 1598852854585,
      url: 'blob:http://localhost:8080/94465053-2696-4432-9faf-e8f5cb7a4f35'
    }]
    const params1 = {
      file: JSON.parse(JSON.stringify(file)),
      fileList: JSON.parse(JSON.stringify(fileList))
    }
    wrapper.vm.changeFile(params1.file, params1.fileList)
    expect(wrapper.vm.form.fileList).toEqual(fileList)
    expect(wrapper.vm.form.file).toEqual(file)

    const params2 = {
      file: JSON.parse(JSON.stringify(file)),
      fileList: JSON.parse(JSON.stringify(fileList))
    }
    params2.file.size = 8 * 1024 * 1024
    params2.fileList[0].size = 8 * 1024 * 1024
    wrapper.vm.changeFile(params2.file, params2.fileList)
    expect(mockMessage).toBeCalledWith({'message': 'Files cannot exceed 5M.', 'type': 'error'})
    expect(wrapper.vm.form.fileList).toEqual([])
    expect(wrapper.vm.form.file).toBeNull()

    const params3 = {
      file: JSON.parse(JSON.stringify(file)),
      fileList: JSON.parse(JSON.stringify(fileList))
    }
    params3.file.name = 'test.txt'
    params3.fileList[0].name = 'test.txt'
    wrapper.vm.changeFile(params3.file, params3.fileList)
    expect(mockMessage).toBeCalledWith({'message': 'Invalid file format.', 'type': 'error'})
    expect(wrapper.vm.form.fileList).toEqual([])
    expect(wrapper.vm.form.file).toBeNull()

    expect(wrapper.vm.isFormEdited(wrapper.vm.form, 'defaultDB-settings')).toBeTruthy()
    expect(wrapper.vm.isFormEdited(wrapper.vm.form, 'job-alert')).toBeFalsy()
    expect(wrapper.vm.isFormEdited(wrapper.vm.form, 'yarn-name')).toBeFalsy()
    expect(wrapper.vm.isFormEdited(wrapper.vm.form, 'kerberos-acc')).toBeFalsy()

    await wrapper.vm.handleScdSetting(true)
    expect(mockMsgBox.mock.calls[0][0].title).toBe('Turn On Support History table')
    expect(mockApi.mockToggleEnableSCD.mock.calls[0][1]).toEqual({"project": "kyligence", "scd2_enabled": true})
    expect(mockHandleSuccessAsync).toBeCalled()

    await wrapper.vm.handleScdSetting(false)
    jest.runAllTimers()
    expect(mockApi.mockGetSCD2Model.mock.calls[0][1]).toEqual({"project": "kyligence"})
    expect(mockHandleSuccessAsync).toBeCalledWith(["SCD_MODELS"])
    expect(mockMsgBox.mock.calls[0][0].title).toBe('Turn On Support History table')
    expect(mockApi.mockToggleEnableSCD.mock.calls[0][1]).toEqual({"project": "kyligence", "scd2_enabled": true})
  })
})

describe('Component SettingAdvanced handler', () => {
  it('validate', () => {
    const callback = jest.fn()
    handler.validate.validateYarnName({type: 'boolean', message: ['The queue name is required', 'Incorrect format']}, 'SSB', callback)
    expect(callback).toBeCalledWith('Incorrect format')
    handler.validate.validateYarnName({type: 'boolean', message: ['The queue name is required', 'Incorrect format']}, '', callback)
    expect(callback).toBeCalledWith('The queue name is required')
    handler.validate.validateYarnName({type: 'string', message: ['The queue name is required', 'Incorrect format']}, 'SSB', callback)
    expect(callback).toBeCalledWith()

    handler.validate.positiveNumber({}, '', callback)
    expect(callback).toBeCalled()
    handler.validate.positiveNumber({}, '22', callback)
    expect(callback).toBeCalledWith()

    handler.validate.principalName({}, '', callback)
    expect(callback).toBeCalled()
    handler.validate.principalName({}, 'name', callback)
    expect(callback).toBeCalledWith()
  })
})
