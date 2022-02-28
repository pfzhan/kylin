import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import DataSourceModal from '../../DataSourceModal/index.vue'
import Vuex from 'vuex'
import DataSourceModalStore from '../../DataSourceModal/store'
import * as util from '../../../../util'
import * as business from '../../../../util/business'
import { states, kafkaMeta } from './mock'

const mockHandleError = jest.spyOn(business, 'handleError').mockImplementation(() => {})
const mockHandleSuccessAsync = jest.spyOn(util, 'handleSuccessAsync').mockImplementation((res, callback) => {
  return new Promise((resolve) => (resolve(res.data || res)))
})
const mockMessage = jest.fn().mockImplementation()

const root = {
  state: DataSourceModalStore.state,
  commit: function (name, params) { DataSourceModalStore.mutations[name](root.state, params) },
  dispatch: function (name, params) { return DataSourceModalStore.actions[name]({state: root.state, commit: root.commit, dispatch: root.dispatch}, params) }
}

const mockApi = {
  mockLoadAllProject: jest.fn().mockImplementation(),
  mockUpdateProject: jest.fn().mockImplementation(),
  mockImportTable: jest.fn().mockImplementation(() => {
    return Promise.resolve(true)
  }),
  mockSaveCsvDataSourceInfo: jest.fn().mockImplementation(),
  mockSaveSourceConfig: jest.fn().mockImplementation(),
  mockUpdateProjectDatasource: jest.fn().mockImplementation(),
  mockSaveKafka: jest.fn().mockResolvedValue({code: '000', data: {loaded: ['ABC.X'], failed: [], sourceType: 1}}),
  convertTopicJson: jest.fn().mockResolvedValue({body: {code: '000', data: {lo_commitdate: '19980916', lo_custkey: 225482, lo_discount: 2}, msg: ''}})
}

const dataSourceModal = {
  ...DataSourceModalStore,
  ...{
    state: states
  }
}

const store = new Vuex.Store({
  state: {
    config: {
      platform: 'iframe'
    }
  },
  actions: {
    'LOAD_ALL_PROJECT': mockApi.mockLoadAllProject,
    'UPDATE_PROJECT': mockApi.mockUpdateProject,
    'LOAD_HIVE_IN_PROJECT': mockApi.mockImportTable,
    'SAVE_CSV_INFO': mockApi.mockSaveCsvDataSourceInfo,
    'SAVE_SOURCE_CONFIG': mockApi.mockSaveSourceConfig,
    'UPDATE_PROJECT_DATASOURCE': mockApi.mockUpdateProjectDatasource,
    'SAVE_KAFKA': mockApi.mockSaveKafka,
    'CONVERT_TOPIC_JSON': mockApi.convertTopicJson
  },
  modules: {
    DataSourceModal: dataSourceModal
  }
})

const wrapper = shallowMount(DataSourceModal, {
  store,
  localVue,
  mocks: {
    handleError: mockHandleError,
    handleSuccessAsync: mockHandleSuccessAsync,
    $message: mockMessage
  }
})

wrapper.vm.$refs = {
  'kafka-form2': {
    $refs: {
      kafkaForm: {
        validate: jest.fn().mockResolvedValue(true)
      }
    }
  },
  'source-csv-structure-form': {
    $refs: {
      form: {
        validate: jest.fn().mockResolvedValue(true)
      }
    }
  },
  'source-csv-sql-form': {
    $refs: {
      form: {
        validate: jest.fn().mockResolvedValue(true)
      }
    }
  },
  'source-csv-setting-form': {
    $refs: {
      form: {
        validate: jest.fn().mockResolvedValue(true)
      }
    }
  },
  'source-csv-connection-form': {
    $refs: {
      form: {
        validate: jest.fn().mockResolvedValue(true)
      }
    }
  },
  'source-hive-form': {
    $emit: jest.fn()
  }
}

describe('Component DataSourceModal', () => {
  it('computed', async () => {
    expect(wrapper.vm.modalTitle).toBe('cloudHive')
    wrapper.vm.$store.state.config.platform = ''
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.modalTitle).toBe('loadTables')

    expect(wrapper.vm.modelWidth).toBe('960px')
    wrapper.vm.$store.state.DataSourceModal.editType = 'selectSource'
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.modelWidth).toBe('600px')
    wrapper.vm.$store.state.DataSourceModal.editType = 'configSource'
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.modelWidth).toBe('780px')

    expect(wrapper.vm.confirmText).toBe('Next')
    expect(wrapper.vm.cancelText).toBe('Prev')
    wrapper.vm.$store.state.DataSourceModal.editType = 9
    expect(wrapper.vm.cancelText).toBe('Cancel')
    expect(wrapper.vm.sourceType).toBe(9)
    expect(wrapper.vm.sourceHive).toBeTruthy()
    expect(wrapper.vm.sourceKafka).toBeFalsy()
    expect(wrapper.vm.sourceKafkaStep2).toBeFalsy()
  })
  it('methods', async () => {
    wrapper.vm.lockStep(true)
    expect(wrapper.vm.stepLocked).toBeTruthy()
    wrapper.vm.handleInput('settings.description', 'test')
    expect(wrapper.vm.$store.state.DataSourceModal.form.settings.description).toBe('test')
    wrapper.vm.handleInputTableOrDatabase({selectedTables: ['DB1']})
    expect(wrapper.vm.$store.state.DataSourceModal.form.selectedTables).toEqual(['DB1'])
    wrapper.vm.handleInputKafkaData({kafkaMeta})
    expect(wrapper.vm.$store.state.DataSourceModal.form.kafkaMeta).not.toBeNull()
    wrapper.vm.handleInputDatasource(9)
    expect(wrapper.vm.$store.state.DataSourceModal.form.project.override_kylin_properties['kylin.source.default']).toBe(9)
    wrapper.vm.handleOpen()
    expect(wrapper.vm.$store.state.DataSourceModal.isShow).toBeTruthy()

    await wrapper.vm.handleSubmit()
    expect(wrapper.vm.prevSteps).toEqual([])
    expect(mockApi.mockImportTable.mock.calls[0][1]).toEqual({'data_source_type': 9, 'databases': [], 'need_sampling': true, 'project': 'xm_test', 'sampling_rows': 20000000, 'tables': ['DB1']})
    expect(mockHandleSuccessAsync).toBeCalled()

    wrapper.vm.$store.state.DataSourceModal.form.samplingRows = 100
    wrapper.vm.$store.state.DataSourceModal.form.selectedTables = []
    await wrapper.vm.$nextTick()
    await wrapper.vm.handleSubmit()
    expect(mockMessage).toBeCalledWith('Please select databases or tables.')

    wrapper.vm.$store.state.DataSourceModal.editType = 'selectSource'
    await wrapper.vm.$nextTick()
    await wrapper.vm.handleSubmit()
    expect(mockApi.mockUpdateProjectDatasource.mock.calls[0][1]).toEqual({'project': 'xm_test', 'source_type': '9'})

    wrapper.vm.$store.state.DataSourceModal.editType = 'configCsvStructure'
    await wrapper.vm.$nextTick()
    await wrapper.vm.handleSubmit()
    expect(mockApi.mockSaveCsvDataSourceInfo.mock.calls[0][1]).toEqual({'data': {'credential': '{}', 'project': 'xm_test', 'tableData': undefined}, 'type': 'guide'})
    expect(mockHandleSuccessAsync).toBeCalled()

    wrapper.vm.$store.state.DataSourceModal.editType = 'configCsvSql'
    await wrapper.vm.$nextTick()
    await wrapper.vm.handleSubmit()
    expect(mockApi.mockSaveCsvDataSourceInfo.mock.calls[1][1]).toEqual({'data': {'credential': '{}', 'project': 'xm_test'}, 'type': 'expert'})
    expect(mockHandleSuccessAsync).toBeCalled()

    wrapper.vm.handleCancel()
    expect(wrapper.vm.prevSteps).toEqual([])

    wrapper.vm.$store.state.DataSourceModal.prevSteps = [9, 'selectSource']
    await wrapper.vm.$nextTick()

    wrapper.vm.handleCancel()
    expect(wrapper.vm.$store.state.DataSourceModal.editType).toBe('selectSource')

    wrapper.vm.$store.state.DataSourceModal.prevSteps = []
    await wrapper.vm.$nextTick()
    wrapper.vm.handleCancel()
    expect(wrapper.vm.isLoading).toBeFalsy()
    expect(wrapper.vm.isDisabled).toBeFalsy()
    expect(wrapper.vm.prevSteps).toEqual([])

    wrapper.vm.handleClose()
    expect(wrapper.vm.isLoading).toBeFalsy()
    expect(wrapper.vm.isDisabled).toBeFalsy()
    expect(wrapper.vm.prevSteps).toEqual([])
    wrapper.vm.handleClosed()
    expect(wrapper.vm.$store.state.DataSourceModal.isShow).toBeFalsy()

    wrapper.vm._hideForm()
    expect(wrapper.vm.isFormShow).toBeFalsy()
    wrapper.vm._showForm()
    expect(wrapper.vm.isFormShow).toBeTruthy()
    wrapper.vm._hideLoading()
    expect(wrapper.vm.isLoading).toBeFalsy()
    expect(wrapper.vm.isDisabled).toBeFalsy()
    wrapper.vm._showLoading()
    expect(wrapper.vm.isLoading).toBeTruthy()
    expect(wrapper.vm.isDisabled).toBeTruthy()
  })
  it('datasource submit', async () => {
    states.form.project.override_kylin_properties['kylin.source.default'] = 9
    wrapper.vm.$store.state.DataSourceModal.form = {...states.form}
    await wrapper.vm.$nextTick()
    await wrapper.vm._submit()
    expect(wrapper.vm.$store.state.DataSourceModal.editType).toBe(9)

    wrapper.vm.$store.state.DataSourceModal.editType = 'viewSource'
    await wrapper.vm.$nextTick()
    await wrapper.vm._submit()
    expect(wrapper.vm.prevSteps).toEqual([])
  })
  it('validate', async () => {
    wrapper.vm.$store.state.DataSourceModal.editType = 'kafkaStep2'
    wrapper.vm.$store.state.DataSourceModal.form.kafkaMeta = kafkaMeta
    expect(await wrapper.vm._validate()).toBe()
    expect(mockMessage.mock.calls[1]).toEqual(['Can\'t load. Please ensure the table has at least a timestamp column.'])

    wrapper.vm.$refs['kafka-form2'] = {
      $refs: {
        kafkaForm: {
          validate: jest.fn().mockResolvedValue(true)
        }
      }
    }
    wrapper.vm.$store.state.DataSourceModal.form.kafkaMeta = {...kafkaMeta, isShowHiveTree: true}
    expect(await wrapper.vm._validate()).toBe()
    expect(mockMessage.mock.calls[2]).toEqual(['Please select an attached Hive table.'])

    kafkaMeta.table_desc.columns = [
      {
        attribute: 'lo_partitioncolumn',
        case_sensitive_name: null,
        checked: 'Y',
        comment: '',
        datatype: 'date',
        fromSource: 'Y',
        guid: '1645096164618_010739947305753628',
        id: 10,
        name: 'LO_PARTITIONCOLUMN',
        value: '2022-02-17 16:02:00'
      }
    ]

    wrapper.vm.$store.state.DataSourceModal.form.kafkaMeta = kafkaMeta
    expect(await wrapper.vm._validate()).toBe()
    expect(mockMessage.mock.calls[2]).toEqual(['Please select an attached Hive table.'])

    kafkaMeta.batchTableColumns = [
      {
        cardinality: 5,
        case_sensitive_name: 'c_region',
        datatype: 'varchar(4096)',
        id: '6',
        max_value: 'MIDDLE EAST',
        min_value: 'AFRICA',
        name: 'C_REGION',
        null_count: 0
      }
    ]
    kafkaMeta.kafka_config.has_shadow_table = true
    kafkaMeta.isShowHiveTree = true
    wrapper.vm.$store.state.DataSourceModal.form.kafkaMeta = kafkaMeta
    expect(await wrapper.vm._validate()).toBe()
    expect(mockMessage.mock.calls[3]).toEqual(['Can\'t load. Please ensure the table has at least a timestamp column.'])

    kafkaMeta.table_desc.columns.splice(1, 0,
      {
        attribute: 'C_CUSTOM',
        case_sensitive_name: null,
        checked: 'Y',
        comment: '',
        datatype: 'date',
        fromSource: 'Y',
        guid: '1645096164618_010739947305753628',
        id: 10,
        name: 'C_CUSTOM',
        value: '2022-02-17 16:02:00'
      }
    )
    kafkaMeta.batchTableColumns.splice(1, 0,
      {
        cardinality: 5,
        case_sensitive_name: 'C_CUSTOM',
        datatype: 'varchar(4096)',
        id: '7',
        max_value: 'MIDDLE EAST',
        min_value: 'AFRICA',
        name: 'C_CUSTOM',
        null_count: 0
      }
    )
    wrapper.vm.$store.state.DataSourceModal.form.kafkaMeta = kafkaMeta
    expect(await wrapper.vm._validate()).toBe()
    expect(mockMessage.mock.calls[4]).toEqual(['Can\'t attach. Please ensure that the columns of the Kafka table and the Hive table are identical.'])

    kafkaMeta.batchTableColumns.splice(0, 1,
      {
        cardinality: 5,
        case_sensitive_name: 'LO_PARTITIONCOLUMN',
        datatype: 'varchar(4096)',
        id: '7',
        max_value: 'MIDDLE EAST',
        min_value: 'AFRICA',
        name: 'LO_PARTITIONCOLUMN',
        null_count: 0
      }
    )
    wrapper.vm.$store.state.DataSourceModal.form.kafkaMeta = kafkaMeta
    expect(await wrapper.vm._validate()).toBe()
    expect(mockMessage.mock.calls[5]).toEqual(['Can\'t attach. Please ensure that the columns of the Kafka table and the Hive table are identical.'])

    kafkaMeta.isShowHiveTree = false
    kafkaMeta.table_desc.columns.splice(1, 0,
      {
        attribute: 'lo_partitioncolumn',
        case_sensitive_name: null,
        checked: 'Y',
        comment: '',
        datatype: 'timestamp',
        fromSource: 'Y',
        guid: '1645096164618_010739947305753628',
        id: 10,
        name: 'LO_PARTITIONCOLUMN',
        value: '2022-02-17 16:02:00'
      }
    )
    wrapper.vm.$store.state.DataSourceModal.form.kafkaMeta = kafkaMeta
    expect(await wrapper.vm._validate()).toBeTruthy()

    kafkaMeta.isShowHiveTree = true
    kafkaMeta.table_desc.columns = []
    wrapper.vm.$store.state.DataSourceModal.form.kafkaMeta = kafkaMeta
    expect(await wrapper.vm._validate()).toBe()
    expect(mockMessage.mock.calls[7]).toEqual(['Can\'t attach. Please ensure that the columns of the Kafka table and the Hive table are identical.'])

    wrapper.vm.$store.state.DataSourceModal.editType = 'jdbc'
    expect(await wrapper.vm._validate()).toBeTruthy()

    wrapper.vm.$store.state.DataSourceModal.editType = 1
    expect(await wrapper.vm._validate()).toBeNull()
    expect(mockMessage.mock.calls[8]).toEqual(['Please get the Kafka cluster and topic information.'])

    wrapper.vm.$store.state.DataSourceModal.editType = 'configCsvSetting'
    expect(await wrapper.vm._validate()).toBeTruthy()

    wrapper.vm.$store.state.DataSourceModal.editType = 13
    expect(await wrapper.vm._validate()).toBeTruthy()

    wrapper.vm.$refs['source-hive-setting-form'] = {
      $refs: {
        form: {
          validate: jest.fn().mockResolvedValue(true)
        }
      }
    }
    wrapper.vm.$store.state.DataSourceModal.editType = 'configSource'
    expect(await wrapper.vm._validate()).toBeTruthy()
  })
  it('validate fail', async () => {
    wrapper.vm.$store.state.DataSourceModal.editType = 'kafkaStep2'
    wrapper.vm.$refs['kafka-form2'] = {
      $refs: {
        kafkaForm: {
          validate: jest.fn().mockResolvedValue(false)
        }
      }
    }
    expect(await wrapper.vm._validate()).toBe()
    expect(mockMessage.mock.calls[9]).toEqual()
  })
  it('submit events', async () => {
    wrapper.vm.$store.state.DataSourceModal.editType = 'kafkaStep2'
    expect(await wrapper.vm._submit()).toEqual({'failed': [], 'loaded': ['ABC.X'], 'sourceType': 1})

    kafkaMeta.isShowHiveTree = false
    await wrapper.vm._submit()
    expect(mockApi.mockSaveKafka.mock.calls[1][1].kafka_config.has_shadow_table).toBeFalsy()

    wrapper.vm.$store.state.DataSourceModal.editType = 1
    expect(await wrapper.vm._submit()).toEqual()

    wrapper.vm.$store.state.DataSourceModal.editType = 'configCsvSetting'
    await wrapper.vm._submit()
    expect(wrapper.vm.editType).toBe('configCsvStructure')

    wrapper.vm.$store.state.DataSourceModal.editType = 13
    await wrapper.vm._submit()
    expect(wrapper.vm.editType).toBe('configCsvSetting')

    wrapper.vm.$store.state.DataSourceModal.form.csvSettings.addTableType = 1
    wrapper.vm.$store.state.DataSourceModal.editType = 13
    await wrapper.vm._submit()
    expect(wrapper.vm.editType).toBe('configCsvSql')
  })
  it('store', () => {
    DataSourceModalStore.actions['CALL_MODAL'](root, { editType: 9, project: {}, datasource: undefined, databaseSizeObj: null })
    expect(DataSourceModalStore.state.isShow).toBeTruthy()

    DataSourceModalStore.mutations['UPDATE_JDBC_CONFIG'](DataSourceModalStore.state, 8)
    expect(DataSourceModalStore.state.form.connectGbaseSetting).toEqual({'connectionString': '', 'password': '', 'synced': false, 'username': ''})
    // console.log(states)
    DataSourceModalStore.mutations['UPDATE_JDBC_CONFIG']({...states}, 8)
    expect(DataSourceModalStore.state.form.connectGbaseSetting).toEqual({'connectionString': '', 'password': '', 'synced': false, 'username': ''})
  })
})
