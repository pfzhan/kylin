import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import DataSourceModal from '../../DataSourceModal/index.vue'
import Vuex from 'vuex'
import DataSourceModalStore from '../../DataSourceModal/store'
import * as util from '../../../../util'
import * as business from '../../../../util/business'
import { states } from './mock'
import mock from '../../../../../mock'

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
  mockUpdateProjectDatasource: jest.fn().mockImplementation()
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
    'UPDATE_PROJECT_DATASOURCE': mockApi.mockUpdateProjectDatasource
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
  }
}

describe('Component DataSourceModal', () => {
  it('computed', async () => {
    expect(wrapper.vm.modalTitle).toBe('cloudHive')
    wrapper.vm.$store.state.config.platform = ''
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.modalTitle).toBe('loadhiveTables')

    expect(wrapper.vm.modelWidth).toBe('960px')
    wrapper.vm.$store.state.DataSourceModal.editType = 'selectSource'
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.modelWidth).toBe('480px')
    wrapper.vm.$store.state.DataSourceModal.editType = 'configSource'
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.modelWidth).toBe('780px')

    expect(wrapper.vm.confirmText).toBe('Next')
    expect(wrapper.vm.cancelText).toBe('Prev')
    wrapper.vm.$store.state.DataSourceModal.editType = 9
    expect(wrapper.vm.cancelText).toBe('Cancel')
    expect(wrapper.vm.sourceType).toBe(9)
    expect(wrapper.vm.sourceHive).toBeTruthy()
  })
  it('methods', async () => {
    wrapper.vm.lockStep(true)
    expect(wrapper.vm.stepLocked).toBeTruthy()
    wrapper.vm.handleInput('settings.description', 'test')
    expect(wrapper.vm.$store.state.DataSourceModal.form.settings.description).toBe('test')
    wrapper.vm.handleInputTableOrDatabase({selectedTables: ['DB1']})
    expect(wrapper.vm.$store.state.DataSourceModal.form.selectedTables).toEqual(['DB1'])
    wrapper.vm.handleInputDatasource(9)
    expect(wrapper.vm.$store.state.DataSourceModal.form.project.override_kylin_properties['kylin.source.default']).toBe(9)
    wrapper.vm.handleOpen()
    expect(wrapper.vm.$store.state.DataSourceModal.isShow).toBeTruthy()

    await wrapper.vm.handleSubmit()
    expect(wrapper.vm.prevSteps).toEqual([])
    expect(mockApi.mockImportTable.mock.calls[0][1]).toEqual({"data_source_type": 9, "databases": [], "need_sampling": true, "project": "xm_test", "sampling_rows": 20000000, "tables": ["DB1"]})
    expect(mockHandleSuccessAsync).toBeCalled()

    wrapper.vm.$store.state.DataSourceModal.form.samplingRows = 100
    wrapper.vm.$store.state.DataSourceModal.form.selectedTables = []
    await wrapper.vm.$nextTick()
    await wrapper.vm.handleSubmit()
    expect(mockMessage).toBeCalledWith('Please select databases or tables.')

    wrapper.vm.$store.state.DataSourceModal.editType = 'selectSource'
    await wrapper.vm.$nextTick()
    await wrapper.vm.handleSubmit()
    expect(mockApi.mockUpdateProjectDatasource.mock.calls[0][1]).toEqual({"project": "xm_test", "source_type": "9"})

    wrapper.vm.$store.state.DataSourceModal.editType = 'configCsvStructure'
    await wrapper.vm.$nextTick()
    await wrapper.vm.handleSubmit()
    expect(mockApi.mockSaveCsvDataSourceInfo.mock.calls[0][1]).toEqual({"data": {"credential": "{}", "project": "xm_test", "tableData": undefined}, "type": "guide"})
    expect(mockHandleSuccessAsync).toBeCalled()

    wrapper.vm.$store.state.DataSourceModal.editType = 'configCsvSql'
    await wrapper.vm.$nextTick()
    await wrapper.vm.handleSubmit()
    expect(mockApi.mockSaveCsvDataSourceInfo.mock.calls[1][1]).toEqual({"data": {"credential": "{}", "project": "xm_test"}, "type": "expert"})
    expect(mockHandleSuccessAsync).toBeCalled()

    wrapper.vm.handleCancel()
    expect(wrapper.vm.prevSteps).toEqual(["selectSource", "configCsvStructure"])

    wrapper.vm.$store.state.DataSourceModal.prevSteps = [9, 'selectSource']
    await wrapper.vm.$nextTick()
    wrapper.vm.handleCancel()
    expect(wrapper.vm.$store.state.DataSourceModal.editType).toBe('configCsvStructure')

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
  })
  it('store', () => {
    DataSourceModalStore.actions['CALL_MODAL'](root, {editType: 9, project: {}, datasource: undefined, databaseSizeObj: null })
    expect(DataSourceModalStore.state.isShow).toBeTruthy()
  })
})
