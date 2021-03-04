import { shallowMount, mount } from '@vue/test-utils'
import { localVue } from '../../../../../../test/common/spec_common'
import SourceHive from '../../../DataSourceModal/SourceHive/SourceHive.vue'
import areaLabel from '../../../../common/area_label.vue'
import * as util from '../../../../../util'
import * as business from '../../../../../util/business'
import Vuex from 'vuex'

const mockHandleError = jest.spyOn(business, 'handleError').mockImplementation(() => {})
const mockHandleSuccessAsync = jest.spyOn(util, 'handleSuccessAsync').mockImplementation((res, callback) => {
  return new Promise((resolve) => (resolve(res.data || res)))
})
const mockMessage = jest.fn().mockImplementation()

const mockApi = {
  loadHiveBasicDatabase: jest.fn().mockImplementation(),
  loadHiveTables: jest.fn().mockImplementation(),
  loadHiveBasicDatabaseTables: jest.fn().mockImplementation(() => {
    return Promise.resolve({
      databases: [
        {dbname: "BI", size: 2, tables: [{table_name: "TEST_DATA_TYPES", loaded: false}, {table_name: "TEST_MAP", loaded: false}]},
        {dbname: "DEFAULT", size: 111, tables: [{table_name: "BITMAP_TABLE", loaded: false}, {table_name: "CALCS", loaded: true}, {table_name: "CG", loaded: false}]}
      ]
    })
  }),
  reloadHiveDBTables: jest.fn().mockImplementation(() => {
    return {
      then: (callback, errorCallback) => {
        callback()
      }
    }
  })
}

const store = new Vuex.Store({
  state: {
    system: {
      loadHiveTableNameEnabled: true,
      lang: 'zh-cn',
      config: {
        platform: 'pc'
      }
    }
  },
  actions: {
    LOAD_HIVEBASIC_DATABASE: mockApi.loadHiveBasicDatabase,
    LOAD_HIVE_TABLES: mockApi.loadHiveTables,
    LOAD_HIVEBASIC_DATABASE_TABLES: mockApi.loadHiveBasicDatabaseTables,
    RELOAD_HIVE_DB_TABLES: mockApi.reloadHiveDBTables
  },
  getters: {
    currentSelectedProject: () => {
      return 'kyligence'
    }
  }
})

const wrapper = shallowMount(SourceHive, {
  store,
  localVue,
  propsData: {
    selectedTables: ['CALCS'],
    selectedDatabases: ['SSB'],
    needSampling: false,
    sourceType: 9
  },
  mocks: {
    handleError: mockHandleError,
    handleSuccessAsync: mockHandleSuccessAsync,
    $message: mockMessage
  }
  // components: {
  //   arealabel
  // }
})
wrapper.vm.$refs = {
  'tree-list': {
    showLoading: jest.fn().mockResolvedValue(true)
  }
}

describe('Component SourceHive', () => {
  it('init', () => {
    // expect(wrapper.vm.$refs['tree-list'].showLoading).toBeCalled()
    // expect(mockApi.loadHiveBasicDatabaseTables.mock.calls[0][1]).toEqual()
    // expect(wrapper.vm.treeKey).toEqual()
    // expect(wrapper.vm.treeData).toEqual()
  })
})


