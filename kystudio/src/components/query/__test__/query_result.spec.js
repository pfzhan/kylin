import { shallow, mount } from 'vue-test-utils'
import Vuex from 'vuex'
import { localVue } from '../../../../test/common/spec_common'
import queryResult from '../query_result'
import { extraoptions, queryExportData } from './mock'

jest.useFakeTimers()

const mockApi = {
  mockQueryBuildTables: jest.fn().mockImplementation(() => {
    return Promise.resolve()
  }),
  mockExportCsv: jest.fn().mockImplementation(() => {
    return Promise.resolve({
      status: 200,
      body: 'd_datekey,d_date,d_dayofweek',
      headers: {
        map: {
          'content-disposition': ['attachment; filename="20200928050549357.result.csv"']
        }
      }
    })
  })
}

const store = new Vuex.Store({
  state: {
    system: {
      allowNotAdminExport: 'true'
    },
    user: {
      currentUser: {
        authorities: [{authority: "ROLE_ADMIN"}],
        create_time: 1600148965832,
        defaultPassword: true,
        disabled: false,
        first_login_failed_time: 0,
        last_modified: 1601214213365,
        locked: false,
        locked_time: 0,
        mvcc: 97,
        username: "ADMIN",
        uuid: "2400ccc1-8d17-44f9-bb5a-74def5286953",
        version: "4.0.0.0",
        wrong_time: 0
      }
    },
    config: {
      platform: 'iframe'
    }
  },
  actions: {
    QUERY_BUILD_TABLES: mockApi.mockQueryBuildTables,
    EXPORT_CSV: mockApi.mockExportCsv
  },
  getters: {
    currentSelectedProject () {
      return 'xm_test'
    },
    insightActions () {
      return ['viewAppMasterURL']
    }
  }
})

const _refs = {
  tableLayout: {
    bodyWrapper: {
      addEventListener: jest.fn(),
      removeEventListener: jest.fn()
    },
    scrollPosition: 'right'
  }
}

const wrapper = mount(queryResult, {
  localVue,
  store,
  propsData: {
    extraoption: extraoptions,
    isWorkspace: true,
    queryExportData,
    isStop: false
  },
  mocks: {
  }
})
wrapper.vm.$refs = _refs

const mockSaveBlob = jest.fn()
const mockRevokeObjectURL = jest.fn().mockImplementation()

global.window.navigator.msSaveOrOpenBlob = false
global.window.URL = {
  revokeObjectURL: mockRevokeObjectURL,
  createObjectURL: jest.fn().mockImplementation(() => {
    return 'blob:1234567890'
  })
}
global.navigator.msSaveBlob = mockSaveBlob


describe('Component queryResult', () => {
  it('init', () => {
    // expect(wrapper.vm.$refs.tableLayout.bodyWrapper.addEventListener).toBeCalled()
    expect(wrapper.vm.tableMeta).toEqual([{"autoIncrement": false, "caseSensitive": false, "catelogName": null, "columnType": 91, "columnTypeName": "DATE", "currency": false, "definitelyWritable": false, "displaySize": 2147483647, "isNullable": 1, "label": "d_datekey", "name": "d_datekey", "precision": 0, "readOnly": false, "scale": 0, "schemaName": null, "searchable": false, "signed": true, "tableName": null, "writable": false}])
    expect(wrapper.vm.currentPage).toBe(0)
    expect(wrapper.vm.tableData).toEqual([["1992-01-01"]])
    expect(wrapper.vm.pagerTableData).toEqual([["1992-01-01"]])
  })
  it('computed', async () => {
    expect(wrapper.vm.showExportCondition).toBeFalsy()
    expect(wrapper.vm.isAdmin).toBeTruthy()
    expect(wrapper.vm.answeredBy).toEqual('HIVE')
    expect(wrapper.vm.layoutIds).toBe('')

    extraoptions.realizations = [{
      modelAlias: 'model_test',
      layoutId: -1
    }]
    wrapper.setProps({extraoption: extraoptions})
    await wrapper.update()
    expect(wrapper.vm.answeredBy).toBe('model_test')
    expect(wrapper.vm.layoutIds).toBe('Snapshot')

    extraoptions.realizations = [{
      modelAlias: 'model_test_1',
      layoutId: 5
    }]
    wrapper.setProps({extraoption: extraoptions})
    await wrapper.update()
    expect(wrapper.vm.layoutIds).toBe('5')

    expect(wrapper.vm.isShowNotModelRangeTips).toBeFalsy()

    extraoptions.realizations = [{
      modelAlias: 'model_test_1',
      layoutId: -1
    }, {
      modelAlias: 'model_test',
      layoutId: null
    }]
    wrapper.setProps({extraoption: extraoptions})
    await wrapper.update()
    expect(wrapper.vm.isShowNotModelRangeTips).toBeTruthy()
  })
  it('methods', async () => {
    wrapper.vm.transDataForGrid()
    expect(wrapper.vm.tableMeta).toEqual([{"autoIncrement": false, "caseSensitive": false, "catelogName": null, "columnType": 91, "columnTypeName": "DATE", "currency": false, "definitelyWritable": false, "displaySize": 2147483647, "isNullable": 1, "label": "d_datekey", "name": "d_datekey", "precision": 0, "readOnly": false, "scale": 0, "schemaName": null, "searchable": false, "signed": true, "tableName": null, "writable": false}, {"autoIncrement": false, "caseSensitive": false, "catelogName": null, "columnType": 91, "columnTypeName": "DATE", "currency": false, "definitelyWritable": false, "displaySize": 2147483647, "isNullable": 1, "label": "d_datekey", "name": "d_datekey", "precision": 0, "readOnly": false, "scale": 0, "schemaName": null, "searchable": false, "signed": true, "tableName": null, "writable": false}])
    
    wrapper.vm.toggleDetail()
    expect(wrapper.vm.showDetail).toBeTruthy()

    expect(wrapper.vm.filterTableData()).toEqual([["1992-01-01"]])
    wrapper.setData({resultFilter: 'test'})
    expect(wrapper.vm.filterTableData()).toEqual([])

    wrapper.vm.getMoreData()
    expect(wrapper.vm.pageX).toBe(1)

    wrapper.vm.pageSizeChange(0, 20)
    expect(wrapper.vm.pageSize).toBe(20)
    expect(wrapper.vm.pagerTableData).toEqual([])

    wrapper.setData({
      resultFilter: ''
    })
    jest.runAllTimers()
    expect(wrapper.vm.currentPage).toBe(0)

    await wrapper.vm.exportData()
    expect(mockApi.mockExportCsv.mock.calls[0][1]).toEqual({"limit": 500, "project": "xm_test", "sql": "select * from SSB.DATES"})
    // expect(mockRevokeObjectURL).toBeCalled()

    global.window.navigator.msSaveOrOpenBlob = true
    await wrapper.vm.exportData()
    expect(mockApi.mockExportCsv.mock.calls[0][1]).toEqual({"limit": 500, "project": "xm_test", "sql": "select * from SSB.DATES"})
    expect(mockSaveBlob).toBeCalled()

    wrapper.vm.$store._actions.EXPORT_CSV = [jest.fn().mockImplementation(() => {
      return {
        then: (callback, errorCallback) => {
          errorCallback()
        }
      }
    })]
    await wrapper.vm.exportData()
    expect(wrapper.vm.hasClickExportBtn).toBeFalsy()

    wrapper.vm.$el.querySelectorAll = jest.fn().mockReturnValue([])
    wrapper.vm.$store.state.config.platform = ''
    await wrapper.update()
    await wrapper.vm.exportData()
    expect(wrapper.vm.hasClickExportBtn).toBeFalsy()

    wrapper.vm.$options.methods.beforeDestory.call(wrapper.vm)
    // console.log(wrapper.vm.$options)
    expect(wrapper.vm.$refs.tableLayout.bodyWrapper.removeEventListener).toBeCalled()
  })
})
