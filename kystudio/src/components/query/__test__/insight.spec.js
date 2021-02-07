import { shallow, mount } from 'vue-test-utils'
import { localVue } from '../../../../test/common/spec_common'
import Insight from '../insight'
import Vuex from 'vuex'
import Vue from 'vue'
import VueI18n from 'vue-i18n'
import * as mockData from './mock'
import * as bussiness from '../../../util/business'
import DataSourceBar from '../../common/DataSourceBar'
import tab from '../../common/tab'
import queryTab from '../query_tab'

Vue.use(VueI18n)
jest.useFakeTimers()

const mockHandleSuccess = jest.spyOn(bussiness, 'handleSuccess').mockImplementation((res, callback) => {
  callback && callback(res)
})
const mockHandleError = jest.spyOn(bussiness, 'handleError').mockRejectedValue(false)
const mockKapConfirm = jest.spyOn(bussiness, 'kapConfirm').mockResolvedValue(true)
const mockMessage = jest.fn().mockImplementation()

const mockApi = {
  mockGetSaveQueries: jest.fn().mockImplementation(() => {
    return Promise.resolve({
      total_size: 1,
      limit: 10,
      offset: 0,
      value: [{
        description: "",
        id: "-1317973139",
        name: "_fwewfe",
        project: "xm_test",
        sql: "select * from SSB.DATES_VIEW"
      }]
    })
  }),
  mockDeleteQuery: jest.fn().mockResolvedValue(true),
  mockLoadDataSource: jest.fn().mockImplementation(),
  mockQueryBuildTables: jest.fn().mockImplementation(),
  mockStopQueryBuild: jest.fn().mockImplementation(),
  mockSetQueryTabs: jest.fn().mockImplementation()
}

const store = new Vuex.Store({
  state: {
    config: {
      platform: 'pc'
    },
    system: {
      showHtrace: 'false',
      isShowGlobalAlter: false
    },
    user: {
      currentUser: {
        authorities: [{authority: "ROLE_ADMIN"}, {authority: "ALL_USERS"}],
        create_time: 1600148965832,
        defaultPassword: true,
        disabled: false,
        first_login_failed_time: 0,
        last_modified: 1600689322444,
        locked: false,
        locked_time: 0,
        mvcc: 50,
        username: "ADMIN",
        uuid: "2400ccc1-8d17-44f9-bb5a-74def5286953",
        version: "4.0.0.0",
        wrong_time: 0
      }
    }
  },
  actions: {
    GET_SAVE_QUERIES: mockApi.mockGetSaveQueries,
    DELETE_QUERY: mockApi.mockDeleteQuery,
    LOAD_DATASOURCE: mockApi.mockLoadDataSource,
    QUERY_BUILD_TABLES: mockApi.mockQueryBuildTables,
    STOP_QUERY_BUILD: mockApi.mockStopQueryBuild,
    FETCH_DB_AND_TABLES: jest.fn()
  },
  mutations: {
    SET_QUERY_TABS: mockApi.mockSetQueryTabs
  },
  getters: {
    currentSelectedProject: () => 'Kyligence',
    getQueryTabs: () => {},
    datasourceActions: () => ['loadSource'],
    currentProjectData () {
      return {
        override_kylin_properties: {
          'kylin.source.default': '9'
        }
      }
    },
    isProjectAdmin: () => true,
    isAdminRole: () => true
  }
})

const dataSourceBarComp = shallow(DataSourceBar, { localVue, store })
const tabComp = shallow(tab)
const queryTabComp = shallow(queryTab, { localVue, store, propsData: {tabsItem: {}, completeData: {}, tipsName: ''}, mocks: {
  $refs: {
    insightBox: {
      $emit: jest.fn()
    }
  }
} })
// queryTabComp.vm.$store.state = {}

const wrapper = mount(Insight, {
  localVue,
  store,
  mocks: {
    handleError: mockHandleError,
    handleSuccess: mockHandleSuccess,
    $message: mockMessage,
    kapConfirm: mockKapConfirm
  },
  components: {
    DataSourceBar: dataSourceBarComp.vm,
    tab: tabComp.vm,
    queryTab: queryTabComp.vm
  }
})

describe('Component Insight', () => {
  it('init', () => {
    expect(wrapper.vm.editableTabs).toEqual([{"cancelQuery": false, "extraoption": null, "i18n": "sqlEditor", "icon": "", "index": 0, "name": "WorkSpace", "queryErrorInfo": "", "queryObj": null, "spin": true, "title": "sqlEditor"}])
    expect(mockApi.mockGetSaveQueries.mock.calls[0][1]).toEqual({"limit": 10, "offset": 0, "project": "Kyligence"})
    expect(mockHandleSuccess).toBeCalled()
    expect(wrapper.vm.savedSize).toBe(1)
  })
  it('computed', () => {
    expect(wrapper.vm.isAdmin).toBeTruthy()
  })
  it('methods', async () => {
    wrapper.vm.handleAutoComplete([{ caption: "SSB", id: "9.SSB", meta: "database", scope: 1, value: "SSB" }])
    expect(wrapper.vm.completeData.length).toBe(436)
    await wrapper.vm.openSaveQueryListDialog()
    expect(wrapper.vm.savedQueryListVisible).toBeTruthy()
    expect(wrapper.vm.queryCurrentPage).toBe(1)
    expect(wrapper.vm.checkedQueryList).toEqual([])
    expect(mockApi.mockGetSaveQueries.mock.calls[0][1]).toEqual({"limit": 10, "offset": 0, "project": "Kyligence"})
    expect(mockHandleSuccess).toBeCalled()
    expect(wrapper.vm.savedList).toEqual([{"description": "", "id": "-1317973139", "isShow": false, "name": "_fwewfe", "project": "xm_test", "sql": "select * from SSB.DATES_VIEW"}])
    expect(wrapper.vm.savedSize).toBe(1)

    await wrapper.vm.openSaveQueryDialog()
    jest.runAllTimers()
    expect(wrapper.vm.saveQueryFormVisible).toBeTruthy()

    wrapper.vm.toggleDetail(0)
    expect(wrapper.vm.savedList[0].isShow).toBeTruthy()

    wrapper.vm.resetQuery()
    expect(wrapper.vm.editableTabs).toEqual([{"cancelQuery": false, "extraoption": null, "i18n": "sqlEditor", "icon": "", "index": 0, "name": "WorkSpace", "queryErrorInfo": "", "queryObj": null, "spin": true, "title": "sqlEditor"}])
    expect(mockApi.mockSetQueryTabs.mock.calls[0][1]).toEqual({"tabs": {"Kyligence": [{"cancelQuery": false, "extraoption": null, "i18n": "sqlEditor", "icon": "", "index": 0, "name": "WorkSpace", "queryErrorInfo": "", "queryObj": null, "spin": true, "title": "sqlEditor"}]}})

    wrapper.vm.addTab('query', {
      acceptPartial: true,
      backdoorToggles: { DEBUG_TOGGLE_HTRACE_ENABLED: false },
      limit: 500,
      offset: 0,
      project: "xm_test",
      sql: "select * from SSB.DATES_VIEW",
      stopId: "query_1eiqct9fm"
    })
    expect(wrapper.vm.editableTabs.length).toBe(2)

    wrapper.vm.activeTab('query1')
    expect(wrapper.vm.activeSubMenu).toBe('query1')

    wrapper.vm.delTab('query1')
    expect(wrapper.vm.editableTabs).toEqual([{"cancelQuery": true, "extraoption": null, "i18n": "sqlEditor", "icon": "", "index": 0, "name": "WorkSpace", "queryErrorInfo": "", "queryObj": {"acceptPartial": true, "backdoorToggles": {"DEBUG_TOGGLE_HTRACE_ENABLED": false}, "limit": 500, "offset": 0, "project": "xm_test", "sql": "select * from SSB.DATES_VIEW", "stopId": "query_1eiqct9fm"}, "spin": true, "title": "sqlEditor"}])
    expect(wrapper.vm.activeSubMenu).toEqual('WorkSpace')

    wrapper.vm.delTab('WorkSpace')
    expect(wrapper.vm.activeSubMenu).toEqual('WorkSpace')

    await wrapper.vm.clickTable({label: 'SSB'})
    expect(wrapper.vm.tipsName).toBe('SSB')

    let data = {
      cancelQuery: true,
      extraoption: null,
      i18n: "sqlEditor",
      icon: "el-icon-loading",
      index: 0,
      isStop: false,
      name: "WorkSpace",
      queryErrorInfo: undefined,
      queryObj: {
        acceptPartial: true,
        backdoorToggles: null,
        limit: 500,
        offset: 0,
        project: "xm_test",
        sql: "select * from SSB.DATES_VIEW",
        stopId: "query_1eiqdhq9u"
      },
      spin: true,
      title: "sqlEditor"
    }
    wrapper.setData({editableTabs: [...wrapper.vm.editableTabs, data]})
    await wrapper.update()
    wrapper.vm.closeAllTabs()
    expect(mockApi.mockStopQueryBuild.mock.calls[0][1]).toEqual({"id": "query_1eiqct9fm"})
    expect(wrapper.vm.activeSubMenu).toBe('WorkSpace')
    expect(wrapper.vm.editableTabs).toEqual([{"cancelQuery": true, "extraoption": null, "i18n": "sqlEditor", "icon": "", "index": 0, "name": "WorkSpace", "queryErrorInfo": "", "queryObj": {"acceptPartial": true, "backdoorToggles": {"DEBUG_TOGGLE_HTRACE_ENABLED": false}, "limit": 500, "offset": 0, "project": "xm_test", "sql": "select * from SSB.DATES_VIEW", "stopId": "query_1eiqct9fm"}, "spin": true, "title": "sqlEditor"}])

    wrapper.vm.pageCurrentChange()
    expect(wrapper.vm.queryCurrentPage).toBe(2)
    expect(mockApi.mockGetSaveQueries.mock.calls[2][1]).toEqual({"limit": 10, "offset": 1, "project": "Kyligence"})

    await wrapper.vm.removeQuery({
      description: "",
      id: "-1317973139",
      isShow: false,
      name: "_fwewfe",
      project: "xm_test",
      sql: "select * from SSB.DATES_VIEW"
    })
    // jest.runAllTimers()
    expect(mockKapConfirm).toBeCalledWith('Are you sure you want to delete _fwewfe?', null, 'Delete SQL')
    expect(mockApi.mockDeleteQuery.mock.calls[0][1]).toEqual({"id": "-1317973139", "project": "Kyligence"})
    // expect(mockMessage).toBeCalled()
    expect(wrapper.vm.savedList.length).toBe(2)

    wrapper.vm.cancelResubmit()
    expect(wrapper.vm.savedQueryListVisible).toBeFalsy()

    // wrapper.vm._data.savedList = [{sql: 'select * from SSB.DATES'}]
    // wrapper.setData({checkedQueryList: [0]})
    // await wrapper.update()
    wrapper.find('.saved_query_dialog .el-checkbox').trigger('click')
    wrapper.vm.resubmit()
    expect(wrapper.vm.editableTabs.length).toBe(2)
    expect(wrapper.vm.savedQueryListVisible).toBeFalsy()

    const _data = {
      affectedRowCount: 0,
      appMasterURL: "/kylin/sparder/SQL/execution/?id=2895",
      columnMetas: [],
      duration: 413,
      engineType: "HIVE",
      exception: false,
      exceptionMessage: null,
      hitExceptionCache: false,
      isException: false,
      is_prepare: false,
      is_stop_by_user: false,
      is_timeout: false,
      partial: false,
      prepare: false,
      pushDown: true,
      queryId: "334b5850-0c60-4b40-98ea-7d2a32689bf0",
      queryStatistics: null,
      realizations: [],
      resultRowCount: 500,
      results: [],
      scanBytes: [0],
      scanRows: [500],
      server: "sandbox.hortonworks.com:7072",
      shufflePartitions: 1,
      signature: null,
      stopByUser: false,
      storageCacheUsed: false,
      suite: null,
      timeout: false,
      totalScanBytes: 0,
      totalScanRows: 500,
      traceUrl: null
    }
    wrapper.vm.changeTab(0, _data)
    expect(Array.isArray(wrapper.vm.editableTabs)).toBeTruthy()
    wrapper.vm.changeTab(1, _data)
    expect(wrapper.vm.editableTabs[1].icon).toEqual('el-icon-ksd-good_health')
    wrapper.vm.changeTab(1, _data, 'error')
    expect(wrapper.vm.editableTabs[1].icon).toEqual('el-icon-ksd-error_01')

    wrapper.vm.$store._actions.GET_SAVE_QUERIES = [jest.fn().mockImplementation(() => {
      return {
        then: (callback, errorCallback) => {
          errorCallback()
        }
      }
    })]
    wrapper.vm.loadSavedQuery()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.loadSavedQuerySize()
    expect(wrapper.vm.savedSize).toBe(0)

    wrapper.vm.$store._actions.GET_SAVE_QUERIES = [jest.fn().mockImplementation(() => {
      return {
        then: (callback, errorCallback) => {
          callback(null)
        }
      }
    })]
    wrapper.vm.loadSavedQuerySize()
    expect(wrapper.vm.savedSize).toBe(0)
  })
})
