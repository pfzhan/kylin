
import { mount } from '@vue/test-utils'
import Vuex from 'vuex'
import Vue from 'vue'
import VueI18n from 'vue-i18n'
import KyligenceUI from 'kyligence-ui'
import { localVue } from '../../../../test/common/spec_common'
import queryHistory from '../query_history'
import * as util from '../../../util/index'
import queryHistoryTable from '../query_history_table'
import ModelAggregate from '../../studio/StudioModel/ModelList/ModelAggregate/index.vue'
import TableIndex from '../../studio/StudioModel/TableIndex/index.vue'

Vue.use(VueI18n)
Vue.use(KyligenceUI)

const mockHandleSuccessAsync = jest.spyOn(util, 'handleSuccessAsync').mockImplementation((res) => {
  return res
})
const queryHistorys = [{
  cache_hit: false,
  count: 0,
  duration: 1074,
  editorH: 0,
  engine_type: "HIVE",
  error_type: "No realization found",
  exception: false,
  flexHeight: 0,
  id: 230,
  index_hit: false,
  insertTime: 0,
  project_name: "xm_test",
  queryHistoryInfo: {
    error_msg: "There is no compatible model to accelerate this sql.",
    exactly_match: false,
    execution_error: false,
    scan_segment_num: 0,
    state: "SUCCESS"
  },
  queryRealizations: null,
  query_id: "b3b89153-141c-4ae8-8b9a-924fb889e1e2",
  query_status: "SUCCEEDED",
  query_time: 1600410798490,
  realizations: null,
  result_row_count: 500,
  server: ['sandbox.hortonworks.com:7072'],
  sql_limit: "select * from SSB.DATES_VIEW LIMIT 500",
  sql_pattern: "SELECT * FROM SSB.DATES_VIEW LIMIT 1",
  sql_text: "select * from SSB.DATES_VIEW LIMIT 500",
  submitter: "ADMIN",
  total_scan_bytes: 0,
  total_scan_count: 500
}]
const mockApi = {
  mockGetHistoryList: jest.fn().mockImplementation(() => {
    return Promise.resolve({
      query_histories: queryHistorys,
      size: 1
    })
  }),
  mockLoadOnlineQueryNodes: jest.fn().mockImplementation(() => {
    return Promise.resolve(["sandbox.hortonworks.com:7072", "hdp198.hortonworks.com:7072"])
  }),
  mockGetTableIndex: jest.fn().mockResolvedValue(true),
  mockFetchIndexGraph: jest.fn(),
  mockFetchHitModelsList: jest.fn().mockImplementation(() => {
    return Promise.resolve(['HIVE', 'CONSTANTS', 'OBJECT STORAGE', 'NATIVE'])
  }),
  mockFetchSubmitterList: jest.fn().mockImplementation(() => {
    return Promise.resolve(['ADMIN'])
  })
}

const TableIndexEditModal = {
  namespaced: true,
  actions: {
    CALL_MODAL: jest.fn()
  }
}

const store = new Vuex.Store({
  actions: {
    GET_HISTORY_LIST: mockApi.mockGetHistoryList,
    LOAD_ONLINE_QUERY_NODES: mockApi.mockLoadOnlineQueryNodes,
    GET_TABLE_INDEX: mockApi.mockGetTableIndex,
    FETCH_INDEX_GRAPH: mockApi.mockFetchIndexGraph,
    FETCH_HIT_MODELS_LIST: mockApi.mockFetchHitModelsList,
    FETCH_SUBMITTER_LIST: mockApi.mockFetchSubmitterList,
  },
  getters: {
    currentSelectedProject () {
      return 'Kyligence'
    },
    datasourceActions () {
      return ['editAggGroup', 'buildIndex', 'tableIndexActions']
    },
    queryHistoryFilter () {
      return ['filterActions']
    }
  },
  modules: {
    TableIndexEditModal
  }
})
const wrapper = mount(queryHistory, {
  store,
  localVue,
  components: {
    queryHistoryTable,
    ModelAggregate,
    TableIndex
  }
})

describe('Component QueryHistory', () => {
  it('init', () => {
    expect(mockApi.mockLoadOnlineQueryNodes).toBeCalled()
    expect(mockHandleSuccessAsync).toBeCalledWith(["sandbox.hortonworks.com:7072", "hdp198.hortonworks.com:7072"])
    expect(wrapper.vm.queryNodes).toEqual(["sandbox.hortonworks.com:7072", "hdp198.hortonworks.com:7072"])
    // expect(mockApi.mockGetHistoryList.mock.calls[0][1]).toEqual({"latency_from": null, "latency_to": null, "limit": 20, "offset": 0, "project": "Kyligence", "query_status": [], "realization": [], "server": undefined, "sql": "", "start_time_from": null, "start_time_to": null})
  })
  it('router', () => {
    const route = {
      to: {
        name: 'QueryHistory'
      },
      from: {
        name: 'Insight'
      },
      next: jest.fn().mockImplementation(func => func && func(wrapper.vm))
    }
    queryHistory.options.beforeRouteEnter(route.to, route.from, route.next)
    expect(mockApi.mockGetHistoryList.mock.calls[0][1]).toEqual({"latency_from": null, "latency_to": null, "limit": 20, "offset": 0, "project": "Kyligence", "query_status": [], "realization": [], "server": undefined, "sql": "", "start_time_from": null, "start_time_to": null, "submitter": []})
    const route1 = {
      to: {
        name: 'QueryHistory',
        params: {
          source: 'homepage-history'
        }
      },
      from: {
        name: 'Dashboard'
      },
      next: jest.fn().mockImplementation(func => func && func(wrapper.vm))
    }
    queryHistory.options.beforeRouteEnter(route1.to, route1.from, route1.next)
    expect(wrapper.vm.filterDirectData.startTimeFrom).not.toBeNull()
    expect(wrapper.vm.filterDirectData.startTimeTo).not.toBeNull()
  })
  it('methods', async () => {
    await wrapper.vm.loadHistoryList()
    expect(wrapper.vm.queryHistoryData).toEqual({query_histories: queryHistorys, size: 1})
    await wrapper.setData({pageSize: null, filterData: {...wrapper.vm.filterData, startTimeFrom: 1613318400000, startTimeTo: 1613923200000}})
    await wrapper.vm.$nextTick()
    await wrapper.vm.loadHistoryList()
    expect(mockApi.mockGetHistoryList.mock.calls[3][1]).toEqual({"latency_from": null, "latency_to": null, "limit": 20, "offset": 0, "project": "Kyligence", "query_status": [], "realization": [], "server": "", "sql": null, "start_time_from": 1613318400000, "start_time_to": 1613923200000, "submitter": []})
    await wrapper.vm.openIndexDialog({modelId: 'b3b89153-141c-4ae8-8b9a-924fb889e1e2', layoutId: 1}, [])
    expect(wrapper.vm.model.uuid).toBe('b3b89153-141c-4ae8-8b9a-924fb889e1e2')
    // expect(wrapper.vm.tabelIndexLayoutId).toBe(1)
    // expect(wrapper.vm.tabelIndexVisible).toBeTruthy()
    await wrapper.vm.openIndexDialog({modelId: 'b3b89153-141c-4ae8-8b9a-924fb889e1e2', layoutId: ''}, [{
      indexType: "Table Index",
      layoutExist: true,
      layoutId: 20000040001,
      modelAlias: "model_test",
      modelId: "0145588a-3760-4f7f-a511-283dcaa38008",
      partialMatchModel: false,
      snapshots: [],
      unauthorized_columns: [],
      unauthorized_tables: [],
      valid: true,
      visible: true
    }])
    expect(wrapper.vm.model.uuid).toBe('b3b89153-141c-4ae8-8b9a-924fb889e1e2')
    expect(wrapper.vm.aggDetailVisible).toBeTruthy()
    expect(wrapper.vm.aggIndexLayoutId).toBe('')

    await wrapper.setData({pageSize: 20})
    wrapper.vm.loadFilterList('test')
    expect(wrapper.vm.filterData).toBe('test')
    expect(wrapper.vm.queryCurrentPage).toBe(1)
    expect(wrapper.vm.pageSize).toBe(20)
    expect(mockApi.mockGetHistoryList).toBeCalled()
  })
})
