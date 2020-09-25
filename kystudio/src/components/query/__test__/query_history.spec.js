import { shallow } from 'vue-test-utils'
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
  mockFetchIndexGraph: jest.fn()
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
    FETCH_INDEX_GRAPH: mockApi.mockFetchIndexGraph
  },
  getters: {
    currentSelectedProject () {
      return 'Kyligence'
    },
    datasourceActions () {
      return ['editAggGroup', 'buildIndex', 'tableIndexActions']
    },
    isAutoProject () {
      return false
    }
  },
  modules: {
    TableIndexEditModal
  }
})
const queryHistoryTableComp = shallow(queryHistoryTable, { propsData: { queryNodes: [], queryHistoryData: [] } })
const modelAggregateComp = shallow(ModelAggregate, { localVue })
const tableIndexComp = shallow(TableIndex, { localVue, store, propsData: {
  modelDesc: {},
  isHideEdit: false,
  layoutId: '',
  isShowTableIndexActions: true,
  isShowBulidIndex: true
} })
const wrapper = shallow(queryHistory, {
  store,
  localVue,
  components: {
    queryHistoryTable: queryHistoryTableComp.vm,
    ModelAggregate: modelAggregateComp.vm,
    TableIndex: tableIndexComp.vm
  }
})

describe('Component QueryHistory', () => {
  it('init', () => {
    expect(mockApi.mockLoadOnlineQueryNodes).toBeCalled()
    expect(mockHandleSuccessAsync).toBeCalledWith(["sandbox.hortonworks.com:7072", "hdp198.hortonworks.com:7072"])
    expect(wrapper.vm.queryNodes).toEqual(["sandbox.hortonworks.com:7072", "hdp198.hortonworks.com:7072"])
    expect(mockApi.mockGetHistoryList.mock.calls[0][1]).toEqual({"latency_from": null, "latency_to": null, "limit": 20, "offset": 0, "project": "Kyligence", "query_status": [], "realization": [], "server": undefined, "sql": "", "start_time_from": null, "start_time_to": null})
    expect(wrapper.vm.queryHistoryData).toEqual({query_histories: queryHistorys, size: 1})
  })
  it('methods', async () => {
    await wrapper.vm.openIndexDialog('b3b89153-141c-4ae8-8b9a-924fb889e1e2', 1)
    expect(wrapper.vm.model.uuid).toBe('b3b89153-141c-4ae8-8b9a-924fb889e1e2')
    expect(wrapper.vm.tabelIndexLayoutId).toBe(1)
    expect(wrapper.vm.tabelIndexVisible).toBeTruthy()
    await wrapper.vm.openIndexDialog('b3b89153-141c-4ae8-8b9a-924fb889e1e2', '')
    expect(wrapper.vm.aggDetailVisible).toBeTruthy()

    wrapper.vm.loadFilterList('test')
    expect(wrapper.vm.filterData).toBe('test')
    expect(wrapper.vm.queryCurrentPage).toBe(1)
    expect(wrapper.vm.pageSize).toBe(20)
    expect(mockApi.mockGetHistoryList).toBeCalled()
  })
})
