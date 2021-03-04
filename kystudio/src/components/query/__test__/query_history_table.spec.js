import { shallowMount, mount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import queryHistoryTable from '../query_history_table'
import Vuex from 'vuex'
import * as mockData from './mock'
import * as bussiness from '../../../util/business'
import * as util from '../../../util'

jest.useFakeTimers()

const mockHandleSuccess = jest.spyOn(bussiness, 'handleSuccess').mockImplementation((res, callback) => {
  callback && callback()
})
const mockHandleSuccessAsync = jest.spyOn(util, 'handleSuccessAsync').mockImplementation((res, callback) => {
  return new Promise((resolve) => (resolve(res.data || res)))
})
const mockHandleError = jest.spyOn(bussiness, 'handleError').mockImplementation(() => {
  return new Promise((resolve, reject) => reject('error'))
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
  query_history_info: {
    error_msg: "There is no compatible model to accelerate this sql.",
    exactly_match: false,
    execution_error: false,
    scan_segment_num: 0,
    state: "SUCCESS",
    traces: []
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
  sql_text: "select * from\n SSB.DATES_VIEW\n LIMIT 500",
  submitter: "ADMIN",
  total_scan_bytes: 0,
  total_scan_count: 500
}]

const mockApi = {
  mockMarkFav: jest.fn().mockImplementation(() => {
    return Promise.resolve(true)
  }),
  mockFetchHitModelsList: jest.fn().mockImplementation(() => {
    return Promise.resolve(['HIVE', 'CONSTANTS', 'OBJECT STORAGE', 'NATIVE'])
  }),
  mockFetchSubmitterList: jest.fn().mockImplementation(() => {
    return Promise.resolve(['ADMIN'])
  }),
  mockLoadAllIndex: jest.fn().mockImplementation(() => {
    return Promise.resolve({
      value: [
      {
        col_order: [{cardinality: null, key: "TEST_KYLIN_FACT.ORDER_ID", value: "column"}, {cardinality: 1048, key: "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", value: "column"}],
        data_size: 0,
        id: 70001,
        last_modified: 1575725006889,
        manual: false,
        model: "e0e90065-e7c3-49a0-a801-20465ca64799",
        name: null,
        owner: null,
        project: "gc_test",
        shard_by_columns: [],
        sort_by_columns: [],
        source: "RECOMMENDED_AGG_INDEX",
        status: "NO_BUILD",
        storage_type: 20,
        usage: 0
      }
    ]})
  })
}
const mockDialogModal = jest.fn().mockImplementation()

const DetailDialogModal = {
  namespaced: true,
  actions: {
    'CALL_MODAL': mockDialogModal
  }
}

const mockMessage = jest.fn()
const mockFlyEvent = jest.fn()

const store = new Vuex.Store({
  actions: {
    MARK_FAV: mockApi.mockMarkFav,
    FETCH_HIT_MODELS_LIST: mockApi.mockFetchHitModelsList,
    FETCH_SUBMITTER_LIST: mockApi.mockFetchSubmitterList,
    LOAD_ALL_INDEX: mockApi.mockLoadAllIndex
  },
  modules: {
    DetailDialogModal
  },
  getters: {
    currentSelectedProject () {
      return 'Kyligence'
    },
    briefMenuGet () {
      return true
    },
    queryHistoryFilter () {
      return ['filterActions']
    }
  }
})

const wrapper = mount(queryHistoryTable, {
  localVue,
  store,
  propsData: {
    queryHistoryData: queryHistorys,
    queryNodes: []
  },
  mocks: {
    $message: mockMessage,
    flyer: {
      fly: mockFlyEvent
    },
    handleSuccess: mockHandleSuccess,
    handleSuccessAsync: mockHandleSuccessAsync,
    handleError: mockHandleError,
    $: jest.fn().mockImplementation(() => {
      return {
        height: () => 200
      }
    })
  }
})
wrapper.vm.$refs = {
  queryHistoryTable: {
    toggleRowExpansion: jest.fn()
  }
}

global.$ = jest.fn().mockImplementation(() => {
  return {
    height: () => 200
  }
})

const mockGetElementId = jest.spyOn(document, 'getElementById').mockImplementation(() => {
  return {
    offsetHeight: 200
  }
})

describe('Component QueryHistoryTable', () => {
  it('init', async () => {
    await wrapper.vm.$nextTick()
    expect(mockApi.mockFetchHitModelsList.mock.calls[0][1]).toEqual({"model_name": undefined, "page_size": 100, "project": "Kyligence"})
    expect(wrapper.vm.realFilteArr).toEqual([{"icon": "el-icon-ksd-hive", "text": "HIVE", "value": "HIVE"}, {"icon": "el-icon-ksd-contants", "text": "CONSTANTS", "value": "CONSTANTS"}, {"icon": "el-icon-ksd-data_source", "text": "OBJECT STORAGE", "value": "OBJECT STORAGE"}, {"icon": "el-icon-ksd-model", "text": "NATIVE", "value": "NATIVE"}])
    expect(mockApi.mockFetchSubmitterList.mock.calls[0][1]).toEqual({"page_size": 100, "project": "Kyligence", "submitter": undefined})
    expect(wrapper.vm.submitterFilter).toEqual(["ADMIN"])
  })
  it('computed', () => {
    expect(wrapper.vm.isHasFilterValue).toBe(0)
    expect(wrapper.vm.emptyText).toBe('No data')

    expect(wrapper.vm.$options.filters.filterNumbers(0)).toBe(0)
  })
  it('watch', async () => {
    await wrapper.setProps({filterDirectData: {...wrapper.vm.filterDirectData, startTimeFrom: 1613318400000, startTimeTo: 1613923200000}})
    expect(wrapper.vm.datetimerange).toEqual([1613318400000, 1613923200000])
    expect(wrapper.vm.filterData).toEqual({"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": [], "sql": null, "startTimeFrom": 1613318400000, "startTimeTo": 1613923200000, "submitter": []})
    expect(wrapper.vm.filterTags).toEqual([{"key": "datetimerange", "label": "2021-02-15 00:00:00 GMT+8 To 2021-02-22 00:00:00 GMT+8", "source": "kylinLang.query.startTime_th"}])
  
    queryHistorys[0].query_history_info = null
    await wrapper.setProps({queryHistoryData: [...queryHistorys]})
    expect(wrapper.vm.toggleExpandId).toEqual([])
  })
  it('methods', async () => {
    expect(wrapper.vm.checkIsShowMore([])).toBeFalsy()
    expect(wrapper.vm.checkIsShowMore([{modelAlias: 'model_test_123'}])).toBeFalsy()
    expect(wrapper.vm.checkShowCount([{modelAlias: 'model_auto_sandbox__horton_test_123'}])).toBe(1)
    expect(wrapper.vm.checkShowCount([{modelAlias: 'model1'}, {modelAlias: 'model_auto_sandbox__horton_test_123'}])).toBe(1)
    expect(wrapper.vm.checkShowCount([{modelAlias: 'model1'}, {modelAlias: 'model2'}, {modelAlias: 'model3'}])).toBe(3)

    expect(wrapper.vm.sqlOverLimit(['select * from SSB'])).toBeFalsy()
    wrapper.vm.handleExpandType(mockData.currentRow, true)
    expect(wrapper.vm.$refs.queryHistoryTable.toggleRowExpansion).toBeCalled()
    expect(mockData.currentRow.row.hightlight_realizations).toBeTruthy()

    wrapper.vm.onCopy()
    expect(wrapper.vm.showCopyStatus).toBeTruthy()
    jest.runAllTimers()
    expect(wrapper.vm.showCopyStatus).toBeFalsy()

    wrapper.vm.onError()
    expect(mockMessage).toBeCalledWith('Can\'t copy! Your browser does not support paste boards!')


    wrapper.vm.expandChange({...queryHistorys[0], sql_text: Array(200).fill('select \n').join('')})
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.toggleExpandId).toEqual(["b3b89153-141c-4ae8-8b9a-924fb889e1e2"])
    // expect(mockGetElementId).toBeCalled()
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.currentExpandId).toBe('')
    expect(queryHistorys[0].editorH).toBe(0)

    wrapper.vm.expandChange(queryHistorys[0])
    expect(wrapper.vm.toggleExpandId).toEqual([])
    expect(queryHistorys[0].hightlight_realizations).toBeFalsy()

    expect(wrapper.vm.getLayoutIds(mockData.realizations)).toBe('20000010001')
    let data = [
      ...mockData.realizations,
      {
        indexType: "Agg Index",
        layoutId: -1,
        modelAlias: "model_text",
        modelId: "adf65a2d-0b10-48bd-9e9b-9792b2c55eef",
        partialMatchModel: false,
        unauthorized_columns: [],
        unauthorized_tables: [],
        valid: true,
        visible: true
      }
    ]
    expect(wrapper.vm.getLayoutIds(data)).toBe('20000010001')
    expect(wrapper.vm.getLayoutIds([])).toBe('')
    // await wrapper.vm.toAcce(null, mockData.currentRow)
    // expect(mockApi.mockMarkFav.mock.calls[0][1]).toEqual({"project": "Kyligence", "queryStatus": undefined, "queryTime": undefined, "sql": undefined, "sqlPattern": undefined})
    // expect(mockHandleSuccess).toBeCalled()

    wrapper.vm.fiterList('loadFilterHitModelsList', '')
    expect(wrapper.vm.timer).not.toBe(0)
    jest.runAllTimers()
    expect(mockApi.mockFetchHitModelsList).toBeCalled()

    wrapper.vm.clearDatetimeRange()
    expect(wrapper.vm.filterTags).toEqual([])

    await wrapper.setData({filterData: {...wrapper.vm.filterData, sql: 'select * from SSB'}})
    // await wrapper.update()
    wrapper.vm.onSqlFilterChange()
    expect(mockMessage).toBeCalledWith({"duration": 0, "message": "Invalide entering: cannot search space", "showClose": true, "type": "warning"})
    expect(wrapper.emitted().loadFilterList).toEqual([[{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1613318400000, "startTimeTo": 1613923200000, "submitter": []}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": 1613318400000, "startTimeTo": 1613923200000, "submitter": []}]])
    expect(wrapper.vm.toggleExpandId).toEqual([])

    wrapper.vm.openIndexDialog(mockData.realizations[0])
    expect(wrapper.emitted().openIndexDialog).toEqual([[{"indexType": "Table Index", "layoutId": 20000010001, "modelAlias": "model_text", "modelId": "adf65a2d-0b10-48bd-9e9b-9792b2c55eef", "partialMatchModel": false, "snapshots": ["snapshot1", "snapshot2"], "unauthorized_columns": [], "unauthorized_tables": [], "valid": true, "visible": true}, undefined]])

    mockData.realizations.push({...mockData.realizations[0], indexType: 'Agg Index'})
    wrapper.vm.openIndexDialog(mockData.realizations[1])
    expect(wrapper.emitted().openIndexDialog).toEqual([[{"indexType": "Table Index", "layoutId": 20000010001, "modelAlias": "model_text", "modelId": "adf65a2d-0b10-48bd-9e9b-9792b2c55eef", "partialMatchModel": false, "snapshots": ["snapshot1", "snapshot2"], "unauthorized_columns": [], "unauthorized_tables": [], "valid": true, "visible": true}, undefined], [{"indexType": "Agg Index", "layoutId": 20000010001, "modelAlias": "model_text", "modelId": "adf65a2d-0b10-48bd-9e9b-9792b2c55eef", "partialMatchModel": false, "snapshots": ["snapshot1", "snapshot2"], "unauthorized_columns": [], "unauthorized_tables": [], "valid": true, "visible": true}, undefined]])
  
    wrapper.vm.resetLatency()
    expect(wrapper.vm.startSec).toBe(0)
    expect(wrapper.vm.endSec).toBe(10)
    expect(wrapper.vm.filterData.latencyFrom).toBeNull()
    expect(wrapper.vm.filterData.latencyTo).toBeNull()
    expect(wrapper.vm.latencyFilterPopoverVisible).toBeFalsy()
    expect(wrapper.emitted().loadFilterList).toEqual([[{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1613318400000, "startTimeTo": 1613923200000, "submitter": []}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": 1613318400000, "startTimeTo": 1613923200000, "submitter": []}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": 1613318400000, "startTimeTo": 1613923200000, "submitter": []}]])

    await wrapper.setData({startSec: 1599062400000, endSec: 1599667100000})
    // await wrapper.update()
    wrapper.vm.saveLatencyRange()
    expect(wrapper.vm.filterData.latencyFrom).toBe(1599062400000)
    expect(wrapper.vm.filterData.latencyTo).toBe(1599667100000)
    expect(wrapper.vm.latencyFilterPopoverVisible).toBeFalsy()
    expect(wrapper.vm.filterTags).toEqual([{"key": "latency", "label": "1599062400000s To 1599667100000s", "source": "kylinLang.query.latency_th"}])

    wrapper.vm.filterContent(['SUCCESSED'], 'query_status')
    expect(wrapper.vm.filterData['query_status']).toEqual(['SUCCESSED'])
    expect(wrapper.vm.filterTags).toEqual([{"key": "latency", "label": "1599062400000s To 1599667100000s", "source": "kylinLang.query.latency_th"}, {"key": "query_status", "label": "SUCCESSED", "source": "taskStatus"}])
    
    wrapper.vm.handleClose({key: 'datetimerange'})
    expect(wrapper.vm.datetimerange).toBe('')
    wrapper.vm.handleClose({key: 'latency'})
    expect(wrapper.vm.datetimerange).toBe('')
    expect(wrapper.vm.filterData.latencyFrom).toBeNull()
    expect(wrapper.vm.filterData.latencyTo).toBeNull()
    wrapper.vm.handleClose({key: 'server'})
    expect(wrapper.vm.filterData.server).toEqual([])
    expect(wrapper.vm.filterTags).toEqual([])
    // expect(wrapper.emitted().loadFilterList).toEqual([[{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": 1599062400000, "latencyTo": 1599667100000, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": 1599062400000, "latencyTo": 1599667100000, "query_status": ["SUCCESSED"], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": 1599062400000, "latencyTo": 1599667100000, "query_status": ["SUCCESSED"], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": null, "latencyTo": null, "query_status": ["SUCCESSED"], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": null, "latencyTo": null, "query_status": ["SUCCESSED"], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}]])
    wrapper.vm.handleClose({key: 'query_status'})
    expect(wrapper.vm.filterTags).toEqual([])
  
    wrapper.vm.clearAllTags()
    expect(wrapper.vm.query_status).toBeUndefined()
    expect(wrapper.vm.realization).toBeUndefined()
    expect(wrapper.vm.filterData.server).toEqual([])
    expect(wrapper.vm.filterData.latencyFrom).toBeNull()
    expect(wrapper.vm.filterData.latencyTo).toBeNull()
    expect(wrapper.vm.datetimerange).toBe('')

    wrapper.vm.openAuthorityDialog({...mockData.realizations[0], unauthorized_tables: ['P_LINEORDER'], unauthorized_columns: ['C_PHONE', 'C_MKTSEGMENT']})
    expect(mockDialogModal.mock.calls[0][1]).toEqual({"customClass": "no-acl-model", "details": [{"list": ["P_LINEORDER"], "title": "Table (1)"}, {"list": ["C_PHONE", "C_MKTSEGMENT"], "title": "Columns (2)"}], "dialogType": "error", "msg": "You cannot access the model model_text. Because you lack data access to the following tables and columns.", "showCopyBtn": true, "showCopyTextLeftBtn": true, "showDetailBtn": false, "showDetailDirect": true, "showIcon": false, "theme": "plain-mult", "title": "Permission denied details"})
    
    await wrapper.vm.openLayoutDetails({layoutExist: true, modelId: 'adf65a2d-0b10-48bd-9e9b-9792b2c55eef', layoutId: 10001})
    expect(mockApi.mockLoadAllIndex.mock.calls[0][1]).toEqual({"key": 10001, "model": "adf65a2d-0b10-48bd-9e9b-9792b2c55eef", "page_offset": 0, "page_size": 10, "project": "Kyligence", "reverse": "", "sort_by": "", "sources": [], "status": []})
    expect(mockHandleSuccessAsync).toBeCalled()
    expect(wrapper.vm.cuboidData).toEqual({"col_order": [{"cardinality": null, "key": "TEST_KYLIN_FACT.ORDER_ID", "value": "column"}, {"cardinality": 1048, "key": "TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "value": "column"}], "data_size": 0, "id": 70001, "last_modified": 1575725006889, "manual": false, "model": "e0e90065-e7c3-49a0-a801-20465ca64799", "name": null, "owner": null, "project": "gc_test", "shard_by_columns": [], "sort_by_columns": [], "source": "RECOMMENDED_AGG_INDEX", "status": "NO_BUILD", "storage_type": 20, "usage": 0})
    expect(wrapper.vm.detailType).toBe('aggDetail')
    expect(wrapper.vm.indexDetailTitle).toBe('Aggregate Detail [70001]')
    expect(wrapper.vm.indexDetailShow).toBeTruthy()

    wrapper.vm.handleInputDateRange([1599062400000, 1599667100000])
    expect(wrapper.vm.datetimerange).toEqual([1599062400000, 1599667100000])
    expect(wrapper.vm.filterTags).toEqual([{"key": "datetimerange", "label": "2020-09-03 00:00:00 GMT+8 To 2020-09-09 23:58:20 GMT+8", "source": "kylinLang.query.startTime_th"}])
    expect(wrapper.emitted().loadFilterList).toEqual([[{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1613318400000, "startTimeTo": 1613923200000, "submitter": []}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": 1613318400000, "startTimeTo": 1613923200000, "submitter": []}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": 1613318400000, "startTimeTo": 1613923200000, "submitter": []}], [{"latencyFrom": 1599062400000, "latencyTo": 1599667100000, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": 1613318400000, "startTimeTo": 1613923200000, "submitter": []}], [{"latencyFrom": 1599062400000, "latencyTo": 1599667100000, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": 1613318400000, "startTimeTo": 1613923200000, "submitter": []}], [{"latencyFrom": 1599062400000, "latencyTo": 1599667100000, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null, "submitter": []}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null, "submitter": []}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null, "submitter": []}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null, "submitter": []}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null, "submitter": []}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": 1599062400000, "startTimeTo": 1599667100000, "submitter": []}]])

    wrapper.vm.$store._actions.LOAD_ALL_INDEX = jest.fn().mockImplementation(() => {
      return Promise.reject()
    })
    await wrapper.vm.$nextTick()
    await wrapper.vm.openLayoutDetails({layoutExist: true, modelId: 'adf65a2d-0b10-48bd-9e9b-9792b2c55eef', layoutId: 10001})
    expect(mockHandleError).toBeCalled()

    wrapper.vm.$store._actions.FETCH_SUBMITTER_LIST = jest.fn().mockImplementation(() => {
      return Promise.reject()
    })
    await wrapper.vm.$nextTick()
    await wrapper.vm.loadFilterSubmitterList()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.$store._actions.FETCH_HIT_MODELS_LIST = jest.fn().mockImplementation(() => {
      return Promise.reject()
    })
    await wrapper.vm.$nextTick()
    await wrapper.vm.loadFilterHitModelsList()
    expect(mockHandleError).toBeCalled()

    expect(wrapper.vm.getSnapshots(mockData.realizations)).toBe('snapshot1, snapshot2')
    expect(wrapper.vm.getSnapshots([])).toBe('')

    expect(wrapper.vm.getStepData([])).toEqual([])
    expect(wrapper.vm.getStepData(mockData.extraoptions.traces)).toEqual([{"duration": 347, "name": "totalDuration"}, {"duration": 231, "name": "PREPARATION"}, {"duration": 1, "group": "PREPARATION", "name": "GET_ACL_INFO"}, {"duration": 24, "group": "PREPARATION", "name": "SQL_TRANSFORMATION"}, {"duration": 194, "group": "PREPARATION", "name": "SQL_PARSE_AND_OPTIMIZE"}, {"duration": 12, "group": "PREPARATION", "name": "MODEL_MATCHING"}, {"duration": 41, "group": null, "name": "SQL_PUSHDOWN_TRANSFORMATION"}, {"duration": 75, "group": null, "name": "PREPARE_AND_SUBMIT_JOB"}])
    expect(wrapper.vm.getStepData(mockData.extraoptions.traces.filter(it => it.group !== 'PREPARATION'))).toEqual([{"duration": 116, "name": "totalDuration"}, {"duration": 41, "group": null, "name": "SQL_PUSHDOWN_TRANSFORMATION"}, {"duration": 75, "group": null, "name": "PREPARE_AND_SUBMIT_JOB"}])

    wrapper.vm.closeDetailDialog()
    expect(wrapper.vm.indexDetailShow).toBeFalsy()

    await wrapper.setProps({queryHistoryData: []})
    expect(wrapper.vm.toggleExpandId).toEqual([])

    jest.clearAllTimers()
  })
  it('render function', async () => {
    wrapper.find('.el-input-number').trigger('input', 10)
    expect(wrapper.vm.startSec).toBe(1599062400000)
    let options = {
      stopPropagation: jest.fn()
    }
    wrapper.find('.el-icon-ksd-data_range').trigger('click', options)
    expect(options.stopPropagation).toBeCalled()
    expect(wrapper.vm.renderColumn2(wrapper.vm.$createElement).tag).toBe('span')
    await wrapper.setData({filterData: {...wrapper.vm.filterData, latencyTo: 1599667100000}})
    // await wrapper.update()
    expect(wrapper.vm.renderColumn2(wrapper.vm.$createElement).tag).toBe('span')
    wrapper.find('.el-icon-ksd-data_range').trigger('click', options)
    expect(options.stopPropagation).toBeCalled()

    // expect(wrapper.vm.renderColumn(wrapper.vm.$createElement).tag).toBe('span')
    // wrapper.findAll('.el-picker-panel').trigger('input', [1599062400000, 1599162400000])
    // expect(wrapper.vm.datetimerange).toEqual()
    // wrapper.find('.el-date-editor i').trigger('click')
    
    // wrapper.find('.el-date-picker').trigger('input', [1599062400000, 1599162400000])
    // expect(wrapper.vm.datetimerange).toEqual()
  })
})

