import { shallow, mount } from 'vue-test-utils'
import { localVue } from '../../../../test/common/spec_common'
import queryHistoryTable from '../query_history_table'
import Vuex from 'vuex'
import * as mockData from './mock'
import * as bussiness from '../../../util/business'

jest.useFakeTimers()

const mockHandleSuccess = jest.spyOn(bussiness, 'handleSuccess').mockImplementation((res, callback) => {
  callback && callback()
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
  sql_text: "select * from\n SSB.DATES_VIEW\n LIMIT 500",
  submitter: "ADMIN",
  total_scan_bytes: 0,
  total_scan_count: 500
}]

const mockApi = {
  mockMarkFav: jest.fn().mockImplementation(() => {
    return Promise.resolve(true)
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
    MARK_FAV: mockApi.mockMarkFav
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
  it('computed', () => {
    expect(wrapper.vm.isHasFilterValue).toBe(0)
    expect(wrapper.vm.emptyText).toBe('No data')

    expect(wrapper.vm.$options.filters.filterNumbers(0)).toBe(0)
  })
  it('watch', async () => {
    wrapper.setData({ datetimerange: [1599062400000, 1599667200000] })
    await wrapper.update()
    expect(wrapper.vm.filterData).toEqual({"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": [], "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000})
    expect(wrapper.vm.filterTags).toEqual([{"key": "datetimerange", "label": "2020-09-03 00:00:00 GMT+8 To 2020-09-10 00:00:00 GMT+8", "source": "kylinLang.query.startTime_th"}])
    expect(wrapper.vm.emptyText).toBe('No Results')
    wrapper.setData({ datetimerange: null })
    await wrapper.update()
    expect(wrapper.vm.filterData.startTimeFrom).toBeNull()
    expect(wrapper.vm.filterData.startTimeTo).toBeNull()
    expect(wrapper.vm.filterTags).toEqual([])
    expect(wrapper.emitted().loadFilterList).toEqual([[{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": null, "startTimeTo": null}]])
  })
  it('methods', async (done) => {
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
    jest.runAllTimers()
    expect(wrapper.vm.toggleExpandId).toEqual(["b3b89153-141c-4ae8-8b9a-924fb889e1e2"])
    // expect(mockGetElementId).toBeCalled()
    wrapper.vm.$nextTick(() => {
      expect(wrapper.vm.currentExpandId).toBe('')
      expect(queryHistorys[0].editorH).toBe(0)
      done()
    })

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
    expect(wrapper.vm.getLayoutIds(data)).toBe('20000010001, Snapshot')
    expect(wrapper.vm.getLayoutIds([])).toBe('')
    // await wrapper.vm.toAcce(null, mockData.currentRow)
    // expect(mockApi.mockMarkFav.mock.calls[0][1]).toEqual({"project": "Kyligence", "queryStatus": undefined, "queryTime": undefined, "sql": undefined, "sqlPattern": undefined})
    // expect(mockHandleSuccess).toBeCalled()

    wrapper.setData({filterData: {...wrapper.vm.filterData, sql: 'select * from SSB'}})
    await wrapper.update()
    wrapper.vm.onSqlFilterChange()
    expect(mockMessage).toBeCalledWith({"duration": 0, "message": "Invalide entering: cannot search space", "showClose": true, "type": "warning"})
    expect(wrapper.emitted().loadFilterList).toEqual([[{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}]])
    expect(wrapper.vm.toggleExpandId).toEqual([])

    wrapper.vm.openIndexDialog(mockData.realizations[0])
    expect(wrapper.emitted().openIndexDialog).toEqual([["adf65a2d-0b10-48bd-9e9b-9792b2c55eef", 20000010001]])

    mockData.realizations.push({...mockData.realizations[0], indexType: 'Agg Index'})
    wrapper.vm.openIndexDialog(mockData.realizations[1])
    expect(wrapper.emitted().openIndexDialog).toEqual([["adf65a2d-0b10-48bd-9e9b-9792b2c55eef", 20000010001], ["adf65a2d-0b10-48bd-9e9b-9792b2c55eef"]])
  
    wrapper.vm.resetLatency()
    expect(wrapper.vm.startSec).toBeNull()
    expect(wrapper.vm.endSec).toBeNull()
    expect(wrapper.vm.filterData.latencyFrom).toBeNull()
    expect(wrapper.vm.filterData.latencyTo).toBeNull()
    expect(wrapper.vm.latencyFilterPopoverVisible).toBeFalsy()
    expect(wrapper.emitted().loadFilterList).toEqual([[{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}]])

    wrapper.setData({startSec: 1599062400000, endSec: 1599667100000})
    await wrapper.update()
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
    expect(wrapper.emitted().loadFilterList).toEqual([[{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": 1599062400000, "startTimeTo": 1599667200000}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": null, "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": null, "latencyTo": null, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": 1599062400000, "latencyTo": 1599667100000, "query_status": [], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": 1599062400000, "latencyTo": 1599667100000, "query_status": ["SUCCESSED"], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": 1599062400000, "latencyTo": 1599667100000, "query_status": ["SUCCESSED"], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": null, "latencyTo": null, "query_status": ["SUCCESSED"], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}], [{"latencyFrom": null, "latencyTo": null, "query_status": ["SUCCESSED"], "realization": [], "server": "", "sql": "select * from SSB", "startTimeFrom": null, "startTimeTo": null}]])
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
    wrapper.setData({filterData: {...wrapper.vm.filterData, latencyTo: 1599667100000}})
    await wrapper.update()
    expect(wrapper.vm.renderColumn2(wrapper.vm.$createElement).tag).toBe('span')
    wrapper.find('.el-icon-ksd-data_range').trigger('click', options)
    expect(options.stopPropagation).toBeCalled()

    // expect(wrapper.vm.renderColumn(wrapper.vm.$createElement).tag).toBe('span')
    // console.log(wrapper.findAll('.el-picker-panel'))
    // wrapper.findAll('.el-picker-panel').trigger('input', [1599062400000, 1599162400000])
    // expect(wrapper.vm.datetimerange).toEqual()
    // wrapper.find('.el-date-editor i').trigger('click')
    
    // wrapper.find('.el-date-picker').trigger('input', [1599062400000, 1599162400000])
    // expect(wrapper.vm.datetimerange).toEqual()
  })
})

