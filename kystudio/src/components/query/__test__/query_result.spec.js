import { shallowMount } from '@vue/test-utils'
import Vuex from 'vuex'
import { localVue, mockEchartsEvent } from '../../../../test/common/spec_common'
import queryResult from '../query_result'
import commonTip from 'components/common/common_tip'
import kapPager from 'components/common/kap_pager'
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
        authorities: [{authority: 'ROLE_ADMIN'}],
        create_time: 1600148965832,
        defaultPassword: true,
        disabled: false,
        first_login_failed_time: 0,
        last_modified: 1601214213365,
        locked: false,
        locked_time: 0,
        mvcc: 97,
        username: 'ADMIN',
        uuid: '2400ccc1-8d17-44f9-bb5a-74def5286953',
        version: '4.0.0.0',
        wrong_time: 0
      }
    },
    project: {
      multi_partition_enabled: true
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
    scrollPosition: 'right',
    doLayout: jest.fn()
  }
}

const wrapper = shallowMount(queryResult, {
  localVue,
  store,
  propsData: {
    extraoption: extraoptions,
    isWorkspace: true,
    queryExportData,
    isStop: false,
    tabsItem: {
      name: 'query1'
    }
  },
  mocks: {
  },
  components: {
    commonTip,
    kapPager
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
    expect(wrapper.vm.tableMeta).toEqual([{'autoIncrement': false, 'caseSensitive': false, 'catelogName': null, 'columnType': 91, 'columnTypeName': 'DATE', 'currency': false, 'definitelyWritable': false, 'displaySize': 2147483647, 'isNullable': 1, 'label': 'd_datekey', 'name': 'd_datekey', 'precision': 0, 'readOnly': false, 'scale': 0, 'schemaName': null, 'searchable': false, 'signed': true, 'tableName': null, 'writable': false}, {'autoIncrement': false, 'caseSensitive': false, 'catelogName': null, 'columnType': 5, 'columnTypeName': 'number', 'currency': false, 'definitelyWritable': false, 'displaySize': 2147483647, 'isNullable': 1, 'label': 'd_cuskey', 'name': 'd_cuskey', 'precision': 0, 'readOnly': false, 'scale': 0, 'schemaName': null, 'searchable': false, 'signed': true, 'tableName': null, 'writable': false}])
    expect(wrapper.vm.currentPage).toBe(0)
    expect(wrapper.vm.tableData).toEqual([['1992-01-01']])
    expect(wrapper.vm.pagerTableData).toEqual([['1992-01-01']])

    expect(queryResult.options.filters.filterNumbers(2)).toBe(2)
  })
  it('computed', async () => {
    expect(wrapper.vm.showExportCondition).toBeFalsy()
    expect(wrapper.vm.isAdmin).toBeTruthy()
    // expect(wrapper.vm.answeredBy).toEqual('HIVE')
    // expect(wrapper.vm.layoutIds).toBe('')
    expect(wrapper.vm.insightBtnGroups).toEqual([{'text': 'Data', 'value': 'data'}, {'text': 'Visualization', 'value': 'visualization'}])
    expect(wrapper.vm.displayOverSize).toBe()
    expect(wrapper.vm.snapshots).toEqual('')

    await wrapper.setData({charts: {...wrapper.vm.charts, type: 'pieChart'}})
    expect(wrapper.vm.displayOverSize).toBeFalsy()

    const extra = JSON.parse(JSON.stringify(extraoptions))
    extra.realizations = [{
      modelAlias: 'model_test',
      layoutId: -1,
      snapshots: ['snapshots1', 'snapshots2']
    }]
    await wrapper.setProps({extraoption: extra})
    // await wrapper.update()
    expect(wrapper.vm.snapshots).toEqual('snapshots1, snapshots2')
    // expect(wrapper.vm.answeredBy).toBe('model_test')
    // expect(wrapper.vm.layoutIds).toBe('')

    const extra2 = JSON.parse(JSON.stringify(extraoptions))
    extra2.realizations = [{
      modelAlias: 'model_test_1',
      layoutId: 5
    }]
    await wrapper.setProps({extraoption: extra2})
    // await wrapper.update()
    // expect(wrapper.vm.layoutIds).toBe('5')

    expect(wrapper.vm.isShowNotModelRangeTips).toBeFalsy()

    const extra3 = JSON.parse(JSON.stringify(extraoptions))
    extra3.realizations = [{
      modelAlias: 'model_test_1',
      layoutId: -1,
      indexType: null
    }, {
      modelAlias: 'model_test',
      layoutId: null,
      indexType: null
    }]
    await wrapper.setProps({extraoption: extra3})
    // await wrapper.update()
    expect(wrapper.vm.isShowNotModelRangeTips).toBeTruthy()

    expect(wrapper.vm.noModelRangeTips).toBe('The query is out of the data range for serving queries. Please add segments or subpartitions accordingly.')
    wrapper.vm.$store.state.project.multi_partition_enabled = false
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.noModelRangeTips).toBe('The query is out of the data range for serving queries. Please add segments accordingly.')

    expect(wrapper.vm.querySteps).toEqual([{'duration': 274, 'name': 'totalDuration'}, {'duration': 208, 'name': 'PREPARATION'}, {'duration': 18, 'group': 'PREPARATION', 'name': 'GET_ACL_INFO'}, {'duration': 24, 'group': 'PREPARATION', 'name': 'SQL_TRANSFORMATION'}, {'duration': 154, 'group': 'PREPARATION', 'name': 'SQL_PARSE_AND_OPTIMIZE'}, {'duration': 12, 'group': 'PREPARATION', 'name': 'MODEL_MATCHING'}, {'duration': 41, 'group': null, 'name': 'SQL_PUSHDOWN_TRANSFORMATION'}, {'duration': 25, 'group': null, 'name': 'PREPARE_AND_SUBMIT_JOB'}])

    const extra4 = JSON.parse(JSON.stringify(extraoptions))
    extra4.traces = [
      {
        'name': 'SQL_PUSHDOWN_TRANSFORMATION',
        'group': null,
        'duration': 41
      },
      {
        'name': 'PREPARE_AND_SUBMIT_JOB',
        'group': null,
        'duration': 15
      },
      {
        'name': 'SQL_TRANSFORMATION',
        'group': 'PREPARATION',
        'duration': 44
      }
    ]
    await wrapper.setProps({extraoption: extra4})
    expect(wrapper.vm.querySteps).toEqual([{'duration': 100, 'name': 'totalDuration'}, {'duration': 44, 'name': 'PREPARATION'}, {'duration': 41, 'group': null, 'name': 'SQL_PUSHDOWN_TRANSFORMATION'}, {'duration': 15, 'group': null, 'name': 'PREPARE_AND_SUBMIT_JOB'}, {'duration': 44, 'group': 'PREPARATION', 'name': 'SQL_TRANSFORMATION'}])

    // const extra5 = JSON.parse(JSON.stringify(extraoptions))
    // extra5.traces = []
    // await wrapper.setProps({extraoption: extra5})
    expect(wrapper.vm.getStepData([])).toEqual([])
  })
  it('methods', async () => {
    wrapper.vm.transDataForGrid()
    expect(wrapper.vm.tableMeta).toEqual([{'autoIncrement': false, 'caseSensitive': false, 'catelogName': null, 'columnType': 91, 'columnTypeName': 'DATE', 'currency': false, 'definitelyWritable': false, 'displaySize': 2147483647, 'isNullable': 1, 'label': 'd_datekey', 'name': 'd_datekey', 'precision': 0, 'readOnly': false, 'scale': 0, 'schemaName': null, 'searchable': false, 'signed': true, 'tableName': null, 'writable': false}, {'autoIncrement': false, 'caseSensitive': false, 'catelogName': null, 'columnType': 5, 'columnTypeName': 'number', 'currency': false, 'definitelyWritable': false, 'displaySize': 2147483647, 'isNullable': 1, 'label': 'd_cuskey', 'name': 'd_cuskey', 'precision': 0, 'readOnly': false, 'scale': 0, 'schemaName': null, 'searchable': false, 'signed': true, 'tableName': null, 'writable': false}, {'autoIncrement': false, 'caseSensitive': false, 'catelogName': null, 'columnType': 91, 'columnTypeName': 'DATE', 'currency': false, 'definitelyWritable': false, 'displaySize': 2147483647, 'isNullable': 1, 'label': 'd_datekey', 'name': 'd_datekey', 'precision': 0, 'readOnly': false, 'scale': 0, 'schemaName': null, 'searchable': false, 'signed': true, 'tableName': null, 'writable': false}, {'autoIncrement': false, 'caseSensitive': false, 'catelogName': null, 'columnType': 5, 'columnTypeName': 'number', 'currency': false, 'definitelyWritable': false, 'displaySize': 2147483647, 'isNullable': 1, 'label': 'd_cuskey', 'name': 'd_cuskey', 'precision': 0, 'readOnly': false, 'scale': 0, 'schemaName': null, 'searchable': false, 'signed': true, 'tableName': null, 'writable': false}])

    wrapper.vm.toggleDetail()
    expect(wrapper.vm.showDetail).toBeTruthy()

    expect(wrapper.vm.filterTableData()).toEqual([['1992-01-01']])
    await wrapper.setData({resultFilter: 'test'})
    // setTimeout(() => {
    //   expect(wrapper.vm.filterTableData()).toEqual([["1992-01-01"]])
    // }, 500)
    expect(wrapper.vm.timer).toEqual(2)

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

    wrapper.vm.changeDataType({name: 'visualization'})
    // expect(wrapper.vm.activeResultType).toBe('visualization')
    await wrapper.vm.$nextTick()
    expect(Object.keys(wrapper.vm.chartLayout)).toEqual(['setOption', 'dispose', 'resize'])
    // await wrapper.vm.$nextTick()
    expect(mockEchartsEvent.setOption).toBeCalled()

    wrapper.vm.changeChartDimension()
    expect(mockEchartsEvent.dispose).toBeCalled()
    await wrapper.vm.$nextTick()
    expect(mockEchartsEvent.setOption).toBeCalled()

    wrapper.vm.changeChartMeasure()
    expect(mockEchartsEvent.dispose).toBeCalled()
    await wrapper.vm.$nextTick()
    expect(mockEchartsEvent.setOption).toBeCalled()

    wrapper.vm.changeChartType('barChart')
    expect(wrapper.vm.charts.type).toBe('barChart')
    expect(mockEchartsEvent.dispose).toBeCalled()

    wrapper.vm.resetChartsPosition()
    expect(mockEchartsEvent.resize).toBeCalled()

    await wrapper.setData({tableMetaBackup: []})
    wrapper.vm.disabledChartType()
    expect(wrapper.vm.chartTypeOptions).toEqual([{'isDisabled': true, 'text': 'lineChart', 'value': 'lineChart'}, {'isDisabled': true, 'text': 'barChart', 'value': 'barChart'}, {'isDisabled': true, 'text': 'pieChart', 'value': 'pieChart'}])
    expect(wrapper.vm.charts.type).toEqual('')

    await wrapper.setData({tableMetaBackup: [{
      autoIncrement: false,
      caseSensitive: false,
      catelogName: null,
      columnType: 5,
      columnTypeName: 'number',
      currency: false,
      definitelyWritable: false,
      displaySize: 2147483647,
      isNullable: 1,
      label: 'd_cuskey',
      name: 'd_cuskey',
      precision: 0,
      readOnly: false,
      scale: 0,
      schemaName: null,
      searchable: false,
      signed: true,
      tableName: null,
      writable: false
    }]})
    wrapper.vm.disabledChartType()
    expect(wrapper.vm.chartTypeOptions).toEqual([{'isDisabled': true, 'text': 'lineChart', 'value': 'lineChart'}, {'isDisabled': true, 'text': 'barChart', 'value': 'barChart'}, {'isDisabled': true, 'text': 'pieChart', 'value': 'pieChart'}])
    expect(wrapper.vm.charts.type).toEqual('')

    await wrapper.vm.exportData()
    await wrapper.vm.$nextTick()
    expect(mockApi.mockExportCsv.mock.calls[0][1]).toEqual({'limit': 500, 'project': 'xm_test', 'sql': 'select * from SSB.DATES'})
    // expect(mockRevokeObjectURL).toBeCalled()

    global.window.navigator.msSaveOrOpenBlob = true
    await wrapper.vm.exportData()
    await wrapper.vm.$nextTick()
    expect(mockApi.mockExportCsv.mock.calls[0][1]).toEqual({'limit': 500, 'project': 'xm_test', 'sql': 'select * from SSB.DATES'})
    // expect(mockSaveBlob).toBeCalled()

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
    await wrapper.vm.$nextTick()
    await wrapper.vm.exportData()
    expect(wrapper.vm.hasClickExportBtn).toBeFalsy()

    wrapper.vm.$options.methods.beforeDestory.call(wrapper.vm)
    expect(wrapper.vm.$refs.tableLayout.bodyWrapper.removeEventListener).toBeCalled()
  })
})
