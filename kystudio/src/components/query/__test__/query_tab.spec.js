import { shallow, mount } from 'vue-test-utils'
import Vuex from 'vuex'
import Vue from 'vue'
import VueI18n from 'vue-i18n'
import { localVue } from '../../../../test/common/spec_common'
import queryTab from '../query_tab'
import queryResult from '../query_result'
import saveQueryDialog from '../save_query_dialog'
import { extraoptions, queryExportData } from './mock'
import * as business from '../../../util/business'

jest.useFakeTimers()
Vue.use(VueI18n)

const mockHandleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback) => {
  callback && callback(res)
  return res
})
const mockHandleError = jest.spyOn(business, 'handleError').mockImplementation((res, callback) => {
  callback && callback(res)
})
const mockKapConfirm = jest.spyOn(business, 'kapConfirm').mockResolvedValue(true)

const mockApi = {
  queryBuildTables: jest.fn().mockImplementation(() => {
    return Promise.resolve(extraoptions)
  }),
  stopQueryBuild: jest.fn().mockImplementation()
}

const store = new Vuex.Store({
  state: {
    config: {
      errorMsgBox: {
        detail: '',
        isShow: false,
        msg: ''
      },
      platform: 'iframe'
    },
    system: {
      allowNotAdminExport: 'true',
      showHtrace: 'false'
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
    }
  },
  actions: {
    QUERY_BUILD_TABLES: mockApi.queryBuildTables,
    STOP_QUERY_BUILD: mockApi.stopQueryBuild
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

const mockMessage = jest.fn()

const queryResultComp = shallow(queryResult, {localVue, store, propsData: {extraoption: extraoptions, isWorkspace: true, queryExportData, isStop: false}})
const saveQueryDialogComp = shallow(saveQueryDialog, { localVue })

const wrapper = mount(queryTab, {
  localVue,
  store,
  propsData: {
    tabsItem: {
      extraoption: null,
      icon: "el-icon-ksd-good_health",
      index: 1,
      isStop: false,
      name: "query1",
      queryErrorInfo: undefined,
      queryObj: {
        acceptPartial: true,
        backdoorToggles: {DEBUG_TOGGLE_HTRACE_ENABLED: false},
        limit: 500,
        offset: 0,
        project: "xm_test",
        sql: "select * from SSB.DATES",
        stopId: "query_1ejc0c1m5"
      },
      spin: false,
      title: "query1"
    },
    completeData: [
      {
        caption: "DEFAULT",
        exactMatch: 1,
        id: "9.DEFAULT",
        matchMask: 0,
        meta: "database",
        scope: 1,
        score: -12,
        value: "DEFAULT"
      }
    ],
    tipsName: ''
  },
  components: {
    queryresult: queryResultComp.vm,
    saveQueryDialog: saveQueryDialogComp.vm
  },
  mocks: {
    $message: mockMessage,
    kapConfirm: mockKapConfirm
  }
})

wrapper.vm.$refs = {
  insightBox: {
    $emit: jest.fn(),
    getValue: () => 1
  },
  queryForm: {
    validate: jest.fn().mockResolvedValue(true)
  }
}
wrapper.vm.$parent = {
  name: 'WorkSpace',
  active: true
}

describe('Component queryTab', () => {
  it('init', () => {
    jest.runAllTimers()
    expect(wrapper.vm.sourceSchema).toBe('select * from SSB.DATES')
    expect(wrapper.vm.isWorkspace).toBeFalsy()
    expect(wrapper.vm.tabsItem.queryObj.stopId).toBe('query_1ejc0c1m5')
    expect(mockApi.queryBuildTables.mock.calls[0][1]).toEqual({"acceptPartial": true, "backdoorToggles": {"DEBUG_TOGGLE_HTRACE_ENABLED": false}, "limit": 500, "offset": 0, "project": "xm_test", "sql": "select * from SSB.DATES", "stopId": "query_1ejc0c1m5"})
    expect(mockHandleSuccess).toBeCalled()
    expect(wrapper.emitted().changeView).not.toEqual([])
  })
  it('computed', () => {
    expect(wrapper.vm.limitRule[0].trigger).toBe('blur')
    expect(wrapper.vm.showHtrace).toBeFalsy()
  })
  it('methods', async () => {
    const mockCallback = jest.fn()
    wrapper.vm.checkLimitNum(null, 2147483648, mockCallback)
    expect(mockMessage).toBeCalledWith({"closeOtherMessages": true, "message": "Please enter a value no larger than 2,147,483,647.", "type": "error"})
    expect(mockCallback).toBeCalled()

    wrapper.vm.checkLimitNum(null, 500, mockCallback)
    expect(mockCallback).toBeCalled()

    wrapper.vm.handleForGuide({action: 'intoEditor', data: {}})
    expect(wrapper.vm.activeSubMenu).toBe('WorkSpace')
    wrapper.vm.handleForGuide({action: 'inputSql', data: {}})
    expect(wrapper.vm.sourceSchema).toEqual({})
    wrapper.vm.handleForGuide({action: 'requestSql', data: {}})
    expect(mockApi.queryBuildTables.mock.calls[1][1]).toEqual({"acceptPartial": true, "backdoorToggles": {"DEBUG_TOGGLE_HTRACE_ENABLED": false}, "limit": 500, "offset": 0, "project": "xm_test", "sql": {}})

    wrapper.vm.changeLimit()
    expect(wrapper.vm.queryForm.listRows).toBe(500)

    wrapper.setData({queryForm: {hasLimit: false, istRows: 500, isHtrace: true}})
    await wrapper.update()
    wrapper.vm.changeLimit()
    expect(wrapper.vm.queryForm.listRows).toBe(0)

    wrapper.vm.changeTrace()
    expect(mockKapConfirm).toBeCalledWith('htraceTips')

    wrapper.vm.openSaveQueryDialog()
    expect(wrapper.vm.saveQueryFormVisible).toBeTruthy()

    wrapper.vm.closeModal(true)
    expect(wrapper.emitted().refreshSaveQueryCount).toEqual([[]])
    expect(wrapper.vm.saveQueryFormVisible).toBeFalsy()

    // try {
    //   wrapper.vm.$set(wrapper.vm, 'isWorkspace', true)
    //   await wrapper.vm.$nextTick()
    //   await wrapper.vm.submitQuery(true)
    //   expect(wrapper.emitted().addTab[0][0]).toEqual("query")
    // } catch (e) {}

    wrapper.vm.$set(wrapper.vm, 'isLoading', true)
    await wrapper.vm.$nextTick()
    await wrapper.vm.submitQuery()
    expect(wrapper.vm.isStopping).toBeTruthy()
    expect(Object.keys(mockApi.stopQueryBuild.mock.calls[0][1])).toEqual(['id'])

    wrapper.vm.queryLoading()
    jest.runAllTimers()
    expect(wrapper.vm.percent).not.toBeNaN()

    wrapper.vm.$store._actions.QUERY_BUILD_TABLES = [jest.fn().mockImplementation(() => {
      return {
        then: (callback, errorCallback) => {
          errorCallback({}, 200, 999, 'error')
        }
      }
    })]
    await wrapper.vm.queryResult()
    expect(mockHandleError).toBeCalled()
    expect(wrapper.vm.$store.state.config).toEqual({"errorMsgBox": {"detail": {}, "isShow": true, "msg": "Request timed out!"}, "platform": "iframe"})
    expect(wrapper.emitted().changeView[3]).toEqual([1, {}, "Request timed out!"])

    wrapper.vm.$store._actions.QUERY_BUILD_TABLES = [jest.fn().mockImplementation(() => {
      return Promise.resolve({...extraoptions, isException: true})
    })]
    await wrapper.vm.queryResult()
    expect(wrapper.vm.errinfo).toBeNull()
    expect(wrapper.emitted().changeView[4]).toEqual([1, {"affectedRowCount": 0, "appMasterURL": "/kylin/sparder/SQL/execution/?id=9317", "columnMetas": [{"autoIncrement": false, "caseSensitive": false, "catelogName": null, "columnType": 91, "columnTypeName": "DATE", "currency": false, "definitelyWritable": false, "displaySize": 2147483647, "isNullable": 1, "label": "d_datekey", "name": "d_datekey", "precision": 0, "readOnly": false, "scale": 0, "schemaName": null, "searchable": false, "signed": true, "tableName": null, "writable": false}], "duration": 662, "engineType": "HIVE", "exception": false, "exceptionMessage": null, "hitExceptionCache": false, "isException": true, "is_prepare": false, "is_stop_by_user": false, "is_timeout": false, "partial": false, "prepare": false, "pushDown": true, "queryId": "92ad159f-caa1-4483-a573-03c206cd5917", "queryStatistics": null, "realizations": [], "resultRowCount": 500, "results": [["1992-01-01"]], "scanBytes": [0], "scanRows": [1000], "server": "sandbox.hortonworks.com:7072", "shufflePartitions": 1, "signature": null, "stopByUser": false, "storageCacheUsed": false, "suite": null, "timeout": false, "totalScanBytes": 0, "totalScanRows": 1000, "traceUrl": null}, null])

    wrapper.vm.handleInputChange('n')
    // wrapper.vm.$nextTick(() => {
    expect(wrapper.vm.queryForm.listRows).toBe(0)
    //   done()
    // })
    wrapper.vm.handleInputChange(1000)
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.queryForm.listRows).toBe(1000)

    await wrapper.vm.resetQuery()
    jest.runAllTimers()
    expect(mockKapConfirm).toBeCalledWith('Are you sure to reset the SQL Editor?', {"cancelButtonText": "Cancel", "confirmButtonText": "Reset", "type": "warning"})
    expect(wrapper.emitted().resetQuery).toEqual([[]])
    expect(wrapper.vm.$refs.insightBox.$emit).toBeCalledWith('setValue', '')
  })
  it('watch', async () => {
    const _data = {
      isLoading: true,
      $emit: jest.fn(),
      $nextTick: (callback) => callback(),
      errinfo: null,
      extraoptionObj: null,
      tabsItem: {...wrapper.vm.tabsItem, queryErrorInfo: {detail: '', isShow: true, msg: 'error'}, extraoption: extraoptions}
    }
    wrapper.vm.$options.methods.onCancelQuery.call(_data, true)
    expect(_data.isLoading).toBeFalsy()
    expect(_data.$emit).toBeCalledWith('changeView', 0, null, '')

    wrapper.vm.$options.methods.onQueryException.call(_data, true)
    expect(_data.isLoading).toBeFalsy()
    expect(_data.errinfo).toEqual({detail: '', isShow: true, msg: 'error'})

    wrapper.vm.$options.methods.onTabsResultChange.call(_data)
    expect(_data.extraoptionObj).toEqual(extraoptions)
    expect(_data.errinfo).toEqual({"detail": "", "isShow": true, "msg": "error"})

    const _data1 = {
      extraoptionObj: null,
      tabsItem: wrapper.vm.tabsItem,
      sourceSchema: '',
      errinfo: null,
      isWorkspace: false,
      queryResult: wrapper.vm.queryResult,
      resetResult: wrapper.vm.resetResult
    }
    wrapper.vm.$options.methods.onTabsItemChange.call(_data1, true)
    expect(_data1.isWorkspace).toBeFalsy()
    expect(mockApi.queryBuildTables.mock.calls[2][1]).toEqual({"acceptPartial": true, "backdoorToggles": {"DEBUG_TOGGLE_HTRACE_ENABLED": false}, "limit": 500, "offset": 0, "project": "xm_test", "sql": "select * from SSB.DATES", "stopId": "query_1ejc0c1m5"})

    _data1.tabsItem.index = 0
    wrapper.vm.$options.methods.onTabsItemChange.call(_data1, true)
    expect(wrapper.vm.extraoptionObj).toBeNull()
    expect(wrapper.vm.isStop).toBeFalsy()

    _data1.tabsItem.queryObj = null
    wrapper.vm.$options.methods.onTabsItemChange.call(_data1, true)
    expect(_data1.sourceSchema).toBe('')
    expect(wrapper.vm.extraoptionObj).toBeNull()
    expect(wrapper.vm.isStop).toBeFalsy()

    const _data2 = {
      $parent: wrapper.vm.$parent,
      $refs: wrapper.vm.$refs,
      sourceSchema: null,
      completeData: wrapper.vm.completeData
    }
    wrapper.vm.$options.methods.onTipsNameChange.call(_data2, true)
    expect(wrapper.vm.$refs.insightBox.$emit).toBeCalledWith('insert', true)
    expect(wrapper.vm.$refs.insightBox.$emit).toBeCalledWith('focus')
    expect(_data2.sourceSchema).toBe(1)

    wrapper.vm.$options.methods.onCompleteDataChange.call(_data2, true)
    expect(wrapper.vm.$refs.insightBox.$emit).toBeCalledWith('setAutoCompleteData', wrapper.vm.completeData)
  })
  it('destory', () => {
    wrapper.vm.$options.methods.destoryed()
    expect(wrapper.vm.ST).not.toBeNaN()
  })
  it('isDestory', () => {
    const _data3 = {
      extraoptionObj: null,
      tabsItem: wrapper.vm.tabsItem,
      sourceSchema: '',
      errinfo: null,
      isWorkspace: false,
      queryResult: wrapper.vm.queryResult,
      _isDestroyed: true
    }
    wrapper.vm.$options.created[0].call(_data3)
    jest.runAllTimers()
    expect(mockApi.queryBuildTables).toBeCalled()
  })
})
