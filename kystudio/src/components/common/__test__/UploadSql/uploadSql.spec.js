import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import Vuex from 'vuex'
import * as utils from '../../../../util'
import * as business from '../../../../util/business'
import UploadSql from '../../UploadSql/UploadSql.vue'
import UploadSqlModel from '../../UploadSql/store'
import SuggestModel from '../../UploadSql/SuggestModel.vue'

jest.useFakeTimers()

const types = {
  IMPORT_SQL_FILES: 'IMPORT_SQL_FILES',
  FORMAT_SQL: 'FORMAT_SQL',
  ADD_TO_FAVORITE_LIST: 'ADD_TO_FAVORITE_LIST',
  VALIDATE_WHITE_SQL: 'VALIDATE_WHITE_SQL',
  SUGGEST_MODEL: 'SUGGEST_MODEL',
  SAVE_SUGGEST_MODELS: 'SAVE_SUGGEST_MODELS',
  VALIDATE_MODEL_NAME: 'VALIDATE_MODEL_NAME',
  SUGGEST_IS_BY_ANSWERED: 'SUGGEST_IS_BY_ANSWERED',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM'
}

let importSqlFilesData = {
  size: 300,
  imported: 2,
  blacklist: 1,
  capable_sql_num: 1,
  data: [{id: 0, capable: true, sql: 'sql1', sql_advices: []}],
  msg: 'over sizes'
}

const root = {
  state: UploadSqlModel.state,
  commit: function (name, params) { UploadSqlModel.mutations[name](root.state, params) },
  dispatch: function (name, params) { return UploadSqlModel.actions[name]({state: root.state, commit: root.commit, dispatch: root.dispatch}, params) }
}

const handleSuccessAsync = jest.spyOn(utils, 'handleSuccessAsync').mockImplementation((res, callback) => {
  return new Promise((resolve) => (resolve([{capable: true, id: 1}])))
})
let handleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback) => {
  return new Promise((resolve) => {
    res ? callback(res) : callback({...importSqlFilesData, capable: true}, '000', 200, 'over sizes')
    resolve(res)
  })
})
const handleError = jest.spyOn(business, 'handleError').mockImplementation(() => {})

const mockKapConfirm = jest.spyOn(business, 'kapConfirm').mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const mockKapWarn = jest.spyOn(business, 'kapWarn').mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})

let importSqlFiles = jest.fn().mockImplementation(() => {
  return {
    then: (callback, errorCallback) => {
      callback()
      errorCallback()
    }
  }
})
const formatSql = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const addTofavoriteList = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const validateWhite = jest.fn().mockImplementation(() => {
  return {
    then: (callback, errorCallback) => {
      callback()
      errorCallback()
    }
  }
})
let suggestModel = jest.fn().mockImplementation(() => {
  return {
    then: (callback, errorCallback) => {
      callback({reused_models: Array(1001)})
      errorCallback()
    }
  }
})
const saveSuggestModels = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})
const $message = {
  success: jest.fn(),
  warning: jest.fn(),
  error: jest.fn()
}
const $alert = jest.fn()
const mockGlobalConfirm = jest.fn().mockResolvedValue('')
const validateModelName = jest.fn()
const suggestIsByAnswered = jest.fn().mockImplementation(() => {
  return {
    then: (callback, errorCallback) => {
      callback(true)
      errorCallback()
    }
  }
})

const mockClearSelection = jest.fn()
const mockToggleRowSelection = jest.fn()

const store = new Vuex.Store({
  state: {
    system: {lang: 'en'}
  },
  getters: {
    currentSelectedProject () {
      return 'learn_kylin'
    },
    isAdminRole () {
      return true
    }
  },
  actions: {
    [types.IMPORT_SQL_FILES]: importSqlFiles,
    [types.FORMAT_SQL]: formatSql,
    [types.ADD_TO_FAVORITE_LIST]: addTofavoriteList,
    [types.VALIDATE_WHITE_SQL]: validateWhite,
    [types.SUGGEST_MODEL]: suggestModel,
    [types.SAVE_SUGGEST_MODELS]: saveSuggestModels,
    [types.VALIDATE_MODEL_NAME]: validateModelName,
    [types.SUGGEST_IS_BY_ANSWERED]: suggestIsByAnswered
  },
  modules: {
    UploadSqlModel
  }
})

const model = shallowMount(SuggestModel, { localVue, store, propsData: {suggestModels: [], tableRef: '', isOriginModelsTable: true, maxHeight: 200} })

const wrapper = shallowMount(UploadSql, {
  localVue,
  store,
  mocks: {
    handleSuccessAsync: handleSuccessAsync,
    handleSuccess: handleSuccess,
    handleError: handleError,
    kapConfirm: mockKapConfirm,
    kapWarn: mockKapWarn,
    $message: $message,
    $alert: $alert,
    $confirm: mockGlobalConfirm
  },
  propsData: {
    projectName: 'abc',
    whiteSqlData: {capable_sql_num: 0, size: 0, data: []}
  },
  components: {
    SuggestModel: model.vm
  }
})

wrapper.vm.$refs = {
  multipleTable: {
    clearSelection: mockClearSelection,
    toggleRowSelection: mockToggleRowSelection,
    doLayout: jest.fn()
  },
  whiteInputBox: {
    $emit: jest.fn(),
    $refs: {
      kapEditor: {
        editor: {
          setValue: jest.fn()
        }
      }
    }
  }
}

describe('Component SuggestModel', () => {
  it('computed events', async () => {
    expect(wrapper.vm.emptyText).toEqual('No data')
    await wrapper.setData({ whiteSqlFilter: 'data' })
    // await wrapper.update()
    expect(wrapper.vm.emptyText).toEqual('No Results')

    expect(wrapper.vm.uploadTitle).toEqual('Import SQL')
    wrapper.vm.$store.state.UploadSqlModel.isGenerateModel = true
    await wrapper.setData({ uploadFlag: 'step1' })
    // await wrapper.update()
    expect(wrapper.vm.uploadTitle).toEqual('Add Model From SQL')

    // wrapper.setData({ uploadFlag: 'step2' })
    // await wrapper.update()
    // expect(wrapper.vm.uploadTitle).toEqual('Import SQL')

    await wrapper.setData({ uploadFlag: 'step3' })
    // await wrapper.update()
    expect(wrapper.vm.uploadTitle).toEqual('Preview')

    expect(wrapper.vm.isShowSuggestModels).toBeFalsy()
    await wrapper.setData({ suggestModels: ['1'] })
    // await wrapper.update()
    expect(wrapper.vm.isShowSuggestModels).toBeTruthy()

    expect(wrapper.vm.isShowOriginModels).toBeFalsy()
    await wrapper.setData({ suggestModels: [], originModels: ['1'] })
    // await wrapper.update()
    expect(wrapper.vm.isShowOriginModels).toBeTruthy()

    expect(wrapper.vm.isShowTabModels).toBeFalsy()
    await wrapper.setData({ suggestModels: ['1'], originModels: ['1'] })
    // await wrapper.update()
    expect(wrapper.vm.isShowTabModels).toBeTruthy()

    expect(wrapper.vm.getFinalSelectModels).toEqual([])
    await wrapper.setData({ selectModels: ['1'], selectRecommends: ['2'] })
    // await wrapper.update()
    expect(wrapper.vm.getFinalSelectModels).toEqual(['1', '2'])

    expect(wrapper.vm.uploadHeader).toEqual({'Accept-Language': 'en'})
    wrapper.vm.$store.state.system.lang = 'zh'
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.uploadHeader).toEqual({'Accept-Language': 'cn'})

    await wrapper.setData({ selectSqls: [{sql: 'sql1'}, {sql: 'sql2'}], whiteSqlFilter: '' })
    // await wrapper.update()
    expect(wrapper.vm.finalSelectSqls).toEqual([{sql: 'sql1'}, {sql: 'sql2'}])
    await wrapper.setData({ selectSqls: [{sql: 'sql1'}, {sql: 'sql2'}], whiteSqlFilter: '1' })
    // await wrapper.update()
    expect(wrapper.vm.finalSelectSqls).toEqual([{sql: 'sql1'}])
  })
  it('methods events', async () => {
    expect(wrapper.vm.tableRowClassName({ row: {} })).toEqual('')
    await wrapper.setData({ activeSqlObj: {id: 1} })
    // await wrapper.update()
    expect(wrapper.vm.tableRowClassName({ row: {id: 1} })).toEqual('active-row')

    wrapper.vm.showLoading()
    expect(wrapper.vm.sqlLoading).toBeTruthy()
    wrapper.vm.hideLoading()
    expect(wrapper.vm.sqlLoading).toBeFalsy()

    await wrapper.setData({ isEditSql: false })
    // await wrapper.update()
    wrapper.vm.delWhiteComfirm()
    expect(mockKapConfirm).toBeCalledWith('Are you sure you want to delete this SQL?', {centerButton: true}, 'Delete SQL')

    await wrapper.setData({ isEditSql: true })
    // await wrapper.update()
    wrapper.vm.delWhiteComfirm()
    expect(mockKapWarn).toBeCalled()

    wrapper.vm.getSelectModels(['1'])
    expect(wrapper.vm.selectModels).toEqual(['1'])

    wrapper.vm.getSelectOriginModels(['1'])
    expect(wrapper.vm.selectOriginModels).toEqual(['1'])

    wrapper.vm.getSelectRecommends(['1'])
    expect(wrapper.vm.selectRecommends).toEqual(['1'])

    wrapper.vm.isValidated(true)
    expect(wrapper.vm.isNameErrorModelExisted).toBeTruthy()

    await wrapper.setData({ whiteSqlData: {capable_sql_num: 1, size: 1, data: [{id: 0, capable: true, sql: 'sql1', sql_advices: []}]}, selectSqls: [{id: 0, capable: true, sql: 'sql1', sql_advices: []}] })
    // await wrapper.update()
    wrapper.vm.delWhite(0)
    expect(wrapper.vm.whiteSqlData).toEqual({capable_sql_num: 0, size: 0, data: []})
    expect(wrapper.vm.selectSqls).toEqual([])

    await wrapper.setData({ whiteSqlData: {capable_sql_num: 1, size: 1, data: [{id: 0, capable: true, sql: 'sql1', sql_advices: []}, {id: 1, capable: true, sql: 'sql2', sql_advices: []}]} })
    // await wrapper.update()
    await wrapper.vm.whiteSqlDatasPageChange(1, 10)
    expect(wrapper.vm.whiteCurrentPage).toBe(1)
    expect(wrapper.vm.whitePageSize).toBe(10)
    expect(wrapper.vm.$refs.multipleTable.doLayout).toBeCalled()
    wrapper.vm.delWhite(0)
    // wrapper.vm.whiteSqlDatasPageChange(1, 10)
    expect(wrapper.vm.whiteSql).toEqual('')
    // expect(wrapper.vm.activeSqlObj).toEqual({id: 1})
    expect(wrapper.vm.isEditSql).toBeFalsy()
    expect(wrapper.vm.whiteMessages).toEqual([])
    expect(wrapper.vm.isWhiteErrorMessage).toBeFalsy()
    expect(wrapper.vm.inputHeight).toBe(479)

    wrapper.setData({ whiteSqlData: {capable_sql_num: 1, size: 1, data: [{id: 0, capable: true, sql: 'sql1', sql_advices: []}, {id: 1, capable: false, sql: 'sql2', sql_advices: []}]} })
    wrapper.vm.selectAll()
    expect(wrapper.vm.selectSqls).toEqual([{id: 0, capable: true, sql: 'sql1', sql_advices: []}])

    wrapper.vm.handleSelectAllChange([{id: '1'}])
    expect(wrapper.vm.selectSqls).toEqual([{id: 0, capable: true, sql: 'sql1', sql_advices: []}, {id: '1'}])
    await wrapper.setData({ pagerTableData: [{id: '2'}], selectSqls: [{id: '2', sql: 'sql2'}] })
    // await wrapper.update()
    // wrapper.vm.handleSelectAllChange([])
    expect(wrapper.vm.selectSqls).toEqual([{id: '2', sql: 'sql2'}])

    await wrapper.setData({ selectSqls: [] })
    // await wrapper.update()
    wrapper.vm.mergeSelectSqls({id: 1}, 'batchAdd')
    expect(wrapper.vm.selectSqls).toEqual([{id: 1}])
    wrapper.vm.mergeSelectSqls({id: 1}, 'batchRemove')
    expect(wrapper.vm.selectSqls).toEqual([])

    wrapper.vm.handleCloseAcceptModal()
    expect(wrapper.vm.$store.state.UploadSqlModel.isShow).toBeFalsy()
    expect(wrapper.emitted().reloadModelList).toEqual([[]])

    await wrapper.vm.submitModels()
    expect(wrapper.vm.submitModelLoading).toBeFalsy()
    expect(saveSuggestModels.mock.calls[0][1]).toEqual({'new_models': [], 'project': 'learn_kylin', 'reused_models': []})
    expect(handleSuccess).toBeCalled()
    expect(wrapper.emitted().reloadModelList).toEqual([[], []])
    expect(mockGlobalConfirm).toBeCalledWith('Successfully accepted 0 recommendation(s), The added indexes would be ready for queries after being built.', 'Imported successfully', {'confirmButtonText': 'OK', 'showCancelButton': false, 'type': 'success'})

    await wrapper.setData({ isEditSql: true })
    // await wrapper.update()
    await wrapper.vm.submitSqls()
    expect(mockKapWarn).toBeCalled()
    await wrapper.setData({ whiteSqlData: {capable_sql_num: 2, size: 2, data: []}, finalSelectSqls: [{id: 1, sql: 'sql1'}] })
    // await wrapper.update()
    await wrapper.vm.submitSqls()
    expect(mockKapConfirm).toBeCalled()

    wrapper.vm.$store.state.UploadSqlModel.isGenerateModel = false
    await wrapper.setData({ finalSelectSqls: [{id: 1, sql: 'sql1'}] })
    // await wrapper.update()
    await wrapper.vm.submit()
    expect(wrapper.vm.submitSqlLoading).toBeFalsy()
    expect(addTofavoriteList).toBeCalled()
    expect(handleSuccess).toBeCalled()
    expect($alert).toBeCalledWith('0 new SQL statement(s) has(have) been imported successfully. 2 of them has(have) been added to the waiting list (where 1 statement(s) has(have) been added to the black list and will not be accelerated).', 'Notification', {'confirmButtonText': 'OK', 'iconClass': 'el-icon-info primary'})
    expect(wrapper.emitted().reloadListAndSize).toEqual([[], [], []])
    expect(wrapper.vm.$store.state.UploadSqlModel.isShow).toBeFalsy()

    wrapper.vm.$store.state.UploadSqlModel.isGenerateModel = true
    await wrapper.setData({ selectSqls: [{id: 1, sql: 'sql1'}] })
    // await wrapper.update()
    await wrapper.vm.submit()
    expect(wrapper.vm.generateLoading).toBeFalsy()
    expect(suggestIsByAnswered).toBeCalled()
    expect(handleSuccess).toBeCalled()
    expect(wrapper.vm.convertSqls).toEqual(['sql1'])
    expect(handleError).toBeCalled()

    wrapper.vm.handleConvertClose()
    expect(wrapper.vm.isConvertShow).toBeFalsy()
    expect(wrapper.vm.convertSqls).toEqual([])
    expect(wrapper.vm.generateLoading).toBeFalsy()

    await wrapper.vm.convertSqlsSubmit(true)
    jest.runAllTimers()
    expect(mockGlobalConfirm).toBeCalledWith('Recommendations can\'t be shown properly due to the extreme large amount. Please try selecting fewer SQLs to import at a time.', 'Can\'t Show All Recommendations', {'confirmButtonText': 'OK', 'showCancelButton': false, 'showClose': false, 'type': 'warning'})
    expect(wrapper.vm.generateLoading).toBeFalsy()
    expect(wrapper.vm.convertLoading).toBeFalsy()
    expect(wrapper.vm.cancelConvertLoading).toBeFalsy()
    expect(wrapper.vm.isConvertShow).toBeFalsy()
    wrapper.vm.convertSqlsSubmit(false)
    expect(wrapper.vm.cancelConvertLoading).toBeFalsy()
    expect(suggestModel).toBeCalled()

    wrapper.vm.getSuggestModels()
    expect(suggestModel).toBeCalled()
    expect(handleSuccess).toBeCalled()

    wrapper.vm.$store._actions.SUGGEST_MODEL = [jest.fn().mockImplementation(() => {
      return {
        then: (callback, errorCallback) => {
          callback({new_models: [{alias: 'TABLE_NAME', uuid: '23123124-fie3124312-323123fe'}], reused_models: [{rec_items: []}]})
          errorCallback()
        }
      }
    })]
    await wrapper.vm.$nextTick()
    await wrapper.vm.getSuggestModels()
    expect(wrapper.vm.uploadFlag).toBe('step3')
    expect(wrapper.vm.suggestModels).toEqual([{alias: 'TABLE_NAME', uuid: '23123124-fie3124312-323123fe', isChecked: true, isNameError: false}])
    expect(wrapper.vm.originModels).toEqual([{isChecked: true, rec_items: []}])

    wrapper.vm.selectPagerSqls()
    jest.runAllTimers()
    expect(mockClearSelection).toBeCalled()

    await wrapper.setData({ whiteSqlFilter: '1' })
    // await wrapper.update()
    const filterData = wrapper.vm.whiteFilter([{sql: 'sql1'}])
    expect(filterData).toEqual([{sql: 'sql1'}])

    wrapper.vm.toggleSelection(['1'])
    expect(mockClearSelection).toBeCalled()
    expect(mockToggleRowSelection).toBeCalled()

    await wrapper.setData({ isEditSql: true })
    // await wrapper.update()
    wrapper.vm.editWhiteSql({id: 4, capable: false})
    expect(mockKapWarn).toBeCalled()
    await wrapper.setData({ isEditSql: false, activeSqlObj: {id: 1} })
    // await wrapper.update()
    await wrapper.vm.editWhiteSql({id: '2', capable: true})
    expect(wrapper.vm.isEditSql).toBeTruthy()
    expect(wrapper.vm.inputHeight).toBe(382)
    expect(formatSql).toBeCalled()
    expect(handleSuccessAsync).toBeCalled()
    expect(wrapper.vm.sqlFormatterObj).toEqual({'2': {'capable': true, 'id': 1}, '4': {'capable': true, 'id': 1}})
    expect(wrapper.vm.$refs.whiteInputBox.$emit).toBeCalled()
    expect(wrapper.vm.activeSqlObj).toEqual({'capable': true, 'id': '2'})
    expect(wrapper.vm.isReadOnly).toBeFalsy()
    // expect(wrapper.vm.whiteSqlData).toEqual()
    expect($message.success.mock.calls[0][0]).toBe('The operation is successfully')

    await wrapper.setData({ isEditSql: true })
    // await wrapper.update()
    wrapper.vm.activeSql({capable: true, sql_advices: []})
    expect(mockKapWarn).toBeCalled()
    expect(validateWhite).toBeCalled()
    expect(handleError).toBeCalled()

    await wrapper.vm.activeSql({id: 2, capable: false})
    expect(wrapper.vm.$refs.whiteInputBox.$emit).toBeCalledWith('input', {'capable': true, 'id': 1})
    expect(wrapper.vm.whiteSql).toEqual({'capable': true, 'id': 1})
    expect(wrapper.vm.inputHeight).toBe(339)

    wrapper.vm.fileItemChange({name: 'sql.txt', size: 1 * 1024 * 1024}, [{name: 'sql.sql', size: 1 * 1024 * 1024}])
    expect(wrapper.vm.uploadItems).toEqual([{name: 'sql.sql', size: 1 * 1024 * 1024}])
    expect(wrapper.vm.uploadRules.totalSize.status).toBe('success')
    expect(wrapper.vm.uploadRules.fileFormat.status).toBe('success')

    wrapper.vm.fileItemChange({name: 'sql.txt', size: 1 * 1024 * 1024}, [{name: 'sql.sql', size: 6 * 1024 * 1024}])
    expect(wrapper.vm.uploadItems).toEqual([{name: 'sql.sql', size: 6 * 1024 * 1024}])
    // expect($message.warning).toBeCalledWith('The total file size cannot exceed 5 MB')
    expect(wrapper.vm.uploadRules.totalSize.status).toBe('error')
    expect(wrapper.vm.wrongFormatFile).toEqual([])

    wrapper.vm.fileItemChange({name: 'sql.csv', size: 1 * 1024 * 1024}, [{name: 'sql.csv', size: 6 * 1024 * 1024}])
    expect(wrapper.vm.uploadItems).toEqual([{'name': 'sql.csv', 'size': 6291456}])
    expect(wrapper.vm.uploadRules.fileFormat.status).toBe('error')
    expect(wrapper.vm.wrongFormatFile).toEqual([])

    wrapper.vm.handleRemove({}, [])
    expect(wrapper.vm.uploadItems).toEqual([])
    wrapper.vm.handleRemove({}, [{name: 'commem.txt', size: 6 * 1024 * 1024, file: null}])
    // expect($message.warning).toBeCalledWith('The total file size cannot exceed 5 MB')
    expect(wrapper.vm.uploadRules.totalSize.status).toBe('error')

    const isSelectable = wrapper.vm.selectable({capable: true})
    expect(isSelectable).toBeTruthy()
    const isSelectable2 = wrapper.vm.selectable({capable: false})
    expect(isSelectable2).toBeFalsy()

    wrapper.vm.cancelEdit()
    expect(wrapper.vm.isEditSql).toBeFalsy()
    expect(wrapper.vm.inputHeight).toBe(479)
    expect(wrapper.vm.activeSqlObj).toBeNull()
    expect(wrapper.vm.isReadOnly).toBeTruthy()
    await wrapper.setData({ activeSqlObj: {id: 1} })
    // await wrapper.update()
    wrapper.vm.cancelEdit(true)
    expect(wrapper.vm.isEditSql).toBeFalsy()
    expect(wrapper.vm.inputHeight).toBe(339)
    expect(wrapper.vm.activeSqlObj).toBeNull()
    expect(wrapper.vm.isReadOnly).toBeTruthy()

    wrapper.vm.submitFiles()
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.importLoading).toBeFalsy()
    expect(importSqlFiles.mock.calls[0][1].project).toBe('learn_kylin')
    expect(handleSuccess).toBeCalled()
    // expect(wrapper.vm.uploadRules.sqlSizes.status).toBe('success')
    // expect(wrapper.vm.uploadRules.unValidSqls.status).toBe('success')
    // expect($message.error).toBeCalledWith('Up to 200 SQLs could be uploaded at a time. Please try selecting fewer files to upload.')

    expect(handleError).toBeCalled()

    importSqlFilesData.size = 100
    await wrapper.vm.$nextTick()
    wrapper.vm.submitFiles()
    expect(wrapper.vm.importLoading).toBeFalsy()
    expect(importSqlFiles.mock.calls[1][1].project).toBe('learn_kylin')
    expect(handleSuccess).toBeCalled()
    expect(wrapper.vm.uploadFlag).toBe('step3')
    expect(wrapper.vm.whiteSqlData).toEqual({'capable_sql_num': 2, 'data': [], 'size': 2})
    expect(wrapper.vm.selectSqls).toEqual([{'id': 1, 'sql': 'sql1'}])
    // expect($message.warning).toBeCalled()

    await wrapper.vm.handleCancel()
    expect(wrapper.vm.uploadFlag).toBe('step1')
    expect(wrapper.vm.modelType).toBe('suggest')
    expect(wrapper.vm.$store.state.UploadSqlModel.isShow).toBeFalsy()

    await wrapper.setData({ uploadFlag: 'step3' })
    // await wrapper.update()
    await wrapper.vm.handleCancel()
    expect(mockGlobalConfirm).toBeCalledWith('Once cancel, all edits would be discarded. Are you sure you want to cancel?', 'Notice', {'cancelButtonText': 'Continue Editing', 'centerButton': true, 'confirmButtonText': 'Confirm to Cancel', 'type': 'warning'})
    expect(wrapper.vm.uploadFlag).toBe('step1')
    expect(wrapper.vm.modelType).toBe('suggest')
    expect(wrapper.vm.$store.state.UploadSqlModel.isShow).toBeFalsy()

    wrapper.vm.onWhiteSqlFilterChange()
    jest.runAllTimers()
    expect(wrapper.vm.filteredDataSize).toBe(0)

    wrapper.vm.cancelSelectAll()
    expect(wrapper.vm.selectSqls).toEqual([])

    wrapper.vm.handleSelectionChange('', {capable: true, id: 0, sql: 'select * from SSB.P_LINEORDER', sql_advices: []})
    expect(wrapper.vm.selectSqls).toEqual([{'capable': true, 'id': 0, 'sql': 'select * from SSB.P_LINEORDER', 'sql_advices': []}])
    wrapper.vm.handleSelectionChange('', {capable: true, id: 0, sql: 'select * from SSB.P_LINEORDER', sql_advices: []})
    expect(wrapper.vm.selectSqls).toEqual([])

    handleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback) => {
      return new Promise((resolve) => {
        res ? callback(res) : callback({...importSqlFilesData, capable: false}, '000', 200, 'over sizes')
        resolve(res)
      })
    })
    wrapper.vm.validateWhiteSql()
    expect(wrapper.vm.inputHeight).toBe(339)
    expect(wrapper.vm.whiteMessages).toBe()
    expect(wrapper.vm.isWhiteErrorMessage).toBeTruthy()

    handleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback) => {
      return new Promise((resolve) => {
        res ? callback(res) : callback({...importSqlFilesData, capable: false}, '999', 200, 'over sizes')
        resolve(res)
      })
    })
    wrapper.vm.validateWhiteSql().catch()
  })
})

describe('UploadSql Store', () => {
  UploadSqlModel.actions['CALL_MODAL'](root, {isGenerateModel: true})
  expect(UploadSqlModel.state.isGenerateModel).toBeTruthy()
  expect(UploadSqlModel.state.isShow).toBeTruthy()

  UploadSqlModel.mutations['RESET_MODAL_FORM'](root.state)
  expect(UploadSqlModel.state.isGenerateModel).toBeFalsy()
})
