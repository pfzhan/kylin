import { shallow } from 'vue-test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import * as business from '../../../../util/business'
import Vuex from 'vuex'
import SuggestModel from '../../UploadSql/SuggestModel.vue'

jest.useFakeTimers()

const mockApi = {
  mockValidateModelName: jest.fn().mockImplementation(() => {
    return {
      then: (callback, errorCallback) => {
        callback()
        errorCallback()
      }
    }
  })
}
const mockEventListener = jest.spyOn(document, 'addEventListener').mockImplementation()
const mockRemoveEventListener = jest.spyOn(document, 'removeEventListener').mockImplementation()
const mockHandleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback) => {
  callback({})
})
const mockHandleError = jest.spyOn(business, 'handleError').mockImplementation(() => {})

const store = new Vuex.Store({
  state: {},
  getters: {
    currentSelectedProject () {
      return 'learn_kylin'
    }
  },
  actions: {
    'VALIDATE_MODEL_NAME': mockApi.mockValidateModelName
  }
})

const wrapper = shallow(SuggestModel, {
  localVue,
  store,
  propsData: {
    suggestModels: [{
      alias: 'AUTO_MODEL_LINEORDER_1',
      rec_items: [{
        computed_columns: [{ cc: {columnName: 'CC'} }],
        dimensions: [{ dimension: {name: 'LINEORDER_AUTO_1'} }],
        measures: [],
        modelId: '70e81e2a-aaca-4885-b811-287fb462bd2f',
        project: 'xm_test_1',
        index_id: 3
      }],
      uuid: '70e81e2a-aaca-4885-b811-287fb46223kdws',
      isChecked: true,
      isNameError: false
    }],
    tableRef: 'modelsTable',
    isOriginModelsTable: true,

  }
})

wrapper.vm.$refs = {
  modelsTable: {
    toggleRowSelection: jest.fn(),
    doLayout: jest.fn()
  }
}

describe('Component SuggestModel', () => {
  it('init', () => {
    expect(mockEventListener.mock.calls[0][0]).toBe('click')
    expect(wrapper.vm.$refs.modelsTable.toggleRowSelection).toBeCalled()
    expect(wrapper.vm.$refs.modelsTable.doLayout).toBeCalled()
  })
  it('computed', async () => {
    expect(wrapper.vm.modelTips).toBe('You can observe the number of new recommendations for existing models and expand to see which SQL these recommendations come from. The new recommendations have been added to the recommendation lists of the corresponding models, where you can accept the required recommendations.')
    wrapper.setProps({isOriginModelsTable: false})
    await wrapper.update()
    expect(wrapper.vm.modelTips).toBe('You can select the new models you want and expand to see which SQL these models come from. Click the Submit button and the new models will be added to the model list, which you can view and edit on the model page.')

    expect(wrapper.vm.hasRecommendation).toBeTruthy()
  })
  it('methods', async () => {
    const event = {
      target: {
        closest: jest.fn().mockImplementation((res) => {
          return false
        })
      }
    }
    wrapper.vm.handleClickEvent(event)
    expect(wrapper.vm.$data.activeRowId).toBe('')

    expect(wrapper.vm.setRowClass({row: {uuid: '70e81e2a-aaca-4885-b811-287fb462bd2f'}})).toBe('row-click')
    wrapper.setData({ activeRowId: '70e81e2a-aaca-4885-b811-287fb462bd2f' })
    expect(wrapper.vm.setRowClass({row: {uuid: '70e81e2a-aaca-4885-b811-287fb462bd2f'}})).toBe('active-row')

    wrapper.vm.modelRowClick({
      uuid: '70e81e2a-aaca-4885-b811-287fb462bd2f',
      rec_items: [{
        measures: [{measure: {id: 100000, name: 'P_LINEORDER_LO_ORDERKEY_SUM', function: {expression: 'SUM'}}, value: true}],
        dimensions: [{dimension: {column: 'PART.P_PARTKEY', id: 26, name: 'PART_0_DOT_0_P_PARTKEY', status: 'DIMENSION'}, value: false}],
        computed_columns: []
      }]
    })
    expect(wrapper.vm.$data.activeRowId).toBe('70e81e2a-aaca-4885-b811-287fb462bd2f')
    expect(wrapper.vm.$data.modelDetails).toEqual([{"measure": {"function": {"expression": "SUM"}, "id": 100000, "name": "P_LINEORDER_LO_ORDERKEY_SUM"}, "name": "P_LINEORDER_LO_ORDERKEY_SUM", "type": "measure", "value": true}, {"dimension": {"column": "PART.P_PARTKEY", "id": 26, "name": "PART_0_DOT_0_P_PARTKEY", "status": "DIMENSION"}, "name": "PART_0_DOT_0_P_PARTKEY", "type": "dimension", "value": false}])

    // console.log(wrapper.vm.$options.propsData.suggestModels, 77777)
    wrapper.setProps({
      suggestModels: [
        ...wrapper.vm.suggestModels,
        {
          alias: 'AUTO_MODEL_LINEORDER_1',
          isChecked: false,
          isNameError: false,
          fact_table: 'SSB.LINEORDER',
          rec_items: [{
            computed_columns: [],
            dimensions: [],
            measures: [],
            modelId: '70e81e2a-aaca-4885-b811-287fb462bd2f',
            project: 'xm_test_1',
            index_id: 1
          }],
          uuid: '70e81e2a-aaca-4885-b811-287fb46223'
        }
      ]
    })
    await wrapper.update()

    wrapper.vm.handleSelectionModel([], wrapper.vm.suggestModels[1])
    expect(wrapper.vm.$data.modelNameError).toEqual('Model with the same name existed.')
    expect(wrapper.vm.$data.isNameErrorModelExisted).toBeTruthy()
    expect(wrapper.emitted().isValidated).toEqual([[true]])

    wrapper.setProps({
      suggestModels: [
        ...wrapper.vm.suggestModels,
        {
          alias: '@AUTO_MODEL_LINEORDER_1',
          isChecked: false,
          isNameError: false,
          fact_table: 'SSB.LINEORDER',
          rec_items: [{
            computed_columns: [{ cc: {columnName: 'CC'} }],
            dimensions: [{ dimension: {name: 'LINEORDER_AUTO_1'} }],
            measures: [],
            modelId: '70e81e2a-aaca-4885-b811-287fb46223dwe33',
            project: 'xm_test_1',
            index_id: 5
          }],
          uuid: '70e81e2a-aaca-4885-b811-287fb46223dwe33'
        }
      ]
    })
    await wrapper.update()

    wrapper.vm.handleSelectionModel([], wrapper.vm.suggestModels[2])
    expect(wrapper.vm.$data.modelNameError).toEqual('Only supports number, letter and underline.')
    expect(wrapper.vm.$data.isNameErrorModelExisted).toBeTruthy()
    expect(wrapper.emitted().isValidated[1]).toEqual([true])

    wrapper.vm.handleRename({
      isNameError: false,
      isChecked: true,
      alias: 'PART_1',
      uuid: '70e81e2a-aaca-4885-b811-2341321233'
    })
    expect(mockApi.mockValidateModelName.mock.calls[0][1]).toEqual({'alias': 'PART_1', 'project': 'learn_kylin', 'uuid': '70e81e2a-aaca-4885-b811-2341321233'})
    // expect(mockHandleSuccess).toBeCalled()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.handleSelectionRecommendationChange(wrapper.vm.suggestModels[0].rec_items)
    expect('getSelectRecommends' in wrapper.emitted()).toBeTruthy()


    wrapper.vm.handleSelectionAllModel(wrapper.vm.suggestModels)
    expect(wrapper.vm.suggestModels.filter(it => it.isChecked).length).toBe(3)

    wrapper.vm.handleSelectionAllModel([])
    expect(wrapper.vm.$props.suggestModels.filter(it => !it.isChecked).length).toBe(3)

    wrapper.vm.handleSelectionModelChange([])
    expect(wrapper.vm.selectModels).toEqual([])
    expect(wrapper.emitted().getSelectModels[0]).toEqual([[]])

    wrapper.vm.handleSelectionRecommends(null, wrapper.vm.suggestModels[0])
    expect(wrapper.vm.suggestModels[0].isChecked).toBeTruthy()

    expect(wrapper.vm.sqlsTable(['select * from SSB'])).toEqual([{sql: 'select * from SSB'}])

    wrapper.vm.recommendRowClick(wrapper.vm.suggestModels[0].rec_items[0])
    expect(wrapper.vm.$data.activeRowId).toBe(3)
    expect(wrapper.vm.$data.recommendDetails).toEqual([{'cc': {'columnName': 'CC'}, 'name': 'CC', 'type': 'cc'}, {'name': 'LINEORDER_AUTO_1', 'type': 'dimension'}])

    expect(wrapper.vm.renderHeaderSql(jest.fn(), {column: '', index: 0})).toEqual()

    wrapper.destroy()
    expect(mockRemoveEventListener.mock.calls[0][0]).toBe('click')
  })
})

// describe('origin models', () => {
//   it('init', () => {
//     const wrapper1 = shallow(SuggestModel, {
//       localVue,
//       store,
//       propsData: {
//         suggestModels: [{
//           alias: 'AUTO_MODEL_LINEORDER_1',
//           rec_items: [{
//             computed_columns: [{ cc: {columnName: 'CC'} }],
//             dimensions: [{ dimension: {name: 'LINEORDER_AUTO_1'} }],
//             measures: [],
//             modelId: '70e81e2a-aaca-4885-b811-287fb462bd2f',
//             project: 'xm_test_1',
//             index_id: 3
//           }],
//           uuid: '70e81e2a-aaca-4885-b811-287fb46223kdws',
//           isChecked: true,
//           isNameError: false
//         }],
//         tableRef: 'originTable',
//         isOriginModelsTable: false,
//       }
//     })

//     wrapper1.vm.$refs = {
//       originTable: {
//         toggleRowSelection: jest.fn()
//       }
//     }

//     jest.runAllTimers()

//     expect(wrapper1.vm.$refs.originTable.toggleRowSelection).toBeCalled()

//     jest.clearAllTimers()
//   })
// })
