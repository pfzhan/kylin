import { editTypes, volatileTypes } from './handler'
import { handleSuccessAsync } from '../../../util'
import { partitionColumnTypes } from '../../../config'

const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  INIT_FORM: 'INIT_FORM'
}
// 声明：初始state状态
const initialState = JSON.stringify({
  isShow: false,
  editType: '',
  callback: null,
  table: null,
  disabled: false,
  projectName: null,
  model: null,
  form: {
    newDataRange: [new Date(), new Date()],
    freshDataRange: [new Date(), new Date()],
    partitionColumn: '',
    isMergeable: true,
    isAutoMerge: true,
    isVolatile: true,
    autoMergeConfigs: [],
    volatileConfig: {
      value: 0,
      type: 'DAY'
    },
    isPushdownSync: true
  }
})

export default {
  state: JSON.parse(initialState),
  getters: {
    tableFullName (state) {
      return state.table
        ? `${state.table.database}.${state.table.name}`
        : ''
    },
    partitionColumns (state) {
      return state.table
        ? state.table.columns.filter(column => partitionColumnTypes.includes(column.datatype))
        : []
    },
    modelName (state) {
      return state.model.name
    }
  },
  mutations: {
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.form = { ...state.form, ...payload }
    },
    [types.SHOW_MODAL]: (state) => {
      state.isShow = true
    },
    [types.HIDE_MODAL]: (state) => {
      state.isShow = false
    },
    [types.RESET_MODAL_FORM]: (state) => {
      state.form = JSON.parse(initialState).form
    },
    [types.SET_MODAL]: (state, payload) => {
      for (const key in state) {
        payload[key] && (state[key] = payload[key])
      }
    },
    [types.INIT_FORM]: (state, payload) => {
      if (state.table) {
        const [ startTime, endTime ] = state.table.userRange
        state.form.newDataRange = [ new Date(startTime), new Date(endTime) ]
        state.form.freshDataRange = [ new Date(startTime), new Date(endTime) ]
      }
      state.form = { ...state.form, ...payload }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { editType, projectName = null, table = null, model = null }) {
      const { dispatch } = this
      return new Promise(async resolve => {
        const tableFullName = table && `${table.database}.${table.name}`
        const modelName = model && model.name

        commit(types.SET_MODAL, { editType, table, model, callback: resolve })

        switch (editType) {
          case editTypes.DATA_MERGE: {
            const response = await dispatch('FETCH_MERGE_CONFIG', { projectName, modelName, tableFullName })
            const payload = formatMergeConfig(await handleSuccessAsync(response))
            commit(types.INIT_FORM, payload)
            break
          }
          case editTypes.PUSHDOWN_CONFIG: {
            const tableFullName = `${table.database}.${table.name}`
            const response = await dispatch('FETCH_PUSHDOWN_CONFIG', { projectName, tableFullName })
            const isPushdownSync = await handleSuccessAsync(response)
            commit(types.INIT_FORM, { isPushdownSync })
            break
          }
          default: {
            commit(types.INIT_FORM)
          }
        }
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

function formatMergeConfig (response) {
  const isAutoMerge = response.auto_merge
  const isVolatile = response.volatile_range.volatile_range_available
  const autoMergeConfigs = response.auto_merge_time_ranges
  const volatileConfig = {
    value: response.volatile_range.volatile_range_number,
    type: response.volatile_range.volatile_range_type || volatileTypes[1]
  }
  const isMergeable = isAutoMerge && isVolatile
  return { isMergeable, isAutoMerge, isVolatile, autoMergeConfigs, volatileConfig }
}

export { types }
