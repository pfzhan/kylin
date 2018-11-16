import dayjs from 'dayjs'
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
    partitionFormat: '',
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
      return getTableName(state.table, state.model)
    },
    partitionColumns (state) {
      return state.table
        ? state.table.columns.filter(column => partitionColumnTypes.some(partitionColumnType => {
          return partitionColumnType.test(column.datatype)
        }))
        : []
    },
    modelName (state) {
      return !getTableName(state.table, state.model) && state.model ? state.model.name : null
    },
    partitionFormats () {
      return [
        { name: 'COMPACT_DATE_PATTERN', value: 'yyyyMMdd' },
        { name: 'DEFAULT_DATE_PATTERN', value: 'yyyy-MM-dd' },
        { name: 'DEFAULT_TIME_PATTERN', value: 'HH:mm:ss' },
        { name: 'DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS', value: 'yyyy-MM-dd HH:mm:ss' },
        { name: 'DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS', value: 'yyyy-MM-dd HH:mm:ss.SSS' }
      ]
    }
  },
  mutations: {
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.form = { ...state.form, ...payload }
      fixDataRange(state.form.newDataRange || [])
      fixDataRange(state.form.freshDataRange || [])
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
    [types.INIT_FORM]: (state, payload = {}) => {
      for (const [ key, value ] of Object.entries(payload)) {
        if (!['newDataRange', 'freshDataRange'].includes(key)) {
          state.form[key] = value
        }
      }
      const tableRange = state.table && state.table.userRange || []
      const inputDateRange = payload && payload.newDataRange || []
      state.form.newDataRange = state.form.newDataRange.map((date, index) => {
        const newDate = inputDateRange[index] || tableRange[index]
        return newDate ? new Date(newDate) : ''
      })
      state.form.freshDataRange = [ new Date(tableRange[0]), new Date(tableRange[1]) ]
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { editType, projectName = null, table = null, model = null, newDataRange = null }) {
      const { dispatch } = this
      return new Promise(async (resolve, reject) => {
        try {
          const tableFullName = getTableName(table, model)
          const modelName = (!tableFullName && model) ? model.name : null

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
            case editTypes.INCREMENTAL_LOADING: {
              commit(types.INIT_FORM, { newDataRange })
              break
            }
            default: {
              commit(types.INIT_FORM)
            }
          }
          commit(types.SHOW_MODAL)
        } catch (e) {
          reject(e)
        }
      })
    }
  },
  namespaced: true
}

function formatMergeConfig (response) {
  const isAutoMerge = response.auto_merge_enabled
  const isVolatile = response.volatile_range.volatile_range_enabled
  const autoMergeConfigs = response.auto_merge_time_ranges
  const volatileConfig = {
    value: response.volatile_range.volatile_range_number,
    type: response.volatile_range.volatile_range_type || volatileTypes[1]
  }
  const isMergeable = isAutoMerge && isVolatile
  return { isMergeable, isAutoMerge, isVolatile, autoMergeConfigs, volatileConfig }
}

function getTableName (table, model) {
  if (table) {
    return `${table.database}.${table.name}`
  } else if (model) {
    return model.fact_table
  }
}

function fixDataRange (dataRange) {
  dataRange[0] && (dataRange[0] = dayjs(dataRange[0]).toDate())
  dataRange[1] && (dataRange[1] = dayjs(dataRange[1]).toDate())
}

export { types }
