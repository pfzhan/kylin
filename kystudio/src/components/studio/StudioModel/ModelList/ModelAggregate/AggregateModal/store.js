import { handleSuccessAsync, getFullMapping } from '../../../../../../util'

const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  INIT_FORM: 'INIT_FORM'
}
export const initialAggregateData = JSON.stringify({
  id: 0,
  includes: [],
  mandatory: [],
  jointArray: [{
    id: 0,
    items: []
  }],
  hierarchyArray: [{
    id: 0,
    items: []
  }]
})
const initialState = JSON.stringify({
  isShow: false,
  editType: 'edit',
  callback: null,
  model: null,
  projectName: null,
  form: {
    aggregateArray: [
      JSON.parse(initialAggregateData)
    ]
  }
})

export default {
  state: JSON.parse(initialState),
  getters: {
    dimensions (state) {
      if (state.model) {
        return state.model.all_named_columns
          .filter(column => column.status === 'DIMENSION')
          .map(dimension => ({
            label: dimension.column,
            value: dimension.column,
            id: dimension.id
          }))
      } else {
        return []
      }
    },
    dimensionIdMapping (state, getters) {
      const { dimensions } = getters
      const mapping = dimensions.reduce((mapping, item) => {
        mapping[item.value] = item.id
        return mapping
      }, {})
      return getFullMapping(mapping)
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
      for (const key of Object.keys(state)) {
        payload[key] && (state[key] = payload[key])
      }
    },
    [types.INIT_FORM]: (state, payload) => {
      if (payload) {
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { editType, projectName, model }) {
      const { dispatch } = this

      return new Promise(async (resolve, reject) => {
        const modelName = model && model.name

        commit(types.SET_MODAL, { editType, model, projectName, callback: resolve })
        const response = await dispatch('FETCH_AGGREGATE_GROUPS', { projectName, modelName })
        const aggregateGroupRule = await handleSuccessAsync(response)
        commit(types.INIT_FORM, aggregateGroupRule)
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
