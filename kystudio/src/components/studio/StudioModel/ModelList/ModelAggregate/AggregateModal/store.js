const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL'
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
  editType: 'new',
  callback: null,
  model: null,
  form: {
    aggregateArray: [
      JSON.parse(initialAggregateData)
    ]
  }
})

export default {
  state: JSON.parse(initialState),
  mutations: {
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.form = {
        ...state.form,
        ...payload
      }
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
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { editType, model }) {
      return new Promise(resolve => {
        console.log(model)
        commit(types.SET_MODAL, { editType, model, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
