const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  INIT_FORM: 'INIT_FORM',
  CALL_MODAL: 'CALL_MODAL',
  RESET_MODAL: 'RESET_MODAL'
}
const initialState = JSON.stringify({
  isShow: false,
  editType: '',
  callback: null,
  form: {
    project: '',
    tables: []
  },
  project: null,
  tables: []
})

export default {
  state: JSON.parse(initialState),
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
    [types.SET_MODAL]: (state, payload = {}) => {
      for (const key in payload) {
        state[key] = payload[key]
      }
    },
    [types.INIT_FORM]: (state) => {
      const { project = {} } = state
      state.form.project = project.name
    },
    [types.RESET_MODAL]: (state) => {
      const newState = JSON.parse(initialState)
      for (const key in state) {
        state[key] = newState[key]
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { editType = 'batchLoad', project }) {
      return new Promise(async (resolve, reject) => {
        commit(types.SET_MODAL, { editType, project, callback: resolve })
        commit(types.INIT_FORM)
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
