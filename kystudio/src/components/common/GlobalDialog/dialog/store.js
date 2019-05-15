const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  CALL_MODAL: 'CALL_MODAL',
  RESET_MODAL: 'RESET_MODAL'
}
const initialState = JSON.stringify({
  isShow: false,
  callback: null,
  msg: '',
  title: '',
  dialogType: '',
  theme: '',
  details: []
})

export default {
  state: JSON.parse(initialState),
  mutations: {
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
    [types.RESET_MODAL]: (state) => {
      const newState = JSON.parse(initialState)
      for (const key in state) {
        if (newState[key]) {
          state[key] = newState[key]
        }
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { dialogType = 'error', msg, title, details = [], theme = 'plain' }) {
      return new Promise(async (resolve, reject) => {
        commit(types.SET_MODAL, { dialogType, msg, title, details, theme, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
