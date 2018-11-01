const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL'
}
// 声明：初始state状态
const initialState = JSON.stringify({
  isShow: false,
  form: {
    data: {
      modelInstance: null,
      tableIndexDesc: null
    }
  },
  callback: null
})

export default {
  // state深拷贝
  state: JSON.parse(initialState),
  mutations: {
    // 显示Modal弹窗
    [types.SHOW_MODAL]: (state) => {
      state.isShow = true
    },
    // 隐藏Modal弹窗
    [types.HIDE_MODAL]: (state) => {
      state.isShow = false
    },
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.form.data = payload.data
      state.callback = payload.callback
    },
    [types.RESET_MODAL_FORM]: (state) => {
      state.form = JSON.parse(initialState).form
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, data) {
      return new Promise(resolve => {
        commit(types.SET_MODAL_FORM, {data: data, callback: resolve})
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
