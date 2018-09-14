const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL'
}
// 声明：初始state状态
const initialState = JSON.stringify({
  isShow: false,
  addType: '',
  callback: null,
  form: {
    dimensionColumn: ''
  }
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
      state.addType = payload.addType
      state.callback = payload.callback
      state.form.dimensionColumn = payload.dimensionColumn
    },
    // 还原Modal中的值为初始值
    [types.RESET_MODAL_FORM]: (state) => {
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, options) {
      return new Promise(resolve => {
        if (options) {
          commit(types.SET_MODAL_FORM, {callback: resolve, addType: options.addType, dimensionColumn: options.dimensionColumn})
        }
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
