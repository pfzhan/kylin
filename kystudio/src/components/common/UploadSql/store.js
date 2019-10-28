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
  isGenerateModel: false,
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
    [types.SET_MODAL]: (state, payload) => {
      state.isGenerateModel = payload.isGenerateModel
      state.callback = payload.callback
    },
    // 还原Modal中的值为初始值
    [types.RESET_MODAL_FORM]: (state) => {
      state.isGenerateModel = false
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { isGenerateModel }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { isGenerateModel: isGenerateModel, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
