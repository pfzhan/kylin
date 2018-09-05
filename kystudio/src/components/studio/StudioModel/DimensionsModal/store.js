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
  modelTables: [],
  selectedDimensions: [],
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
      state.modelTables = payload.modelTables
      state.selectedDimensions = payload.selectedDimensions
    },
    // 还原Modal中的值为初始值
    [types.RESET_MODAL_FORM]: (state) => {
      state.selectedDimensions = []
      state.modelTables = []
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { modelTables, selectedDimensions }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { modelTables: modelTables, selectedDimensions: selectedDimensions })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
