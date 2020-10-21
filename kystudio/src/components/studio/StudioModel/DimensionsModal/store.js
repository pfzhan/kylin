const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  UPDATE_SYNC_NAME: 'UPDATE_SYNC_NAME',
  COLLECT_OTHER_COLUMNS: 'COLLECT_OTHER_COLUMNS'
}
// 声明：初始state状态
const initialState = JSON.stringify({
  isShow: false,
  modelDesc: [],
  selectedDimensions: [],
  callback: null,
  syncCommentToName: false,
  otherColumns: [] // 同步注释传递没有选中的dimensions
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
      state.modelDesc = payload.modelDesc
      state.callback = payload.callback
    },
    // 还原Modal中的值为初始值
    [types.RESET_MODAL_FORM]: (state) => {
      state.selectedDimensions = []
      state.modelDesc = null
      state.syncCommentToName = false
    },
    [types.UPDATE_SYNC_NAME]: (state) => {
      state.syncCommentToName = !state.syncCommentToName
    },
    [types.COLLECT_OTHER_COLUMNS]: (state, list) => {
      state.otherColumns = list
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { modelDesc }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { modelDesc: modelDesc, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
