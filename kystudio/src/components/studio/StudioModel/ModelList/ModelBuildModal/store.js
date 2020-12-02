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
  type: '',
  title: '',
  isAddSegment: false,
  buildOrComp: 'build',
  isHaveSegment: false,
  disableFullLoad: false,
  form: {
    modelDesc: ''
  },
  callback: null,
  source: ''
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
      state.form.modelDesc = payload.modelDesc
      state.title = payload.title
      state.type = payload.type
      state.source = payload.source || ''
      state.isAddSegment = payload.isAddSegment
      state.isHaveSegment = payload.isHaveSegment
      state.disableFullLoad = payload.disableFullLoad
      state.callback = payload.callback
    },
    [types.RESET_MODAL_FORM]: (state) => {
      state.form = JSON.parse(initialState).form
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { modelDesc, title, type, source = '', isAddSegment, isHaveSegment, disableFullLoad }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL_FORM, {modelDesc: modelDesc, title: title, source, type: type, isAddSegment: isAddSegment, isHaveSegment: isHaveSegment, disableFullLoad: disableFullLoad, callback: resolve})
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
