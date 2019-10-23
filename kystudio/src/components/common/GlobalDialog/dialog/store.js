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
  details: [],
  showDetailBtn: false, // 默认设为不显示详情按钮，如果默认显示，配置为不显示的弹窗，在关闭时会闪现详情按钮
  showCopyBtn: false
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
      // 为兼容以前没传参数，后来传参数，默认不可显示，会有闪现问题，所以默认隐藏，再通过参数判断是否传入，进行动态赋值
      // 如果不传 showDetailBtn 参数，就默认是显示的，如果传了，就取传的值
      payload['showDetailBtn'] = payload['showDetailBtn'] === undefined ? true : payload['showDetailBtn']
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
    [types.CALL_MODAL] ({ commit }, { dialogType = 'error', msg, title, details = [], theme = 'plain', showDetailBtn = true, showCopyBtn = false }) {
      return new Promise(async (resolve, reject) => {
        commit(types.SET_MODAL, { dialogType, msg, title, details, theme, showDetailBtn, showCopyBtn, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
