const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  INIT_FORM: 'INIT_FORM'
}
// 声明：初始state状态
const initialState = JSON.stringify({
  isShow: false,
  sourceType: null,
  callback: null,
  form: {
    project: null,
    // for newten
    // sourceType: 0,
    // should remove
    selectedTables: [],
    selectedDatabases: [],
    sourceType: null,
    isAutoUpdate: false
  }
})

export default {
  // state深拷贝
  state: JSON.parse(initialState),
  mutations: {
    // 设置Modal中Form的field值
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.form = {
        ...state.form,
        ...payload
      }
    },
    // 显示Modal弹窗
    [types.SHOW_MODAL]: (state) => {
      state.isShow = true
    },
    // 隐藏Modal弹窗
    [types.HIDE_MODAL]: (state) => {
      state.isShow = false
    },
    // 还原Modal中Form的值为初始值
    [types.RESET_MODAL_FORM]: (state) => {
      state.form = JSON.parse(initialState).form
    },
    // 设置Modal中的值
    [types.SET_MODAL]: (state, payload) => {
      for (const key of Object.keys(state)) {
        payload[key] !== undefined && (state[key] = payload[key])
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { sourceType, project }) {
      return new Promise(resolve => {
        // sourceType转String是为了兼容没有数据源，sourceType为"undefined"的情况
        if (sourceType === undefined) {
          sourceType = String(sourceType)
        }
        if (project) {
          project = JSON.parse(JSON.stringify(project))
          project.override_kylin_properties['kylin.source.default'] = null
        }
        commit(types.SET_MODAL, { sourceType, callback: resolve })
        commit(types.SET_MODAL_FORM, { project })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
