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
  editType: 'new',
  callback: null,
  totalUsers: [],
  form: {
    groupName: '',
    selectedUsers: []
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
        switch (key) {
          // 有group则设置modal from数据
          case 'form':
            payload.group && setModalForm(payload, state)
            break
          // 设置modal的数据
          default: {
            payload[key] && (state[key] = payload[key])
            break
          }
        }
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, { editType, group }) {
      return new Promise(resolve => {
        commit(types.SET_MODAL, { editType, group, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

function setModalForm (payload, state) {
  const { key, value } = payload.group

  key && (state.form.groupName = key)
  value && (state.form.selectedUsers = value)
}

export { types }
