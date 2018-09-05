import api from './../service/api'
import * as types from './types'
export default {
  state: {
    encodingTip: {
      dict: 'dicTip',
      fixed_length: 'fixedLengthTip',
      int: 'intTip',
      integer: 'integerTip',
      fixed_length_hex: 'fixedLengthHexTip',
      date: 'dataTip',
      time: 'timeTip',
      boolean: 'booleanTip',
      orderedbytes: 'orderedbytesTip'
    },
    defaultConfig: {
      cube: {},
      project: {}
    },
    layoutConfig: {
      briefMenu: localStorage.getItem('menu_type'),
      gloalProjectSelectShow: true
    },
    errorMsgBox: {
      isShow: false,
      msg: '',
      detail: ''
    },
    showLoadingBox: false,
    routerConfig: {
      currentPathName: ''
    },
    overLock: localStorage.getItem('buyit'),
    loginKyaccountDialog: false
  },
  mutations: {
    [types.SAVE_DEFAULT_CONFIG]: function (state, { list, type }) {
      state.defaultConfig[type] = list
    }
  },
  actions: {
    [types.LOAD_DEFAULT_CONFIG]: function ({ commit }, type) {
      return api.config.getDefaults(type).then((response) => {
        commit(types.SAVE_DEFAULT_CONFIG, { type: type, list: response.data.data })
      })
    }
  },
  getters: {
    briefMenuGet (state) {
      return state.layoutConfig.briefMenu
    },
    currentPathNameGet (state) {
      return state.routerConfig.currentPathName
    }
  }
}

