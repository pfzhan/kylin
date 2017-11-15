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
    routerConfig: {
      currentPathName: ''
    },
    overLock: localStorage.getItem('buyit'),
    hiddenFeature: {
      raw_measure: false,
      extendedcolumn_measure: false
    },
    loginKyaccountDialog: false
  },
  mutations: {
    [types.SAVE_DEFAULT_CONFIG]: function (state, { list, type }) {
      state.defaultConfig[type] = list
    },
    [types.SAVE_HIDDEN_FEATURE]: function (state, {value, name}) {
      state.hiddenFeature[name] = value
    }
  },
  actions: {
    [types.LOAD_DEFAULT_CONFIG]: function ({ commit }, type) {
      return api.config.getDefaults(type).then((response) => {
        commit(types.SAVE_DEFAULT_CONFIG, { type: type, list: response.data.data })
      })
    },
    [types.LOAD_HIDDEN_FEATURE]: function ({ commit }, feature) {
      return api.config.hiddenMeasure(feature).then((response) => {
        commit(types.SAVE_HIDDEN_FEATURE, {value: response.data.data, name: feature.feature_name})
      })
    }
  },
  getters: {
    briefMenuGet (state) {
      return state.layoutConfig.briefMenu
    }
  }
}

