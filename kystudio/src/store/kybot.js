import api from './../service/api'
import * as types from './types'
export default {
  state: {
    kyaccount: '',
    kyStatus: false,
    loginKyaccountDialog: false,
    hasLoginAccount: ''
  },
  mutations: {
    [types.GET_KYACCOUNT]: function (state, { data }) {
      state.kyaccount = data
    },
    [types.GET_KYSTATUS]: function (state, { data }) {
      state.kyStatus = data
    }
  },
  actions: {
    [types.GET_KYBOT_ACCOUNT]: function ({ commit }) {
      return api.kybot.getKyAccount().then((response) => {
        commit(types.GET_KYACCOUNT, { data: response.data })
        return response
      })
    },
    [types.GET_CUR_ACCOUNTNAME]: function ({ commit }) {
      return api.kybot.getCurrentAccountName()
    },
    [types.LOGIN_KYBOT]: function ({ commit }, params) {
      return api.kybot.loginKybot(params)
    },
    [types.LOGOUT_KYBOT]: function ({ commit }, params) {
      return api.kybot.kybotLogOut(params)
    }
  },
  getters: {}
}
