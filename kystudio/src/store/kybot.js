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
    },
    [types.GET_KYBOT_STATUS]: function ({commit}) {
      return api.kybot.getkybotStatus().then((response) => {
        // response = {
        //   code: '000',
        //   data: true,
        //   msg: ''
        // }
        commit(types.GET_KYSTATUS, {data: response})
        return response
      })
    },
    [types.START_KYBOT]: function ({ commit }) {
      return api.kybot.startKybot().then((response) => {
        // response = {
        //   code: '000',
        //   data: true,
        //   msg: ''
        // }
        return response
      })
    },
    [types.STOP_KYBOT]: function ({ commit }) {
      return api.kybot.stopKybot().then((response) => {
        // response = {
        //   code: '000',
        //   data: true,
        //   msg: ''
        // }
        return response
      })
    },
    // setAgreement
    // 获取是否已同意协议
    [types.GET_AGREEMENT]: function ({ commit }) {
      return api.kybot.getAgreement().then((resp) => {
        return resp
      })
    },
    [types.SET_AGREEMENT]: function ({ commit }) {
      return api.kybot.setAgreement().then((resp) => {
        return resp
      })
    }
  },
  getters: {}
}
