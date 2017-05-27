import api from './../service/api'
import * as types from './types'
export default {
  state: {
    kyaccount: '',
    kyStatus: false
  },
  mutations: {
    [types.GET_KYACCOUNT]: function (state, { data }) {
      state.kyaccount = data
      console.log('state.kyaccount :', state.kyaccount)
    },
    [types.GET_KYSTATUS]: function (state, { data }) {
      state.kyStatus = data
      console.log('state.kyStatus ::是否已开启：', state.kyStatus)
    }
  },
  actions: {
    [types.GET_KYBOT_ACCOUNT]: function ({ commit }) {
      return api.kybot.getKyAccount().then((response) => {
        console.warn('get kybot account :', response)
        commit(types.GET_KYACCOUNT, { data: response.data })
        return response
      })
    },
    [types.LOGIN_KYBOT]: function ({ commit }, params) {
      return api.kybot.loginKybot(params)
    },
    [types.GET_KYBOT_STATUS]: function ({commit}) {
      return api.kybot.getkybotStatus().then((response) => {
        // response = {
        //   code: '000',
        //   data: true,
        //   msg: ''
        // }
        commit(types.GET_KYSTATUS, {data: response})
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
