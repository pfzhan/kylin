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
    },
    [types.GET_KYSTATUS]: function (state, { data }) {
      state.kyStatus = data
    }
  },
  actions: {
    [types.GET_KYBOT_ACCOUNT]: function () {
      // console.log('xxxxxx')
      return api.kybot.getKyAccount().then((response) => {
        console.warn('get kybot account :', response)
        // commit(types.GET_KYACCOUNT({ data: response.data }))
      })
    },
    [types.LOGIN_KYBOT]: function ({ commit }, params) {
      return api.kybot.loginKybot(params)
    },
    [types.GET_KYBOT_STATUS]: function ({commit}) {
      return api.kybot.getkybotStatus().then((response) => {
        commit(types.GET_KYSTATUS, {data: response})
      })
    },
    [types.START_KYBOT]: function () {
      return api.kybot.startKybot()
    },
    [types.STOP_KYBOT]: function () {
      return api.kybot.stopKybot()
    }
  },
  getters: {}
}
