import api from './../service/api'
import * as types from './types'

export default {
  state: {
    nodeList: []
  },
  mutations: {
    [types.SET_NODES_LIST] (state, list) {
      console.log(list)
      state.nodeList = list
    }
  },
  actions: {
    // 获取节点信息
    [types.GET_NODES_LIST] ({ commit, dispatch }, paras) {
      return new Promise((resolve, reject) => {
        api.datasource.loadOnlineQueryNodes(paras).then(res => {
          const { data, code } = res.data
          if (code === '000') {
            commit(types.SET_NODES_LIST, data)
          }
          resolve(res)
        }).catch((e) => {
          reject(e)
        })
      })
    }
  },
  getters: {
    allNodeNumber (state) {
      return state.nodeList.filter(it => it.mode === 'all').length
    }
  }
}
