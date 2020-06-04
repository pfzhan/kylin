import api from './../service/api'
import * as types from './types'

export default {
  state: {
    nodeList: [],
    maintenance_mode: false
  },
  mutations: {
    [types.SET_NODES_LIST] (state, data) {
      state.nodeList = data.servers
      state.maintenance_mode = data.status.maintenance_mode
    }
  },
  actions: {
    // 获取节点信息
    [types.GET_NODES_LIST] ({ commit, dispatch }, paras) {
      return new Promise((resolve, reject) => {
        api.system.loadOnlineNodes(paras).then(res => {
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
    isOnlyQueryNode (state) {
      return state.nodeList.filter(it => it.mode === 'query').length === state.nodeList.length
    }
  }
}
