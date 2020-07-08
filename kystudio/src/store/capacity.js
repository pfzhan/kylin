import api from './../service/api'
import * as types from './types'
import { handleError } from '../util'
import filterElements from '../filter/index'

let nodeTimer = null
let capacityTimer = null

export default {
  state: {
    nodeList: [],
    maintenance_mode: false,
    systemNodeInfo: {
      current_node: 0,
      node: 0,
      error: false,
      isLoading: false,
      fail: false,
      unlimited: false
    },
    systemCapacityInfo: {
      current_capacity: 0,
      capacity: 0,
      error: false,
      isLoading: false,
      error_over_thirty_days: false,
      fail: false,
      unlimited: false
    },
    latestUpdateTime: 0,
    isRefresh: false,
    capacityAlert: null
  },
  mutations: {
    [types.SET_NODES_LIST] (state, data) {
      state.nodeList = data.servers
      state.maintenance_mode = data.status.maintenance_mode
    },
    [types.SET_NODES_INFOS] (state, data) {
      state.systemNodeInfo = {...state.systemNodeInfo, ...data}
    },
    [types.SET_SYSTEM_CAPACITY_INFO] (state, data) {
      if (state.isRefresh) {
        data.isLoading = true
      }
      state.systemCapacityInfo = {...state.systemCapacityInfo, ...data}
    },
    'LATEST_UPDATE_TIME' (state) {
      state.latestUpdateTime = new Date().getTime()
    },
    'UPDATE_REFRESH_STATUS' (state) {
      state.isRefresh = !state.isRefresh
      state.systemCapacityInfo = {...state.systemCapacityInfo, isLoading: state.isRefresh}
    },
    'CAPACITY_ALERT_TYPE' (state, data) {
      state.capacityAlert = data
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
            resolve(data)
          } else {
            reject()
          }
        }).catch((e) => {
          reject(e)
        })
      })
    },
    // 获取系统数据量
    [types.GET_SYSTEM_CAPACITY] ({commit}, paras) {
      return new Promise((resolve, reject) => {
        api.system.getSystemCapacity(paras).then(res => {
          const { code, data } = res.body
          if (code === '000') {
            resolve(data)
          } else {
            reject(res.body)
          }
        }).catch(e => {
          handleError(e)
          reject(e)
        })
      })
    },
    // 获取项目数据量
    [types.GET_PROJECT_CAPACITY] ({commit}, paras) {
      return new Promise((resolve, reject) => {
        api.system.getProjectCapacity(paras).then(res => {
          const { code, data } = res.body
          if (code === '000') {
            resolve(data)
          } else {
            handleError(res)
            reject(res.body)
          }
        }).catch(e => {
          handleError(e)
          reject(e)
        })
      })
    },
    // 获取节点使用信息
    [types.GET_NODES_INFO] ({commit, dispatch}) {
      const data = {}
      nodeTimer && (data.isAuto = true, clearTimeout(nodeTimer))
      commit('SET_NODES_INFOS', {isLoading: true})
      return new Promise((resolve, reject) => {
        api.system.getNodesInfo(data).then(res => {
          const { code, data } = res.body
          if (code === '000') {
            const fail = ['TENTATIVE', 'ERROR'].includes(data.node_status)
            const unlimited = data.node === -1
            commit('SET_NODES_INFOS', {...data, fail, unlimited, isLoading: false})
            commit('LATEST_UPDATE_TIME')
            dispatch('globalAlertNotice')
          } else {
          }
          nodeTimer = setTimeout(() => {
            dispatch(types.GET_NODES_INFO)
          }, 1000 * 60)
        }).catch(() => {
          // commit('SET_NODES_INFOS', {isLoading: false})
          nodeTimer = setTimeout(() => {
            dispatch(types.GET_NODES_INFO)
          }, 1000 * 60)
        })
      })
    },
    // 获取系统数据量
    [types.GET_SYSTEM_CAPACITY_INFO] ({commit, dispatch}) {
      const data = {}
      capacityTimer && (data.isAuto = true)
      commit('SET_SYSTEM_CAPACITY_INFO', {isLoading: true})
      return new Promise((resolve, reject) => {
        api.system.getSystemCapacityInfo(data).then(res => {
          const { code, data } = res.body
          if (code === '000') {
            const fail = ['TENTATIVE', 'ERROR'].includes(data.capacity_status)
            const unlimited = data.capacity === -1
            commit('SET_SYSTEM_CAPACITY_INFO', {...data, fail, unlimited, isLoading: false})
            commit('LATEST_UPDATE_TIME')
            dispatch('globalAlertNotice')
          } else {
          }
          clearTimeout(capacityTimer)
          capacityTimer = setTimeout(() => {
            dispatch(types.GET_SYSTEM_CAPACITY_INFO)
          }, 1000 * 60)
        }).catch(() => {
          // commit('SET_SYSTEM_CAPACITY_INFO', {isLoading: false})
          clearTimeout(capacityTimer)
          capacityTimer = setTimeout(() => {
            dispatch(types.GET_SYSTEM_CAPACITY_INFO)
          }, 1000 * 60)
        })
      })
    },
    // 项目数据量详情 - table的数据量占比
    [types.GET_PROJECT_CAPACITY_DETAILS] ({commit}, params) {
      return new Promise((resolve, reject) => {
        api.system.getProjectCapacityDetails(params).then(res => {
          const { code, data } = res.body
          if (code === '000') {
            resolve(data)
          } else {
            handleError(res)
            reject(res)
          }
        })
      })
    },
    // 项目数据量 list
    [types.GET_PROJECT_CAPACITY_LIST] ({commit}, params) {
      return new Promise((resolve, reject) => {
        api.system.getProjectCapacityList(params).then(res => {
          const { code, data } = res.body
          if (code === '000') {
            resolve(data)
          } else {
            handleError(res)
            reject(res)
          }
        }).catch(e => {
          handleError(e)
          reject(e)
        })
      })
    },
    [types.REFRESH_SINGLE_PROJECT] (_, params) {
      return new Promise((resolve, reject) => {
        api.system.refreshProjectCapacity(params).then(res => {
          const { code, data } = res.body
          if (code === '000') {
            resolve(data)
          } else {
            handleError(res)
            reject(res)
          }
        }).catch(e => {
          handleError(e)
          reject(e)
        })
      })
    },
    // 刷新系统数据量-重新构建
    [types.REFRESH_ALL_SYSTEM] ({commit}) {
      commit('UPDATE_REFRESH_STATUS')
      return new Promise((resolve, reject) => {
        api.system.refreshAllSystem().then(res => {
          const { code, data } = res.body

          commit('UPDATE_REFRESH_STATUS')
          if (code === '000') {
            const fail = ['TENTATIVE', 'ERROR'].includes(data.capacity_status)
            commit(types.SET_SYSTEM_CAPACITY_INFO, {...data, fail, error: false})
            resolve(data)
          }
        }).catch(e => {
          commit('UPDATE_REFRESH_STATUS')
          handleError(e)
          reject(e)
        })
      })
    },
    // 获取预警通知状态
    [types.GET_EMAIL_NOTIFY_STATUS] () {
      return new Promise((resolve, reject) => {
        api.system.getNotifyStatus().then(res => {
          const { code, data } = res.body

          if (code === '000') {
            resolve(data)
          } else {
            handleError(res)
            reject(res)
          }
        }).catch(e => {
          handleError(e)
          reject(e)
        })
      })
    },
    // 保存预警通知email信息
    [types.SAVE_ALERT_EMAILS] (_, data) {
      return new Promise((resolve, reject) => {
        api.system.saveAlertEmails(data).then(res => {
          const { code, data } = res.body

          if (code === '000') {
            resolve(data)
          } else {
            handleError(res)
            reject(res)
          }
        }).catch(e => {
          handleError(e)
          reject()
        })
      })
    },
    globalAlertNotice ({state, getters, commit}) {
      // 0高危（当获取失败超 30 天 & 容量超额（已使用数据量超额 & 节点超额） & 无活跃 All节点时）> 1获取失败 > 2警告 > 3正常
      const capacity = `${filterElements.dataSize(state.systemCapacityInfo.current_capacity)}/${filterElements.dataSize(state.systemCapacityInfo.capacity)}`
      const nodes = `${state.systemNodeInfo.current_node}/${state.systemNodeInfo.node}`
      let alertType = () => {
        // if (state.systemCapacityInfo.error_over_thirty_days) {
        //   return { flag: 0, status: 'overThirtyDays', text: 'overThirtyDays', query: { capacity } }
        // }
        if (state.systemCapacityInfo.fail || state.systemNodeInfo.fail) {
          // let times = 30
          if (state.systemCapacityInfo.fail && state.systemNodeInfo.fail) {
            // times = 30 - Math.ceil((new Date().getTime() - state.systemCapacityInfo.first_error_time) / (1000 * 60 * 60 * 24))
            return { flag: 1, status: 'failApi', text: 'bothCapacityAndNodesFail' }
          } else if (state.systemCapacityInfo.fail) {
            // times = 30 - Math.ceil((new Date().getTime() - state.systemCapacityInfo.first_error_time) / (1000 * 60 * 60 * 24))
            return { flag: 1, status: 'failApi', text: 'capacityFailTip' }
          } else {
            return { flag: 1, status: 'failApi', text: 'nodesFailTip' }
          }
          // return { flag: 1, status: 'failApi', text: state.systemCapacityInfo.fail && state.systemNodeInfo.fail ? 'bothCapacityAndNodesFail' : state.systemCapacityInfo.fail ? 'capacityFailTip' : 'nodesFailTip' }
        } else if (state.systemCapacityInfo.capacity_status === 'OVERCAPACITY' || state.systemNodeInfo.node_status === 'OVERCAPACITY') {
          const _types = []
          state.systemCapacityInfo.capacity_status === 'OVERCAPACITY' && _types.push('systemCapacity')
          state.systemNodeInfo.node_status === 'OVERCAPACITY' && _types.push('nodes')
          return { flag: 0, status: 'overCapacity', target: _types, text: _types.includes('systemCapacity') && _types.includes('nodes') ? 'bothSystemAndNodeAlert' : _types.includes('systemCapacity') ? 'systemCapacityOverAlert' : 'nodeOverAlert', query: { capacity, nodes } }
        } else if (getters.isOnlyQueryNode) {
          return { flag: 0, status: 'noAllNodes', text: 'noJobNodes' }
        } else if (state.systemCapacityInfo.current_capacity / state.systemCapacityInfo.capacity > 80) {
          return { flag: 2, status: 'warning', text: 'capacityOverPrecent', query: { capacity } }
        } else {
          return {}
        }
      }
      const _flag = alertType()
      if (JSON.stringify(_flag) === '{}') {
        commit('CAPACITY_ALERT_TYPE', null)
      } else {
        if (JSON.stringify(state.capacityAlert) === JSON.stringify(_flag)) return
        commit('CAPACITY_ALERT_TYPE', _flag)
      }
    },
    [types.RESET_CAPACITY_DATA] (_) {
      nodeTimer && clearTimeout(nodeTimer)
      capacityTimer && clearTimeout(capacityTimer)
    }
  },
  getters: {
    isOnlyQueryNode (state) {
      return state.nodeList.length && state.nodeList.filter(it => it.mode === 'query').length === state.nodeList.length
    }
  }
}
