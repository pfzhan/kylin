import api from '../../../service/api'
import { apiUrl } from '../../../config/index'
import { handleError } from '../../../util/business'

const pollingTime = 1000
let timer = {}

export const types = {
  GET_DUMP_REMOTE: 'GET_DUMP_REMOTE',
  GET_SERVERS: 'GET_SERVERS',
  GET_STATUS_REMOTE: 'GET_STATUS_REMOTE',
  UPDATE_DUMP_IDS: 'UPDATE_DUMP_IDS',
  UPDATE_SERVERS: 'UPDATE_SERVERS',
  SET_DUMP_PROGRESS: 'SET_DUMP_PROGRESS',
  UPDATE_CHECK_TYPE: 'UPDATE_CHECK_TYPE',
  RESET_DUMP_DATA: 'RESET_DUMP_DATA',
  DEL_DUMP_ID_LIST: 'DEL_DUMP_ID_LIST',
  POLLING_STATUS_MSG: 'POLLING_STATUS_MSG',
  DOWNLOAD_DUMP_DIAG: 'DOWNLOAD_DUMP_DIAG',
  STOP_INTERFACE_CALL: 'STOP_INTERFACE_CALL'
}

export default {
  state: {
    diagDumpIds: {},
    servers: [],
    isReset: false
  },
  mutations: {
    [types.UPDATE_DUMP_IDS] (state, { host, start, end, id }) {
      let idList = state.diagDumpIds
      if (typeof id === 'string') {
        idList = {
          ...idList,
          ...{[id]: {
            id,
            host,
            start,
            end,
            status: '000',
            stage: '',
            progress: 0,
            error: '',
            showErrorDetail: false,
            isCheck: false,
            running: true
          }}
        }
      }
      // 按时间顺序倒序排列
      let obj = {}
      Object.keys(idList).reverse().forEach(item => { obj[item] = idList[item] })
      state.diagDumpIds = {...obj}
    },
    [types.UPDATE_SERVERS] (state, data) {
      if (!Array.isArray(data)) console.error('[API Servers]: response data is not array')
      state.servers = data
    },
    // 更改当前诊断包生成的进度
    [types.SET_DUMP_PROGRESS] (state, data) {
      const { status, id } = data
      if (!(id in state.diagDumpIds)) return
      if (status === '000') {
        const { stage, progress } = data
        // state.diagDumpIds[id] = {...state.diagDumpIds[id], ...{status, stage, progress}}
        state.diagDumpIds = {...state.diagDumpIds, ...{[id]: {...state.diagDumpIds[id], ...{status, stage, progress, running: progress !== 1}}}}
      } else if (['001', '002', '999'].includes(status)) {
        const { error } = data
        state.diagDumpIds = {...state.diagDumpIds, ...{[id]: {...state.diagDumpIds[id], ...{status, error, running: false}}}}
      } else {
        state.diagDumpIds = {...state.diagDumpIds, ...{[id]: {...state.diagDumpIds[id], ...{status, running: false}}}}
      }
    },
    // 更改诊断包选中状态
    [types.UPDATE_CHECK_TYPE] (state, type) {
      let dumps = state.diagDumpIds
      if (type) {
        Object.keys(dumps).forEach(item => {
          dumps[item].status === '000' && !dumps[item].running && (dumps[item].isCheck = true)
        })
      } else {
        Object.keys(dumps).forEach(item => {
          dumps[item].status === '000' && (dumps[item].isCheck = false)
        })
      }
      state.diagDumpIds = {...state.diagDumpIds, ...dumps}
    },
    [types.DEL_DUMP_ID_LIST] (state, id) {
      let list = state.diagDumpIds
      delete list[id]
      state.diagDumpIds = {...list}
    },
    // 重置生成数据
    [types.RESET_DUMP_DATA] (state, type) {
      if (type) {
        state.diagDumpIds = {}
      }
      Object.keys(timer).forEach(item => {
        clearTimeout(timer[item])
      })
      timer = {}
    },
    [types.STOP_INTERFACE_CALL] (state, value) {
      state.isReset = value
    }
  },
  actions: {
    // 生成诊断包
    [types.GET_DUMP_REMOTE] ({ state, commit, dispatch }, { host = '', start = '', end = '', job_id = '' }) {
      if (!host) return
      return new Promise((resolve, reject) => {
        api.system.getDumpRemote({ host, start, end, job_id }).then(async (res) => {
          if (state.isReset) return
          const { data } = res.data
          await commit(types.UPDATE_DUMP_IDS, { host, start, end, id: data })
          dispatch(types.POLLING_STATUS_MSG, { host, id: data })
          resolve(data)
        }).catch((err) => {
          handleError(err)
          reject(err)
        })
      })
    },
    // 获取all或query节点信息
    [types.GET_SERVERS] ({ commit }, self) {
      return new Promise((resolve, reject) => {
        api.datasource.loadOnlineQueryNodes({ext: true}).then((res) => {
          let { data } = res.data
          if (self.$route.name === 'Job') {
            data = data.filter(item => item.mode === 'all')
          }
          commit(types.UPDATE_SERVERS, data)
          resolve(data)
        }).catch((err) => {
          handleError(err)
        })
      })
    },
    // 获取诊断报生成进度
    [types.GET_STATUS_REMOTE] ({ commit }, { host, id }) {
      return new Promise((resolve, reject) => {
        api.system.getStatusRemote({ host, id }).then(res => {
          const { data } = res.data
          commit(types.SET_DUMP_PROGRESS, {...data, id})
          resolve(res)
        }).catch((err) => {
          reject(err)
        })
      })
    },
    // 轮询接口获取信息
    [types.POLLING_STATUS_MSG] ({ state, commit, dispatch }, { host, id }) {
      if (state.isReset) return
      dispatch(types.GET_STATUS_REMOTE, { host, id }).then((res) => {
        timer[id] = setTimeout(() => {
          dispatch(types.POLLING_STATUS_MSG, { host, id })
        }, pollingTime)
        const { data } = res.data
        if (data.status === '000' && data.stage === 'DONE') {
          clearTimeout(timer[id])
          dispatch(types.DOWNLOAD_DUMP_DIAG, {host, id})
        } else if (['001', '002', '999'].includes(data.status)) {
          clearTimeout(timer[id])
        }
      }).catch((err) => {
        handleError(err)
        clearTimeout(timer[id])
        commit(types.SET_DUMP_PROGRESS, {status: 'error', id})
      })
    },
    // 下载诊断包
    [types.DOWNLOAD_DUMP_DIAG] (_, { host, id }) {
      let dom = document.createElement('a')
      dom.setAttribute('download', true)
      dom.setAttribute('href', `${location.origin}${apiUrl}/system/diag?host=${host}&id=${id}`)
      dom.click()
    }
  },
  namespaced: true
}
