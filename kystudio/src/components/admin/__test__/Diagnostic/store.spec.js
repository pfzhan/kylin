import Vue from 'vue'
import DiagnosticStore, {types} from '../../Diagnostic/store'
import VueResource from 'vue-resource'
// import { localVue } from '../../../../../test/common/spec_common'
// import * as api from '../../../../service/api'
import * as util from '../.././../../util/business'
import api from '../../../../service/api'
import { timer } from 'd3'

jest.setTimeout(50000)

Vue.use(VueResource)

jest.mock('vue-resource')

const mockHandleError = jest.spyOn(util, 'handleError').mockRejectedValue(false)
const mockDownloadFileByXMLHttp = jest.spyOn(util, 'downloadFileByXMLHttp').mockImplementation()


const root = {
  state: DiagnosticStore.state,
  commit: function (name, params) { DiagnosticStore.mutations[name](root.state, params) },
  dispatch: function (name, params) { return DiagnosticStore.actions[name]({state: root.state, commit: root.commit, dispatch: root.dispatch}, params) }
}

describe('Diagnostic Store', () => {
  it('actions GET_DUMP_REMOTE', async () => {
    const mockGetDumpRemote = jest.spyOn(api.system, 'getDumpRemote').mockImplementationOnce(() => {
      return new Promise((resolve, reject) => {
        resolve({code: '000', data: {data: 'front_2020_08_04_07_35_49_494298'}, msg: ''})
      })
    })
    await DiagnosticStore.actions[types.GET_DUMP_REMOTE](root, {host: 'http://sandbox.hortonworks.com:7070', end: 1596526469673, job_id: '', start: 1596440069673, tm: 1596440069673})
    expect(mockGetDumpRemote).toHaveBeenCalledWith({'end': 1596526469673, 'host': 'http://sandbox.hortonworks.com:7070', 'job_id': '', 'start': 1596440069673})
    expect('front_2020_08_04_07_35_49_494298' in DiagnosticStore.state.diagDumpIds).toBeTruthy()
  })
  it('actions GET_DUMP_REMOTE_2', async () => {
    const mockGetDumpRemote = jest.spyOn(api.system, 'getDumpRemote').mockImplementationOnce(() => {
      return new Promise((resolve, reject) => {
        resolve({code: '000', data: {data: 'front_2020_03_04_07_35_49_493622'}, msg: ''})
      })
    })
    await DiagnosticStore.actions[types.GET_DUMP_REMOTE](root, {host: 'http://sandbox.hortonworks.com:7080', end: 1596526479673, job_id: '', start: 1596440069673, tm: 1596440169673})
    expect(mockGetDumpRemote).toHaveBeenCalledWith({'end': 1596526469673, 'host': 'http://sandbox.hortonworks.com:7070', 'job_id': '', 'start': 1596440069673})
    expect('front_2020_03_04_07_35_49_493622' in DiagnosticStore.state.diagDumpIds).toBeTruthy()
  })
  it('actions GET_SERVERS', async () => {
    const mockLoadOnlineServers = jest.spyOn(api.datasource, 'loadOnlineQueryNodes').mockImplementation(() => {
      return new Promise((resolve, reject) => {
        resolve({code: '000', data: {data: [{host: 'sandbox.hortonworks.com:7070', mode: 'all'}]}})
      })
    })
    await DiagnosticStore.actions[types.GET_SERVERS](root, {$route: {name: 'Job'}})
    expect(mockLoadOnlineServers).toHaveBeenCalledWith({'ext': true})
    expect(DiagnosticStore.state.servers).toEqual([{'host': 'sandbox.hortonworks.com:7070', 'mode': 'all'}])
  })
  it('actions GET_STATUS_REMOTE', async () => {
    const mockGetStatusRemote = jest.spyOn(api.system, 'getStatusRemote').mockImplementationOnce(() => {
      return new Promise(resolve => resolve({code: '000', data: {data: {error: null, progress: 0.65, stage: 'DONE', status: '000', uuid: 'front_2020_08_04_07_35_49_494298'}}, msg: ''}))
    })
    await DiagnosticStore.actions[types.GET_STATUS_REMOTE](root, {host: 'http://sandbox.hortonworks.com:7070', id: 'front_2020_08_04_07_35_49_494298'})
    expect(mockGetStatusRemote).toHaveBeenCalledWith({host: 'http://sandbox.hortonworks.com:7070', id: 'front_2020_08_04_07_35_49_494298'})
    expect(DiagnosticStore.state.diagDumpIds['front_2020_08_04_07_35_49_494298'].progress).toBe(0.65)
  })
  it('actions DOWNLOAD_DUMP_DIAG', async () => {
    await DiagnosticStore.actions[types.DOWNLOAD_DUMP_DIAG](root, {host: 'http://sandbox.hortonworks.com:7070', id: 'front_2020_08_04_07_35_49_494298', isIframe: true})
    expect(mockDownloadFileByXMLHttp).toBeCalled()
    await DiagnosticStore.actions[types.DOWNLOAD_DUMP_DIAG](root, {host: 'http://sandbox.hortonworks.com:7070', id: 'front_2020_08_04_07_35_49_494298'})
  })
  it('actions REMOVE_DIAGNOSTIC_TASK', async () => {
    const mockStopDumpTask = jest.spyOn(api.system, 'stopDumpTask').mockImplementationOnce(() => {
      return new Promise(resolve => resolve({code: '000', data: {data: ''}, msg: ''}))
    })
    await DiagnosticStore.actions[types.REMOVE_DIAGNOSTIC_TASK](root)
    expect(mockStopDumpTask).toBeCalled()
  })
  it('actions POLLING_STATUS_MSG', async () => {
    const mockGetStatusRemote = jest.spyOn(api.system, 'getStatusRemote').mockImplementationOnce(() => {
      return new Promise(resolve => resolve({code: '000', data: {data: {error: null, progress: 0.8, stage: '', status: '001', uuid: 'front_2020_08_04_07_35_49_494298'}}, msg: ''}))
    })
    await DiagnosticStore.actions[types.POLLING_STATUS_MSG](root, {host: 'http://sandbox.hortonworks.com:7070', id: 'front_2020_08_04_07_35_49_494298'})
    expect(mockGetStatusRemote).toBeCalled()
    const mockGetStatusRemote1 = jest.spyOn(api.system, 'getStatusRemote').mockImplementationOnce(() => {
      return new Promise(resolve => resolve({code: '000', data: {data: {error: null, progress: 0.5, stage: 'DONE', status: '000', uuid: 'front_2020_08_04_07_35_49_494298'}}, msg: ''}))
    }) 
    await DiagnosticStore.actions[types.POLLING_STATUS_MSG](root, {host: 'http://sandbox.hortonworks.com:7070', id: 'front_2020_08_04_07_35_49_494298', isIframe: true})
    expect(mockGetStatusRemote).toBeCalled()
    expect(mockDownloadFileByXMLHttp).toBeCalled()
    const mockGetStatusRemoteError = jest.spyOn(api.system, 'getStatusRemote').mockRejectedValueOnce(false)
    await DiagnosticStore.actions[types.POLLING_STATUS_MSG](root, {host: 'http://sandbox.hortonworks.com:7070', id: 'front_2020_08_04_07_35_49_494298'})
    expect(mockGetStatusRemoteError).toBeCalled()
    expect(mockHandleError).toBeCalled()
  })
  // it('actions POLLING_STATUS_MSG 2', async (done) => {
  //   const mockGetStatusRemote1 = jest.spyOn(api.system, 'getStatusRemote').mockImplementationOnce(() => {
  //     return new Promise(resolve => resolve({code: '000', data: {data: {error: null, progress: 0.5, stage: '', status: '000', uuid: 'front_2020_08_04_07_35_49_494298'}}, msg: ''}))
  //   })
  //   await DiagnosticStore.actions[types.POLLING_STATUS_MSG](root, {host: 'http://sandbox.hortonworks.com:7070', id: 'front_2020_08_04_07_35_49_494298', isIframe: true})
  //   expect(mockGetStatusRemote1).toBeCalled()
  //   setTimeout(() => {
  //     // clearTimeout(timer['front_2020_08_04_07_35_49_494298'])
  //     expect(mockGetStatusRemote1).toBeCalledTimes(2)
  //     done()
  //   }, 1000)
  // })
  it('mutations STOP_INTERFACE_CALL', () => {
    DiagnosticStore.mutations[types.STOP_INTERFACE_CALL](root.state, false)
    expect(DiagnosticStore.state.isReset).toBeFalsy()
  })
  it('mutations UPDATE_CHECK_TYPE', () => {
    DiagnosticStore.mutations[types.UPDATE_CHECK_TYPE](root.state, true)
    expect('front_2020_08_04_07_35_49_494298' in DiagnosticStore.state.diagDumpIds).toBeTruthy()
    DiagnosticStore.mutations[types.UPDATE_CHECK_TYPE](root.state, false)
    expect('front_2020_08_04_07_35_49_494298' in DiagnosticStore.state.diagDumpIds).toBeTruthy()
  })
  it('mutations DEL_DUMP_ID_LIST', () => {
    DiagnosticStore.mutations[types.DEL_DUMP_ID_LIST](root.state, 'front_2020_4_6')
    expect(DiagnosticStore.state.diagDumpIds).not.toEqual({})
  })
  it('mutations RESET_DUMP_DATA', () => {
    DiagnosticStore.mutations[types.RESET_DUMP_DATA](root.state, true)
    expect(DiagnosticStore.state.diagDumpIds).toEqual({})
  })
})

describe('Diagnostic Store Error', () => {
  it('actions GET_DUMP_REMOTE error', async () => {
    const mockGetDumpRemoteError = jest.spyOn(api.system, 'getDumpRemote').mockImplementationOnce(() => {
      return new Promise((resolve, reject) => reject())
    })
    await DiagnosticStore.actions[types.GET_DUMP_REMOTE](root, {host: 'http://sandbox.hortonworks.com:7070', end: 1596526469673, job_id: '', start: 1596440069673}).catch(() => {})
    expect(mockGetDumpRemoteError).toBeCalled()
    expect(mockHandleError).toBeCalled()
  })
  it('actions GET_STATUS_REMOTE error', async () => {
    const mockGetStatusRemoteError = jest.spyOn(api.system, 'getStatusRemote').mockImplementationOnce(() => {
      return new Promise((resolve, reject) => reject())
    })
    await DiagnosticStore.actions[types.GET_STATUS_REMOTE](root, {host: 'http://sandbox.hortonworks.com:7070', id: 'front_2020_08_04_07_35_49_494298'}).catch(() => {})
    expect(mockGetStatusRemoteError).toBeCalled()
    expect(mockHandleError).toBeCalled()
  })
  it('actions GET_SERVERS error', async () => {
    const mockLoadOnlineServersError = jest.spyOn(api.datasource, 'loadOnlineQueryNodes').mockImplementationOnce(() => {
      return new Promise((resolve, reject) => reject())
    })
    await DiagnosticStore.actions[types.GET_SERVERS](root, {$route: {name: 'Job'}}).catch(() => {})
    expect(mockLoadOnlineServersError).toBeCalled()
    // expect(mockHandleError).toBeCalled()
  })
})
