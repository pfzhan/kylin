import Vuex from 'vuex'
import { shallowMount, mount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import CapacityTopBar from '../../SystemCapacity/CapacityTopBar.vue'
import * as util from '../../../../util/business'
import KyligenceUI from 'kyligence-ui'

const mockHandleError = jest.spyOn(util, 'handleError').mockRejectedValue(false)
const mockApis = {
  mockGetNodesList: jest.fn().mockImplementation(() => {
    return Promise.resolve({servers: [{host: 'sandbox.com:7070', mode: 'all'}]})
  }),
  mockGetNodesInfo: jest.fn().mockImplementation(),
  mockGetSystemCapacityInfo: jest.fn().mockImplementation(),
  mockRefreshAllSystem: jest.fn().mockImplementation(),
  mockResetCapacityData: jest.fn().mockImplementation()
}

const store = new Vuex.Store({
  state: {
    capacity: {
      systemNodeInfo: {
        current_node: 1,
        node: 2,
        error: false,
        isLoading: false,
        fail: false,
        unlimited: false,
        node_status: 'OK'
      },
      systemCapacityInfo: {
        current_capacity: 3333375671,
        capacity: 10995116277760,
        error: false,
        isLoading: false,
        error_over_thirty_days: false,
        fail: false,
        unlimited: false
      },
      latestUpdateTime: ''
    }
  },
  actions: {
    'GET_NODES_LIST': mockApis.mockGetNodesList,
    'GET_NODES_INFO': mockApis.mockGetNodesInfo,
    'GET_SYSTEM_CAPACITY_INFO': mockApis.mockGetSystemCapacityInfo,
    'REFRESH_ALL_SYSTEM': mockApis.mockRefreshAllSystem,
    'RESET_CAPACITY_DATA': mockApis.mockResetCapacityData
  },
  getters: {
    isOnlyQueryNode () {
      return false
    },
    isAdminRole () {
      return true
    }
  }
})

localVue.use(KyligenceUI)

const wrapper = mount(CapacityTopBar, {
  store,
  localVue
})

describe('Components CapacityTopBar', () => {
  it('init', () => {
    expect(mockApis.mockGetNodesList).toBeCalled()
    expect(wrapper.vm.$data.isNodeLoadingSuccess).toBeTruthy()
    expect(wrapper.vm.$data.nodeList).toEqual([{host: 'sandbox.com:7070', mode: 'All'}])
    expect(wrapper.vm.$data.isNodeLoading).toBeFalsy()
    expect(mockApis.mockGetSystemCapacityInfo).toBeCalled()
    expect(mockApis.mockGetNodesInfo).toBeCalled()
  })
  it('computed', async () => {
    expect(wrapper.vm.getDataFails).toBeTruthy()
    wrapper.vm.$store.state.capacity.systemNodeInfo.fail = true
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.getDataFails).toBeFalsy()
    expect(wrapper.vm.getNodesNumColor).toBe('is-success')
    wrapper.vm.$store.state.capacity.systemNodeInfo.node_status = 'OVERCAPACITY'
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.getNodesNumColor).toBe('is-danger')
    wrapper.vm.$store.state.capacity.systemNodeInfo.node_status = 'OK'
    wrapper.vm.$store.state.capacity.systemCapacityInfo.current_capacity = '10996116277760'
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.getNodesNumColor).toBe('is-warning')

    expect(wrapper.vm.getCapacityPrecent).toBe('100.01')
    wrapper.vm.$store.state.capacity.systemCapacityInfo.unlimited = true
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.getCapacityPrecent).toBe(0)

    expect(wrapper.vm.showLoadingStatus).toBeFalsy()
    
  })
  it('methods', async () => {
    wrapper.vm.$store.state.capacity.systemCapacityInfo.fail = true
    wrapper.vm.$store.state.capacity.systemNodeInfo.fail = true
    await wrapper.vm.$nextTick()
    wrapper.vm.refreshCapacityOrNodes()
    expect(mockApis.mockRefreshAllSystem).toBeCalled()
    expect(mockApis.mockGetNodesInfo).toBeCalled()
    expect(wrapper.findAll('.el-icon-ksd-restart').exists()).toBeTruthy()

    wrapper.vm._isDestroyed = true
    await wrapper.vm.$nextTick()
    wrapper.vm.getHANodes()
    expect(wrapper.vm.$data.isNodeLoadingSuccess).toBeTruthy()
    expect(wrapper.vm.$data.nodeList.length).toBe(1)

    const catchEvent1 = (callback) => {
      callback({status: ''})
    }
    const mockGetNodesListError1 = jest.fn().mockImplementation(() => {
      return {
        then: () => {
          return {
            catch: catchEvent1
          }
        },
        catch: catchEvent1
      }
    })
    const options1 = {
      _isDestroyed: false,
      isNodeLoading: false,
      getNodeList: mockGetNodesListError1,
      nodesTimer: null
    }
    CapacityTopBar.options.methods.getHANodes.call(options1)
    expect(mockHandleError).not.toBeCalled()

    const catchEvent = (callback) => {
      callback({status: 401})
    }
    let mockGetNodesListError = jest.fn().mockImplementation(() => {
      return {
        then: () => {
          return {
            catch: catchEvent
          }
        },
        catch: catchEvent
      }
    })
    const options = {
      _isDestroyed: false,
      isNodeLoading: false,
      getNodeList: mockGetNodesListError,
      nodesTimer: null
    }
    // await wrapper.update()
    CapacityTopBar.options.methods.getHANodes.call(options)
    expect(mockHandleError).toBeCalled()

    wrapper.destroy()
    expect(mockApis.mockResetCapacityData).toBeCalled()
  })
})
