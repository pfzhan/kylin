import Vuex from 'vuex'
import { mount, shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import SystemCapacity from '../../SystemCapacity/index.vue'
import charts from '../../../../util/charts'
import KyligenceUI from 'kyligence-ui'
import EmptyData from '../../../common/EmptyData/EmptyData.vue'
import kapPager from '../../../common/kap_pager.vue'
import commonTip from '../../../common/common_tip.vue'
import echarts from 'echarts'

localVue.use(KyligenceUI)

const mockApis = {
  mockGetSystemCapacity: jest.fn().mockImplementation(() => {
    return new Promise(resolve => resolve({1594166391126: 3244484199, 1594252763624: 3237509302}))
  }),
  mockGetPorjectCapacityDetails: jest.fn().mockImplementation(() => {
    return Promise.resolve({tables: [{name: 'tb_text', usage: 0.5}]})
  }),
  mockGetProjectCapacityList: jest.fn().mockImplementation(() => {
    return Promise.resolve({
      capacity_detail: [
        {name: 'test_ssb_tpch', capacity: 3042305565, capacity_ratio: 0.91, status: 'OK'},
        {capacity: 12127598, capacity_ratio: 0, name: 'han_ex', status: 'TENTATIVE'}
      ]
    })
  }),
  mockRefreshSingleProject: jest.fn().mockResolvedValue(true),
  mockRefreshAllSystem: jest.fn().mockImplementation(),
  mockGetNodesInfo: jest.fn().mockImplementation()
}

const mockEventListener = jest.spyOn(window, 'addEventListener').mockImplementation()
const mockRemoveEventListener = jest.spyOn(window, 'removeEventListener').mockImplementation()
const _h = jest.fn().mockImplementation(res => res)

const mockOnEvent = jest.fn().mockImplementation((name, callback) => {
  callback({data: {name: 'test'}})
})

const mockEcharts = jest.spyOn(echarts, 'init').mockImplementation(() => {
  return {
    hideLoading: jest.fn(),
    setOption: jest.fn(),
    showLoading: jest.fn(),
    on: mockOnEvent
  }
})

const store = new Vuex.Store({
  state: {
    capacity: {
      nodeList: [{host: 'sandbox.hortonworks.com:7070', mode: 'all'}],
      systemNodeInfo: {
        current_node: 1,
        node: 0,
        error: false,
        isLoading: false,
        fail: false,
        unlimited: false
      },
      systemCapacityInfo: {
        current_capacity: 3333375671,
        capacity: 10995116277760,
        error: false,
        isLoading: false,
        error_over_thirty_days: false,
        fail: false,
        unlimited: false
      }
    }
  },
  actions: {
    'GET_SYSTEM_CAPACITY': mockApis.mockGetSystemCapacity,
    'GET_PROJECT_CAPACITY_DETAILS': mockApis.mockGetPorjectCapacityDetails,
    'GET_PROJECT_CAPACITY_LIST': mockApis.mockGetProjectCapacityList,
    'REFRESH_SINGLE_PROJECT': mockApis.mockRefreshSingleProject,
    'REFRESH_ALL_SYSTEM': mockApis.mockRefreshAllSystem,
    'GET_NODES_INFO': mockApis.mockGetNodesInfo
  }
})

const EmptyDataComponent = shallowMount(EmptyData, { localVue, store })

const wrapper = mount(SystemCapacity, {
  store,
  localVue,
  mocks: {
    echarts: mockEcharts
  },
  components: {
    EmptyData,
    kapPager,
    commonTip
  }
})

describe('Components SystemCapacity', () => {
  it('init', () => {
    expect(wrapper.vm.$data.nodes).toEqual([{host: 'sandbox.hortonworks.com:7070', mode: 'all'}])
    expect(mockApis.mockGetProjectCapacityList).toBeCalled()
    expect(mockApis.mockGetSystemCapacity).toBeCalled()
    expect(mockEventListener.mock.calls[0][0]).toBe('resize')
  })
  it('computed', async () => {
    expect(wrapper.vm.getCapacityPrecent).toBe('0.03')
    wrapper.vm.$store.state.capacity.systemCapacityInfo.unlimited = true
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.getCapacityPrecent).toBe(0)

    expect(wrapper.vm.dataOptions).toEqual([{'text': 'Last 30 Days', 'value': 'month'}, {'text': 'Last 90 Days', 'value': 'quarter'}])

    expect(wrapper.vm.isChangeValue).toBeFalsy()

    expect(wrapper.vm.getValueColor).toBe('')
    wrapper.vm.$store.state.capacity.systemCapacityInfo.current_capacity = 10995106277760
    wrapper.vm.$store.state.capacity.systemCapacityInfo.unlimited = false
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.getValueColor).toBe('is-warn')
    wrapper.vm.$store.state.capacity.systemCapacityInfo.current_capacity = 11995126277760
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.getValueColor).toBe('is-error')
  })
  it('methods', async () => {
    wrapper.vm.changeDataLine()
    expect(mockApis.mockGetSystemCapacity).toBeCalled()

    wrapper.vm.projectSortChange({prop: 'capacity', order: 'descending'})
    expect(mockApis.mockGetProjectCapacityList).toBeCalled()

    wrapper.vm.pageCurrentChange(2)
    expect(wrapper.vm.$data.projectCapacity.currentPage).toBe(2)
    expect(mockApis.mockGetProjectCapacityList).toBeCalled()

    wrapper.vm.projectDetailsSortChange({prop: 'capacity', order: 'descending'})
    expect(wrapper.vm.$data.projectDetail).toEqual({'currentPage': 0, 'list': [], 'pageSize': 10, 'reverse': true, 'sort_by': 'capacity', 'totalSize': 10})
    expect(mockApis.mockGetPorjectCapacityDetails).toBeCalled()

    wrapper.vm.pageCurrentChangeByDetail(1)
    expect(wrapper.vm.$data.projectDetail.currentPage).toBe(1)
    expect(mockApis.mockGetPorjectCapacityDetails).toBeCalled()

    await wrapper.setData({alertMsg: {...wrapper.vm.$data.alertMsg, emailTags: ['482309233@163.com']}})
    wrapper.vm.delEmailEvent({key: 'Backspace'})
    expect(wrapper.vm.$data.alertMsg.emailTags).toEqual([])

    await wrapper.setData({projectCapacity: {...wrapper.vm.$data.projectCapacity, status: ['fail', 'success']}})
    expect(mockApis.mockGetProjectCapacityList).toBeCalled()

    wrapper.vm.handleEventFocus()
    expect(mockEventListener.mock.calls[1][0]).toBe('keydown')

    const options = {
      lineCharts: {
        resize: jest.fn()
      },
      treeMapCharts: {
        resize: jest.fn()
      }
    }
    SystemCapacity.options.methods.resetChartsPosition.call(options)
    expect(options.lineCharts.resize).toBeCalled()
    expect(options.treeMapCharts.resize).toBeCalled()

    wrapper.vm.changeResult()
    expect(mockApis.mockGetPorjectCapacityDetails).toBeCalled()

    expect(wrapper.vm.renderUsageHeader(_h, { column: '', index: 1 })).toBe('span')
    expect(wrapper.vm.renderDetailUsageHeader(_h, { column: '', index: 1 })).toBe('span')

    wrapper.vm.showDetail({name: 'test'})
    expect(wrapper.vm.$data.currentProjectName).toBe('test')
    expect(wrapper.vm.$data.showProjectChargedDetails).toBeTruthy()
    expect(mockApis.mockGetPorjectCapacityDetails).toBeCalled()

    wrapper.vm.refreshProjectCapacity({refresh: false})
    expect(mockApis.mockRefreshSingleProject).toBeCalled()

    wrapper.vm.refreshSystemBuildJob()
    expect(mockApis.mockRefreshAllSystem).toBeCalled()

    wrapper.vm.refreshNodes()
    expect(mockApis.mockGetNodesInfo).toBeCalled()

    wrapper.vm.filterContent(['OK'], 'project_status')
    expect(wrapper.vm.$data.projectCapacity.status).toEqual(['OK'])
    expect(mockApis.mockGetProjectCapacityList).toBeCalled()

    wrapper.vm.filterContent(['TANTITIVE'], 'project_detail_status')
    expect(wrapper.vm.$data.projectDetail.status).toEqual(['TANTITIVE'])
    expect(mockApis.mockGetPorjectCapacityDetails).toBeCalled()

    const options1 = {
      getSystemCapacity: jest.fn().mockImplementation(() => {
        return Promise.resolve({})
      }),
      selectedDataLine: 'month',
      lineOptions: ''
    }
    await SystemCapacity.options.methods.getCapacityBySystem.call(options1)
    expect(options1.getSystemCapacity).toBeCalled()
    expect(options1.lineOptions).toBeNull()

    const lineCharts = charts.line(wrapper.vm, [1594166391126, 1594252763624], [3244484199, 234232432], wrapper.vm.$data.lineCharts)
    expect(lineCharts.color).toEqual(['#15BDF1'])
    expect(lineCharts.xAxis.data).toEqual([1594166391126, 1594252763624])
    expect(lineCharts.series[0].data).toEqual([3244484199, 234232432])
    expect(lineCharts.tooltip.formatter({dataIndex: 1})).not.toBe('')

    const lineCharts1 = charts.line(wrapper.vm)
    expect(lineCharts1.xAxis.data).toEqual([])
    expect(lineCharts1.series[0].data).toEqual([])

    const mockEncodeHtml = jest.fn().mockImplementation((res) => {
      return res
    })

    const treeMapCharts = charts.treeMap(wrapper.vm, [{name: 'test', value: 30}, {name: 'test1', value: 50}], {encodeHTML: mockEncodeHtml})
    expect(treeMapCharts.series[0].data).toEqual([{name: 'test', value: 30}, {name: 'test1', value: 50}])
    // expect(treeMapCharts.tooltip.formatter({data: {capacity: 121343243}, treePathInfo: [{name: 'test'}, {name: 'test1'}]})).toEqual("<div class=\"tooltip-title\">Project Name：test1</div>Data Volume Used: 121343243")

    wrapper.destroy()
    expect(mockRemoveEventListener).toBeCalled()
  })
})
