import { mount } from 'vue-test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import GlobalDialog from '../../GlobalDialog/dialog/detail_dialog.vue'
import DetailDialogModal from '../../GlobalDialog/dialog/store'
import Vuex from 'vuex'
import KyligenceUI from 'kyligence-ui'

jest.useFakeTimers()

const store = new Vuex.Store({
  state: {
    system: {
      lang: 'en'
    }
  },
  modules: {
    DetailDialogModal: DetailDialogModal
  }
})
const mockMessage = jest.fn()
const customCallback = jest.fn().mockImplementation()
const mockCallback = jest.fn().mockImplementation()
const mockCancelReject = jest.fn().mockImplementation()

localVue.use(KyligenceUI)

const wrapper = mount(GlobalDialog, {
  localVue,
  store,
  mocks: {
    $message: mockMessage
  }
})

describe('Component GlobalDialog', () => {
  it('init', async () => {
    wrapper.vm.$store.state.DetailDialogModal = {
      ...wrapper.vm.$store.state.DetailDialogModal,
      isShow: true,
      isShowSelection: true,
      detailTableData: [{name: 'name', text: '', type: 'd'}]
    }
    await wrapper.update()
    expect(wrapper.find('.global-dialog-box').exists()).toBeTruthy()
    expect(wrapper.vm.$store.state.DetailDialogModal.detailTableData).toEqual([{'name': 'name', 'text': '', 'type': 'd'}])
    expect(wrapper.find('.el-dialog').attributes().style).toContain('width: 480px;')

    wrapper.vm.$store.state.DetailDialogModal.dangerouslyUseHTMLString = true
    wrapper.vm.$store.state.DetailDialogModal.msg = 'alert提醒文案'
    await wrapper.update()
    expect(wrapper.find('.confirm-msg').text()).toBe('alert提醒文案')

    wrapper.vm.$store.state.DetailDialogModal.dangerouslyUseHTMLString = false
    wrapper.vm.$store.state.DetailDialogModal.msg = 'alert提醒文案\n文案'
    await wrapper.update()
    expect(wrapper.find('.confirm-msg').text()).toBe('alert提醒文案\n文案')

    wrapper.vm.$store.state.DetailDialogModal.showDetailBtn = true
    await wrapper.update()
    wrapper.find('.show-detail').trigger('click')
    expect(wrapper.vm.$data.showDetail).toBeTruthy()

    wrapper.vm.$store.state.DetailDialogModal.detailMsg = '详细内容'
    wrapper.setData({ showDetail: true })
    await wrapper.update()
    expect(wrapper.vm.$data.showDetail).toBeTruthy()
    // expect(wrapper.find('.detailMsg').html()).toEqual('详细内容')

    wrapper.vm.$store.state.DetailDialogModal.theme = 'sql'
    wrapper.vm.$store.state.DetailDialogModal.showDetailDirect = true
    wrapper.setData({ showDetail: true })
    await wrapper.update()
    expect(wrapper.find('.list-editor').exists()).toBeFalsy()

    wrapper.vm.$store.state.DetailDialogModal.theme = 'plain'
    wrapper.vm.$store.state.DetailDialogModal.showCopyBtn = true
    wrapper.vm.$nextTick(() => {
      expect(wrapper.find('.copyBtn').exists()).toBeTruthy()
    })
  })
  it('computed events', async () => {
    expect(wrapper.vm.closeT).toBe('Close')
    wrapper.vm.$store.state.DetailDialogModal.closeText = '关闭'
    await wrapper.update()
    expect(wrapper.vm.closeT).toBe('关闭')

    expect(wrapper.vm.cancelT).toBe('Cancel')
    wrapper.vm.$store.state.DetailDialogModal.cancelText = '取消'
    await wrapper.update()
    expect(wrapper.vm.cancelT).toBe('取消')

    expect(wrapper.vm.submitText).toBe('')
    wrapper.vm.$store.state.DetailDialogModal.submitText = '提交'
    await wrapper.update()
    expect(wrapper.vm.submitText).toBe('提交')
  })
  it('methods events', async (done) => {
    wrapper.vm.$store.state.DetailDialogModal.dialogType = 'error'

    wrapper.vm.hideCopyText()
    jest.runAllTimers()
    expect(wrapper.vm.$data.hideCopyText).toBeFalsy()

    expect(wrapper.vm.tableRowClassName({row: {highlight: true}})).toBe('highlight-row')
    expect(wrapper.vm.tableRowClassName({row: {highlight: false}})).toBe('')

    wrapper.vm.handleSelectionChange([{name: 'test', type: 'd'}])
    expect(wrapper.vm.$data.multipleSelection).toEqual([{name: 'test', type: 'd'}])

    wrapper.vm.$store.state.DetailDialogModal.showCopyTextLeftBtn = true
    await wrapper.update()
    wrapper.vm.onError()
    expect(wrapper.vm.$data.showCopyText).toBeTruthy()
    expect(wrapper.vm.$data.copySuccess).toBeFalsy()

    wrapper.vm.$store.state.DetailDialogModal.showCopyTextLeftBtn = false
    await wrapper.update()
    wrapper.vm.onError()
    expect(mockMessage).toHaveBeenCalledWith({'message': 'Failed to copy! Your browser does not support paste boards!', 'type': 'error'})

    wrapper.vm.$store.state.DetailDialogModal.showCopyTextLeftBtn = true
    await wrapper.update()
    wrapper.vm.onCopy()
    expect(wrapper.vm.$data.showCopyText).toBeTruthy()
    expect(wrapper.vm.$data.copySuccess).toBeTruthy()

    wrapper.vm.$store.state.DetailDialogModal.showCopyTextLeftBtn = false
    await wrapper.update()
    wrapper.vm.onCopy()
    expect(mockMessage).toHaveBeenCalledWith({'message': 'Failed to copy! Your browser does not support paste boards!', 'type': 'error'})

    wrapper.vm.$store.state.DetailDialogModal.callback = mockCallback
    await wrapper.update()
    wrapper.vm.handleSubmit()
    jest.runAllTimers()
    expect(wrapper.vm.$data.loading).toBeFalsy()
    wrapper.vm.$nextTick(() => {
      expect(mockCallback).toBeCalledWith({'isOnlySave': true})
      expect(wrapper.vm.$data.loading).toBeFalsy()
      expect(wrapper.vm.$data.showDetail).toBeFalsy()
      expect(wrapper.vm.$data.multipleSelection).toEqual([])
      done()
    })

    wrapper.vm.$store.state.DetailDialogModal = {
      ...wrapper.vm.$store.state.DetailDialogModal,
      isShowSelection: true,
      customCallback: customCallback
    }
    await wrapper.update()
    wrapper.vm.handleSubmit()
    jest.runAllTimers()
    expect(customCallback).toBeCalled()

    wrapper.vm.$store.state.DetailDialogModal.isShowSelection = false
    await wrapper.update()
    wrapper.vm.handleSubmit()
    jest.runAllTimers()
    expect(mockCallback).toBeCalled()

    wrapper.vm.handleCloseAndResove()
    expect(mockCallback).toBeCalled()

    wrapper.vm.$store.state.DetailDialogModal.needCallbackWhenClose = true
    await wrapper.update()
    wrapper.vm.handleClose()
    expect(mockCallback).toBeCalled()

    wrapper.vm.$store.state.DetailDialogModal = {
      ...wrapper.vm.$store.state.DetailDialogModal,
      needConcelReject: true,
      cancelReject: mockCancelReject,
      onlyCloseDialogReject: true
    }
    await wrapper.update()
    wrapper.vm.handleClose('close')
    expect(mockCancelReject).toBeCalled()

    jest.clearAllTimers()
  })
})
