import { shallowMount } from '@vue/test-utils'
import Vuex from 'vuex'
import { localVue } from '../../../../test/common/spec_common'
import saveQueryDialog from '../save_query_dialog.vue'
import kapEditor from '../../common/kap_editor.vue'
import * as bussiness from '../../../util/business'

const mockHandleError = jest.spyOn(bussiness, 'handleError').mockImplementation((res) => {
  return Promise.resolve(res)
})

const mockApi = {
  mockSaveQuery: jest.fn().mockImplementation(() => {
    return new Promise((resolve) => {
      resolve(true)
    })
  })
}
const mockMessage = jest.fn().mockImplementation()

const store = new Vuex.Store({
  actions: {
    SAVE_QUERY: mockApi.mockSaveQuery
  }
})

const wrapper = shallowMount(saveQueryDialog, {
  localVue,
  store,
  propsData: {
    show: false,
    project: 'Kyligence',
    sql: 'select * from SSB;'
  },
  mocks: {
    handleError: mockHandleError,
    $message: mockMessage
  },
  components: {
    'kap-editor': kapEditor
  }
})
wrapper.vm.$refs = {
  saveQueryForm: {
    validate: jest.fn().mockImplementation(callback => {
      callback && callback(true)
    })
  }
}

describe('Component SaveQueryDialog', () => {
  it('init', async () => {
    await wrapper.setProps({ show: true })
    // await wrapper.update()
    expect(wrapper.vm.saveQueryFormVisible).toBeTruthy()
    expect(wrapper.vm.saveQueryMeta.project).toBe('Kyligence')
    expect(wrapper.vm.saveQueryMeta.sql).toBe('select * from SSB;')
  })
  it('methods', async () => {
    await wrapper.setData({saveQueryMeta: { ...wrapper.vm.saveQueryMeta, name: 'test' }})
    // await wrapper.update()
    await wrapper.vm.saveQuery()
    expect(mockApi.mockSaveQuery.mock.calls[0][1]).toEqual({'description': '', 'name': '', 'project': 'Kyligence', 'sql': 'select * from SSB;'})
    expect(wrapper.vm.$refs.saveQueryForm.validate).toBeCalled()
    expect(mockMessage).toBeCalledWith({'message': 'Saved successfully.', 'type': 'success'})
    expect(wrapper.vm.saveQueryMeta).toEqual({'description': '', 'name': '', 'project': 'Kyligence', 'sql': 'select * from SSB;'})
    expect(wrapper.vm.saveQueryFormVisible).toBeFalsy()
    expect(wrapper.emitted().closeModal).toEqual([[true]])

    wrapper.vm.$store._actions.SAVE_QUERY = [jest.fn().mockImplementation(() => {
      return {
        then: (callback, errorCallback) => {
          errorCallback && errorCallback()
        }
      }
    })]

    await wrapper.vm.$nextTick()
    await wrapper.vm.saveQuery()
    expect(wrapper.vm.isSubmit).toBeFalsy()
    expect(mockHandleError).toBeCalled()
    expect(wrapper.vm.saveQueryFormVisible).toBeFalsy()
    expect(wrapper.emitted().closeModal).toEqual([[true], [undefined]])

    const _callback = jest.fn()
    wrapper.vm.checkName(null, '', _callback)
    expect(_callback.mock.calls[0][0].toString()).toEqual('Error: Please enter here')
    wrapper.vm.checkName(null, '*123', _callback)
    expect(_callback.mock.calls[1][0].toString()).toEqual('Error: Only supports number, letter and underline')
    wrapper.vm.checkName(null, 'test', _callback)
    expect(_callback).toBeCalledWith()
  })
})
