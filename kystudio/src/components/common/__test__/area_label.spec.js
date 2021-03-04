import { mount } from '@vue/test-utils'
import AreaLabel from '../area_label.vue'
import { localVue } from '../../../../test/common/spec_common'

const mockEvents = {
  remoteMethod: jest.fn()
}

const wrapper = mount(AreaLabel, {
  localVue,
  propsData: {
    labels: [{label: 'test', value: 'test'}],
    duplicateremove: true,
    validateRegex: /^\D/,
    splitChar: ',',
    selectedlabels: [],
    allowcreate: true,
    placeholder: '请输入或搜索',
    changeable: 'customClassName',
    datamap: {label: 'label', value: 'value'},
    isSignSameValue: true,
    ...mockEvents
  },
  mocks: {
  }
})

describe('Component AreaLabel', () => {
  it('init', async () => {
    expect(wrapper.vm.validateReg).toEqual(/^\D/)
    await wrapper.setProps({selectedlabels: ['test']})
    expect(wrapper.vm.selectedL).toEqual(["TEST"])
    await wrapper.setProps({validateRegex: /^\d(\w)+/})
    expect(wrapper.vm.validateReg).toEqual(/^\d(\w)+/)
  })
  it('computed', async () => {
    expect(wrapper.vm.baseLabel).toEqual([{"label": "test", "value": "test"}])
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.tags).toEqual([])
  })
  it('methods', async (done) => {
    wrapper.vm.remoteMethodSync('tt')
    expect(wrapper.vm.query).toBe('tt')
    expect(mockEvents.remoteMethod).toBeCalled()
    await wrapper.setProps({remoteSearch: false})
    wrapper.vm.remoteMethodSync('test1')
    expect(wrapper.vm.query).toBe('test1')

    wrapper.vm.change(['test'])
    await wrapper.vm.$nextTick()
    expect(wrapper.emitted().change).toEqual([[]])
    expect(wrapper.vm.selectedL).toEqual([])
    expect(wrapper.emitted().refreshData).toEqual([[[], false]])
    setTimeout(() => {
      expect(wrapper.emitted().duplicateTags).toEqual([[true]])
      done()
    }, 200)

    wrapper.vm.removeTag('test')
    setTimeout(() => {
      expect(wrapper.emitted().duplicateTags).toEqual([[false]])
      done()
    }, 200)
    expect(wrapper.emitted().removeTag).toEqual([['test', false]])

    wrapper.vm.selectTag({target: { className: 'el-tag el-select__tags', innerText: 'test'}})
    expect(wrapper.emitted().checklabel[0]).toEqual(["test", {"className": "el-tag el-select__tags", "innerText": "test"}])
    wrapper.vm.selectTag({target: { className: 'el-tag el-select__tags el-select__tags-text', innerText: 'test'}})
    expect(wrapper.emitted().checklabel[1]).toEqual(['test', {"className": "el-tag el-select__tags el-select__tags-text", "innerText": "test"}])

    expect(wrapper.vm.filterCreateTag()).toEqual([])
    expect(wrapper.vm.filterCreateTag('12Test')).toEqual(["12Test"])
    expect(wrapper.vm.filterCreateTag('12Test,1234')).toEqual(["12Test", '1234'])
    expect(wrapper.vm.filterCreateTag('tt1')).toEqual([])
    expect(wrapper.emitted().validateFail).toEqual([[], []])
    await wrapper.setProps({ignoreSplitChar: true})
    expect(wrapper.vm.filterCreateTag('12Test,1234')).toEqual(["12Test,1234"])

    await wrapper.setData({selectedL: ['1test', '1', '1']})
    wrapper.vm.clearDuplicateValue()
    expect(wrapper.vm.selectedL).toEqual(['1test', '1'])
    expect(wrapper.emitted().refreshData[1]).toEqual([["1test", '1'], false])

    wrapper.vm.$refs.select.$refs.input.onblur()
    setTimeout(() => {
      expect(wrapper.emitted().duplicateTags).toEqual([[false]])
      done()
    }, 200)

    await wrapper.setData({query: '1default'})
    wrapper.vm.$refs.select.$refs.input.onkeydown({keyCode: 13})
    expect(wrapper.vm.$refs.select.$refs.input.value).toBe('')
    expect(wrapper.emitted().refreshData[2]).toEqual([["1TEST", "1", "1DEFAULT"], false])

    await wrapper.setData({query: '1TEST'})
    wrapper.vm.$refs.select.$refs.input.onkeydown({keyCode: 13})
    expect(wrapper.emitted().refreshData[3]).toEqual([["1TEST", "1", "1DEFAULT"], false])

    wrapper.vm.$refs.select.$refs.input.value = '123T'
    wrapper.vm.$refs.select.$refs.input.onkeydown({keyCode: 10})
    expect(wrapper.vm.$refs.select.$refs.input.value).not.toBe('')

    await wrapper.setProps({selectedlabels: ["1TEST", "1", "1DEFAULT"]})
    await wrapper.vm.$nextTick()
    wrapper.vm.signSameTags()
    expect(wrapper.findAll('.el-tag').at(0).classes()).not.toContain('error-tag')

    await wrapper.setProps({duplicateremove: false, selectedlabels: ["1TEST", "1", "1DEFAULT", '1TEST']})
    await wrapper.vm.$nextTick()
    wrapper.vm.signSameTags()
    setTimeout(() => {
      expect(wrapper.emitted().duplicateTags).toEqual([[false]])
      done()
    }, 200)

    wrapper.vm.bindTagClick()
    wrapper.find('.el-select__tags span').trigger('click')
    
    wrapper.setProps({isNeedNotUpperCase: true, ignoreSplitChar: false})
    wrapper.vm.change(['12test,123'])
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.selectedL).toEqual(["1TEST", "1", "1DEFAULT", "12test", "123"])
    expect(wrapper.emitted().refreshData[8]).toEqual([["1TEST", "1", "1DEFAULT", "12test", "123"], false])
  })
})
