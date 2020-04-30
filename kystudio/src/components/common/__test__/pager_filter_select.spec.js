import { shallow, createLocalVue, mount } from 'vue-test-utils'
import PagerFilterSelect from '../pager_filter_select.vue'
import ElementUI from 'kyligence-ui'
import VueI18n from 'vue-i18n'

const localVue = createLocalVue()
localVue.use(ElementUI)
localVue.use(VueI18n)

let propsData = {
  list: [{label: 'bigint', value: 'bigint'}, {label: 'int', value: 'int'}, {label: 'double', value: 'double'}],
  size: 10,
  placeholder: '',
  dataMap: null,
  value: '',
  disabled: false,
  asyn: true,
  delay: 2000,
  multiple: true
}
const factory = () => {
  return shallow(PagerFilterSelect, {
    localVue,
    propsData
  })
}

describe('pager filter select', () => {
  it('init', async (done) => {
    const wrapper = await factory()
    expect(wrapper.name()).toBe('filterSelect')
    expect(wrapper.find('div').classes()).toContain('el-select')
    wrapper.findAll('input').trigger('input')
    expect(wrapper.emitted().change).toBeTruthy()
    expect(wrapper.emitted().input).toBeTruthy()
    wrapper.findAll('input').trigger('blur')
    expect(wrapper.emitted().blur).toBeTruthy()
    wrapper.findAll('input').trigger('change')
    expect(wrapper.emitted().change).toBeTruthy()
    wrapper.vm.changeSelect()
    expect(wrapper.emitted().input).toBeTruthy()
    wrapper.vm.clickSelect()
    expect(wrapper.emitted().select).toBeTruthy()
    expect(wrapper.vm.$data.filterVal).toBe('')
    expect(wrapper.vm.asyn).toBeTruthy()
    wrapper.vm.remoteMethod('int')
    expect(wrapper.vm.filterVal).toBe('int')
    expect(wrapper.vm.list).toEqual([{'label': 'bigint', 'value': 'bigint'}, {'label': 'int', 'value': 'int'}, {'label': 'double', 'value': 'double'}])
    expect(wrapper.vm.filterList).toEqual([])
    setTimeout(() => {
      expect(wrapper.emitted().req).toBeTruthy()
      done()
    }, 2000)
    wrapper.setProps({ delay: null })
    wrapper.vm.remoteMethod('int')
    setTimeout(() => {
      expect(wrapper.emitted().req).toBeTruthy()
      done()
    }, 1000)
    wrapper.setProps({ asyn: false })
    await wrapper.update()
    wrapper.vm.remoteMethod('int')
    expect(wrapper.vm.filterList).toEqual([{'label': 'bigint', 'value': 'bigint'}, {'label': 'int', 'value': 'int'}])
    wrapper.vm.remoteMethod('')
    expect(wrapper.vm.filterList).toEqual([])
    wrapper.setProps({ dataMap: {label: 'name', value: 'value'}, list: [{name: 'bigint', value: 'bigint'}, {name: 'int', value: 'int'}, {name: 'double', value: 'double'}] })
    wrapper.setData({ datamap: {label: 'name', value: 'value'} })
    await wrapper.update()
    wrapper.vm.remoteMethod('double')
    expect(wrapper.vm.filterList).toEqual([{'name': 'double', 'value': 'double'}])
    let ot = {
      $refs: {}
    }
    PagerFilterSelect.mounted.call(ot)
    wrapper.findAll('input').trigger('input')
    expect(wrapper.emitted().change).toEqual([[''], ['']])
    wrapper.destroy()
  })
  it('test watch event', async () => {
    const wrapper = await factory()
    wrapper.vm.$options.watch.value.call(wrapper.vm, 'double')
    expect(wrapper.vm.filterVal).toBe('double')
    wrapper.destroy()
  })
  it('no dataMap data', async () => {
    propsData.dataMap = {label: 'name', value: 'value'}
    const wrapper = await factory()
    expect(wrapper.vm.datamap).toEqual({'label': 'name', 'value': 'value'})
    wrapper.destroy()
  })
})
