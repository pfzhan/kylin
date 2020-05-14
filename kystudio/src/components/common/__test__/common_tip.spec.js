import { shallow, createLocalVue } from 'vue-test-utils'
import VueI18n from 'vue-i18n'
import commonTip from '../common_tip.vue'

const localVue = createLocalVue()
localVue.use(VueI18n)
const wrapper = shallow(commonTip, {
  localVue,
  propsData: {
    tips: '无内容'
  }
})

describe('Component commonTip', () => {
  it('default component commonTip', () => {
    expect(wrapper.exists()).toBe(true)
    expect(wrapper.name()).toBe('common_tip')
    expect(wrapper.isVueInstance()).toBeTruthy()
    expect(wrapper.html().replace(/\n/g, '')).toBe('<span class="tip_box"><el-tooltip content="无内容" placement="top" open-delay="400"><div slot="content"></div> <!----> <span class="icon"></span></el-tooltip></span>')
  })
  it('set tips', async () => {
    wrapper.setProps({
      tips: '暂无数据'
    })
    await wrapper.update()
    expect(wrapper.find('el-tooltip').attributes().content).toBe('暂无数据')
  })
  it('set placement', async () => {
    wrapper.setProps({
      placement: 'left'
    })
    await wrapper.update()
    expect(wrapper.find('el-tooltip').attributes().placement).toBe('left')
  })
  it('set content', async () => {
    wrapper.setProps({
      content: '<p>hehehe</p>'
    })
    await wrapper.update()
    expect(wrapper.find('el-tooltip').find('div').html()).toBe('<div slot="content"></div>')
  })
  it('set disabled', async () => {
    wrapper.setProps({
      disabled: false
    })
    await wrapper.update()
    expect(wrapper.vm.visible).toBe(false)
  })
})
