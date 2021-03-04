import { createLocalVue, shallowMount } from '@vue/test-utils'
import VueI18n from 'vue-i18n'
import noData from '../nodata.vue'
import enLocale from 'kyligence-ui/lib/locale/lang/en'
import zhLocale from 'kyligence-ui/lib/locale/lang/zh-CN'
import enKylinLocale from '../../../locale/en'
import zhKylinLocale from '../../../locale/zh-CN'

const localVue = createLocalVue()
enLocale.kylinLang = enKylinLocale.default
zhLocale.kylinLang = zhKylinLocale.default

localVue.use(VueI18n)
localVue.locale('en', enLocale)
localVue.locale('zh-cn', zhLocale)

const wrapper = shallowMount(noData, {
  localVue,
  propsData: {
    content: 'xxx'
  }
})

describe('Component noData', () => {
  it('default component config', () => {
    expect(wrapper.exists()).toBe(true)
    expect(wrapper.name()).toBe('NoData')
    expect(wrapper.isVueInstance()).toBeTruthy()
    expect(wrapper.html().replace(/\n/g, '')).toBe("<div class=\"no-data\">  xxx</div>")
  })
  it('test computed', async () => {
    await wrapper.setProps({ content: 'ccc' })
    expect(wrapper.vm.tips).toBe('ccc')
    await wrapper.setProps({ content: '123' })
    expect(wrapper.vm.tips).toBe('123')
    await wrapper.setProps({ content: '' })
    expect(wrapper.vm.tips).toBe('No data')
    await wrapper.vm.$nextTick()
    expect(wrapper.find('div').text()).toBe('No data')
  })
  it('set props', async () => {
    await wrapper.setProps({
      content: '暂无数据'
    })
    // await wrapper.update()
    expect(wrapper.find('div').text().trim()).toBe('暂无数据')
  })
})
