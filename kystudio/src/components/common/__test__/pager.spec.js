import { mount, createLocalVue } from '@vue/test-utils'
import pager from '../pager.vue'
import ElementUI from 'kyligence-ui'
import VueI18n from 'vue-i18n'
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
localVue.use(ElementUI)

const wrapper = mount(pager, {
  localVue,
  propsData: {
    totalSize: 50,
    totalSum: 10
  }
})

describe('Component Pager', () => {
  it('init', () => {
    expect(wrapper.isEmpty()).toBe(false)
    expect(wrapper.name()).toBe('pager')
  })
  it('set props1', (done) => {
    wrapper.setProps = {
      perPageSize: 10,
      totalSize: 50,
      curPage: 1,
      totalSum: 50,
      noBackground: true
    }
    wrapper.vm.$nextTick(() => {
      expect(wrapper.vm.$data.pageSize).toBe(10)
      expect(wrapper.vm.$data.currentPage).toBe(1)
      expect(wrapper.vm.$data.hasBackground).toBeTruthy()
      done()
    })
  })
  it('set props2', (done) => {
    wrapper.setProps = {
      totalSize: 50,
      totalSum: 50,
      noBackground: false
    }
    wrapper.vm.$nextTick(() => {
      expect(wrapper.vm.$data.pageSize).toBe(10)
      expect(wrapper.vm.$data.currentPage).toBe(1)
      expect(wrapper.vm.$data.hasBackground).toBeTruthy()
      done()
    })
  })
  it('get dom', () => {
    expect(wrapper.classes()).toContain('pager')
    expect(wrapper.find('span').html()).toEqual('<span class="total_size">Total Size : 10</span>')
    expect(wrapper.find('.el-pagination')).toBeTruthy()
  })
  it('emit', (done) => {
    wrapper.setProps = {
      perPageSize: 10,
      totalSize: 40,
      curPage: 1,
      totalSum: 50,
      noBackground: true
    }
    wrapper.vm.$nextTick(() => {
      const button = wrapper.findAll('button')

      expect(button.at(0).classes()).toContain('btn-prev')
      expect(button.at(1).classes()).toContain('btn-next')
      expect(button.length).toBe(2)
      wrapper.vm.$options.methods.currentChange.call(wrapper.vm)
      expect(wrapper.emittedByOrder().map(it => it.name)).toEqual(['handleCurrentChange'])
      done()
    })
  })
})
