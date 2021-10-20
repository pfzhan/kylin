import { mount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import noAuthority from '../index'

jest.useFakeTimers()

describe('Component NoAuthority', () => {
  it('init', () => {
    const mockChangeRoute = jest.fn().mockImplementation((route) => {
      wrapper.vm.$route.name = route
    })
    const wrapper = mount(noAuthority, {
      localVue,
      mocks: {
        $route: {
          name: 'noAuthority'
        },
        $router: {
          push: mockChangeRoute
        }
      }
    })
    jest.runAllTimers()
    expect(wrapper.vm.$route).toEqual({name: '/dashboard'})
    wrapper.destroy()
  })
  it('init2', () => {
    const mockChangeRoute = jest.fn().mockImplementation((route) => {
      wrapper.vm.$route.name = route
    })
    const wrapper = mount(noAuthority, {
      localVue,
      mocks: {
        $route: {
          name: ''
        },
        $router: {
          push: mockChangeRoute
        }
      }
    })
    jest.runAllTimers()
    expect(wrapper.vm.$route).toEqual({name: ''})
  })
  it('before route enter', () => {
    const wrapper = mount(noAuthority, {
      localVue,
      mocks: {
        $route: {
          name: ''
        }
      }
    })
    const route = {
      to: {
        name: '',
        query: {resouce: 'isNoAuthority'}
      },
      from: {
        name: ''
      },
      next: jest.fn().mockImplementation(func => func && func(wrapper.vm))
    }
    noAuthority.options.beforeRouteEnter(route.to, route.from, route.next)
    expect(route.next).toBeCalled()
    expect(wrapper.vm.$data.tipType).toBe('isNoAuthority')

    delete route.to.query.resouce
    noAuthority.options.beforeRouteEnter(route.to, route.from, route.next)
    expect(wrapper.vm.$data.tipType).toBe('')
  })
})
