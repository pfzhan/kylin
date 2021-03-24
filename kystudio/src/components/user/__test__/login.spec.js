import { shallowMount, createLocalVue } from '@vue/test-utils'
import Vuex from 'vuex'
import Login from '../login.vue'
import VueResource from 'vue-resource'
import VueRouter from 'vue-router'
import Vue from 'vue'
import ElementUI from 'kyligence-ui'
import * as util from '../../../util/business'
import License from '../license.vue'
import iconBtn from 'components/common/icon_button'

jest.setTimeout(30000)

const types = {
  LOGIN: 'LOGIN',
  GET_ABOUTKAP: 'GET_ABOUTKAP',
  TRIAL_LICENSE_FILE: 'TRIAL_LICENSE_FILE',
  SAVE_CURRENT_LOGIN_USER: 'SAVE_CURRENT_LOGIN_USER',
  TOGGLE_LICENSE_DIALOG: 'TOGGLE_LICENSE_DIALOG',
  IS_CLOUD: 'IS_CLOUD'
}
const router = new VueRouter()
const localVue = createLocalVue()
localVue.use(Vuex)
localVue.use(ElementUI)
localVue.use(VueRouter)
Vue.use(VueResource)

const data = {
  code: '000',
  data: {
    authorities: [{ authority: 'ALL_USERS' }],
    create_time: 1586771094749,
    disabled: false,
    first_login_failed_time: 1587979334328,
    last_modified: 1587979334506,
    locked: false,
    locked_time: 0,
    mvcc: 262,
    username: 'ADMIN',
    uuid: 'd9392429-dd8a-4508-8998-58f7e78c96b4',
    version: '4.0.0.0',
    wrong_time: 1
  },
  msg: ''
}

const mockLogin = jest.fn().mockImplementation((user) => {
  return new Promise((resolve, reject) => {
    resolve(data)
  })
})
const mockGetAboutKap = jest.fn().mockResolvedValue({code: '000', data: '', msg: ''})
const mockLicenseFile = jest.fn().mockImplementation(() => {
  return new Promise((resolve) => {
    resolve({code: '000', data: {'ke.dates': '2020-04-10,2020-07-13'}, msg: ''})
  })
})
const mockLicenseDialog = jest.fn()
const saveUser = jest.fn()
const mockAlert = jest.fn().mockImplementation(() => {})
let store = new Vuex.Store({
  actions: {
    [types.LOGIN]: mockLogin,
    [types.GET_ABOUTKAP]: mockGetAboutKap,
    [types.TRIAL_LICENSE_FILE]: mockLicenseFile,
    [types.IS_CLOUD]: jest.fn().mockResolvedValue()
  },
  mutations: {
    [types.SAVE_CURRENT_LOGIN_USER]: saveUser,
    [types.TOGGLE_LICENSE_DIALOG]: mockLicenseDialog
  },
  state: {
    system: {
      serverAboutKap: {
        code: '001',
        'ke.dates': null
      }
    },
    user: {},
    config: {
      overLock: true
    }
  },
  getters: {
    supportUrl: () => {},
    systemActions: () => {}
  }
})
const mockLoginReject = jest.fn().mockRejectedValue({message: 'error'})
const mockGetAboutKapReject = jest.fn().mockRejectedValue({message: 'error'})
const mockLicenseFileReject = jest.fn().mockRejectedValue({message: 'error'})
let store1 = new Vuex.Store({
  actions: {
    [types.LOGIN]: mockLoginReject,
    [types.GET_ABOUTKAP]: mockGetAboutKapReject,
    [types.TRIAL_LICENSE_FILE]: mockLicenseFileReject,
    [types.IS_CLOUD]: jest.fn().mockResolvedValue()
  },
  state: {
    system: {
      serverAboutKap: {
        code: '001',
        'ke.dates': null
      }
    },
    user: {},
    config: {
      overLock: true
    }
  },
  getters: {
    supportUrl: () => {},
    systemActions: () => {}
  }
})

let mockHandleSuccess = null
let mockHandleError = jest.spyOn(util, 'handleError').mockImplementation()

let factory = (type, store, otherMocks) => {
  mockHandleSuccess = type !== 'applyLicense' ? jest.spyOn(util, 'handleSuccess').mockImplementation((res, callback, errorCallback) => {
    callback(data.data)
    errorCallback && errorCallback()
  }) : jest.spyOn(util, 'handleSuccess').mockImplementation((res, callback) => {
    callback({'ke.dates': '2020-04-10,2020-07-13'})
  })
  return shallowMount(Login, {
    localVue,
    store,
    router,
    mocks: {
      handleSuccess: mockHandleSuccess,
      handleError: mockHandleError,
      ...otherMocks
    },
    components: {
      'license': License,
      kapIconButton: iconBtn
    }
  })
}

describe('Component Login', () => {
  it('init', async () => {
    const wrapper = await factory('', store)
    expect(wrapper.element.tagName).toBe('DIV')
    expect(wrapper.classes()).toEqual([])
    expect(wrapper.vm.$data.user).toEqual({ username: null, password: '' })
    expect(wrapper.find('.login-footer').text()).toEqual('©2021 Kyligence Inc. All rights reserved.')
    expect(mockGetAboutKap).toBeCalled()
    expect(mockHandleSuccess).toBeCalled()
    expect(mockHandleError).toBeCalled()
    expect(wrapper.vm.$data.hasLicense).toBe(true)
    wrapper.destroy()
  })
  it('dom element', async () => {
    const wrapper = await factory('', store)
    await wrapper.setData({
      user: {
        username: 'ADMIN',
        password: 'KYLIN'
      }
    })
    // await wrapper.update()
    expect(wrapper.find('.ksd-fright').findAll('li').length).toBe(2)
    const guideList = wrapper.find('.login-box').find('ul').findAll('li')
    expect(guideList.length).toBe(3)
    expect(guideList.at(0).find('a').attributes().href).toEqual('http://kyligence.io/enterprise/#analytics')
    expect(guideList.at(1).find('a').attributes().href).toEqual('http://docs.kyligence.io/?lang=undefined')
    expect(guideList.at(2).find('a').attributes().href).toEqual('mailto:info@Kyligence.io')
    // expect(wrapper.find('.forget-pwd').attributes().style).toBe('')
    // expect(wrapper.find('.forget-pwd').text()).toBe('Forget Password')
    const form = wrapper.find('.input_group').findAll('el-input-stub')
    expect(form.length).toBe(2)
    expect(form.at(0).attributes().placeholder).toBe('Username')
    expect(form.at(1).attributes().placeholder).toBe('Password')
    form.at(0).element.__vue__.$emit('input', 'test')
    form.at(1).element.__vue__.$emit('input', '123456')
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.$data.user).toEqual({'password': '123456', 'username': 'test'})
    wrapper.destroy()
  })
  it('login success', async () => {
    const wrapper = await factory('', store)
    await wrapper.setData({
      user: {
        username: 'ADMIN',
        password: 'KYLIN'
      }
    })
    let options = {
      ...wrapper.vm,
      $refs: {
        loginForm: {
          validate: function (callback) { callback(true) }
        },
        loginBtn: {
          loading: false
        },
        applyLicenseForm: {
          resetFields: jest.fn().mockResolvedValue(true)
        }
      },
      setCurUser: saveUser,
      loginEnd: () => {},
      loginSuccess: wrapper.vm.$data.loginSuccess,
      showLicenseCheck: false
    }
    // await wrapper.update()
    expect(wrapper.vm.$data.user).toEqual({ username: 'ADMIN', password: 'KYLIN' })
    // wrapper.vm.onLoginSubmit.call(options)
    Login.methods.onLoginSubmit.call(options)
    expect(Vue.http.headers.common['Authorization']).toBe('Basic QURNSU46S1lMSU4=')
    expect(mockLogin).toBeCalled()
    expect(mockHandleSuccess).toBeCalled()
    await wrapper.vm.$nextTick()
    expect(saveUser).toHaveBeenCalledWith({user: data.data})
    expect(options.loginSuccess).toBe(true)
    expect(options.showLicenseCheck).toBe(true)
    expect(localStorage.getItem('username')).toBe('ADMIN')
    expect(wrapper.vm.$store.state.config.overLock).toBe(false)
    Login.methods.loginEnd.call(options)
    expect(Vue.http.headers.common['Authorization']).toBe('')
    wrapper.vm.$data.hasLicenseMsg = true
    // wrapper.vm.$store.state.system.serverAboutKap.code = '002'
    // await wrapper.update()
    Login.methods.loginEnd.call(options)
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.$route).toEqual({'fullPath': '/', 'hash': '', 'matched': [], 'meta': {}, 'name': null, 'params': {}, 'path': '/', 'query': {}})
    wrapper.destroy()
  })
  it('login api error', async () => {
    const wp = await factory('', store1)
    let options1 = {
      ...wp.vm,
      $refs: {
        loginForm: {
          validate: function (callback) { callback(true) }
        },
        loginBtn: {
          loading: false
        }
      },
      showLicenseCheck: false
    }
    Login.methods.onLoginSubmit.call(options1)
    expect(Vue.http.headers.common['Authorization']).toBe('Basic QURNSU46')
    expect(mockHandleError).toBeCalled()
    wp.vm.$store.state.system.serverAboutKap.code = '002'
    Login.methods.onLoginSubmit.call(options1)
    // expect(wp.vm.$data).toBe(true)
    expect(options1.showLicenseCheck).toBe(false)
    wp.destroy()
  })
  it('close', async () => {
    const wrapper = await factory('', store)
    wrapper.find('.dialog-footer').find('el-button-stub[name="cancelUpdate"]').element.__vue__.$emit('click')
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.$data.hasLicense).toBeFalsy()
    expect(wrapper.vm.$data.loadCheck).toBeFalsy()
    await wrapper.vm.$nextTick()
    expect(wrapper.find('.license-msg').findAll('el-button-stub').at(0).text()).toBe('I Know')
    wrapper.find('.license-msg').findAll('el-button-stub').at(0).element.__vue__.$emit('click')
    expect(wrapper.vm.$data.showLicenseCheck).toBe(false)
    wrapper.setData({
      loginSuccess: true
    })
    wrapper.vm.$store.state.system.serverAboutKap.code = '001'
    await wrapper.vm.$nextTick()
    wrapper.find('.license-msg').findAll('el-button-stub').at(0).element.__vue__.$emit('click')
    expect(wrapper.vm.$data.showLicenseCheck).toBe(false)
    expect(wrapper.vm.$route).toEqual({'fullPath': '/dashboard', 'hash': '', 'matched': [], 'meta': {}, 'name': null, 'params': {}, 'path': '/dashboard', 'query': {}})
    wrapper.destroy()
  })
  it('apply license', async () => {
    const wrapper = await factory('applyLicense', store)
    let that = {
      ...wrapper.vm,
      $refs: {
        applyLicenseForm: {
          validate: function (callback) { callback(true) }
        }
      },
      '$alert': mockAlert
    }
    wrapper.vm.apply()
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.hasLicense).toBeFalsy()
    expect(wrapper.vm.$data.applyLicense).toBeTruthy()
    expect(wrapper.vm.$data.changeDialog).toBeTruthy()
    Login.methods.submitApply.call(that)
    expect(mockLicenseFile).toBeCalled()
    expect(wrapper.vm.$data.applyLoading).toBe(false)
    expect(wrapper.vm.$data.userMessage).toEqual({'category': 'undefined.x', 'company': '', 'email': '', 'lang': 'en', 'productType': 'kap', 'username': ''})
    expect(mockHandleSuccess).toBeCalled()
    expect(wrapper.vm.$store.state.system.serverAboutKap).toEqual({'code': '001', 'ke.dates': null})
    wrapper.destroy()
    const wp = await factory('applyLicense', store1)
    let th = {
      ...wp.vm,
      $refs: {
        applyLicenseForm: {
          validate: function (callback) { callback(true) }
        }
      }
    }
    Login.methods.submitApply.call(th)
    expect(mockHandleError).toBeCalled()
    expect(wp.vm.$data.applyLoading).toBe(false)
    wp.destroy()
  })
  it('close apply dialog', async () => {
    const wrapper = await factory('', store)
    wrapper.vm.$refs = {
      applyLicenseForm: {
        resetFields: jest.fn().mockResolvedValue(true)
      }
    }
    expect(wrapper.find('.applyLicense').findAll('el-button-stub').at(0).text()).toBe('Cancel')
    wrapper.find('.applyLicense').findAll('el-button-stub').at(0).element.__vue__.$emit('click')
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.updateLicenseVisible).toBe(true)
    expect(wrapper.vm.$data.applyLicense).toBe(false)
    wrapper.destroy()
  })
  it('check lastTime', async () => {
    const wrapper = await factory('', store)
    expect(wrapper.vm.lastTime('')).toBe(0)
    expect(wrapper.vm.lastTime('2020-04-10,2020-07-13')).not.toBeNaN()
    expect(wrapper.vm.lastTime('2020-04-10,2020-04-10')).toBe(0)
    wrapper.destroy()
  })
  it('license success', async () => {
    let wrapper = await factory('', store)
    wrapper.vm.licenseValidSuccess(true)
    expect(wrapper.vm.$data.hasLicense).toBeTruthy()
    expect(wrapper.vm.$data.showLicenseCheck).toBeTruthy()
    wrapper.vm.$store.state.system.serverAboutKap.code = '000'
    await wrapper.vm.$nextTick()
    wrapper.vm.licenseValidSuccess(true)
    expect(wrapper.vm.hasLicense).toBeFalsy()
    expect(mockLicenseDialog).toBeCalled()
    wrapper.destroy()
  })
  it('submit license form', async () => {
    const wrapper = await factory('', store)
    wrapper.find('.updateKAPLicense').findAll('el-button-stub').at(1).element.__vue__.$emit('click')
    expect(wrapper.vm.loadCheck).toBeTruthy()
    wrapper.destroy()
  })
  it('validate', async () => {
    const wrapper = await factory('', store, { personalEmail: ['name', '121'] })
    const fn = jest.fn()
    wrapper.vm.validateEmail(null, '3234235232432jljjfsdkfjlwe23243234235232432jljjfsdkfjlwe2324', fn)
    expect(fn).toBeCalled()
    expect(fn).toHaveBeenLastCalledWith(new Error('The maximum is 50 characters'))
    wrapper.vm.validateEmail(null, '323414@qq.com', fn)
    expect(fn).toHaveBeenCalledWith(new Error('Please enter your enterprise email.'))
    wrapper.vm.validateEmail(null, '', fn)
    expect(fn).toHaveBeenCalledWith()
    wrapper.vm.validateName(null, '12345678993423423432@#$%^^&&**#@$12345678993423423432@#$%^^&&**#@$', fn)
    expect(fn).toHaveBeenLastCalledWith(new Error('Only Chinese characters, letters, digits and space are supported. The maximum is 50 characters.'))
    wrapper.vm.validateName(null, '12345', fn)
    expect(fn).toHaveBeenLastCalledWith()
    wrapper.destroy()
  })
})
