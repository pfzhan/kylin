import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import Help from '../help.vue'
import aboutKap from '../about_kap.vue'
import updateLicense from '../../user/license.vue'
import * as business from '../../../util/business'
import Vuex from 'vuex'
import { call } from 'function-bind'

jest.useFakeTimers()

const mockHandleError = jest.spyOn(business, 'handleError').mockRejectedValue(false)
const mockHandleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback) => {
  callback && callback(res)
})
const mockAlert = jest.fn().mockImplementation()

const mockApi = {
  mockGetAboutKap: jest.fn().mockRejectedValue(false),
  mockTrialLicenseFile: jest.fn().mockImplementation(() => {
    return {
      then: (successCallback, errorCallback) => {
        successCallback && successCallback({
          'ke.dates': "2019-06-01,2019-07-30",
          'ke.license.category': "4.x",
          'ke.license.info': "Evaluation license for Kyligence Enterprise"
        })
        errorCallback && errorCallback()
      }
    }
  }),
  mockCheckSSB: jest.fn().mockImplementation(() => {
    return {
      then: (successCallback, errorCallback) => {
        successCallback && successCallback(true)
        errorCallback && errorCallback()
      }
    }
  }),
  mockImportSSBDatabase: jest.fn().mockImplementation(),
  mockToggleLicenseDialog: jest.fn().mockImplementation()
}

const store = new Vuex.Store({
  state: {
    system: {
      serverAboutKap: {
        msg: '',
        code: '000',
        version: '4',
        'ke.dates': "2019-06-01,2019-07-30",
        'ke.license.category': "4.x",
        'ke.license.info': "Evaluation license for Kyligence Enterprise",
        'ke.license.isCloud': false,
        'ke.license.isEnterprise': false,
        'ke.license.isEvaluation': true,
        'ke.license.isTest': false,
        'ke.license.level': "professional",
        'ke.license.nodes': "Unlimited",
        'ke.license.serviceEnd': "2019-07-30",
        'ke.license.statement': "Evaluation license for Kyligence Enterprise↵Category: 4.x↵SLA Service: NO↵Volume: 1↵Level: professional↵Insight License: 5 users; evaluation; 2019-06-01,2019-07-30↵",
        'ke.license.volume': "1099511627776"
      },
      guideConfig: {
        guideModeCheckDialog: false
      }
    }
  },
  actions: {
    'GET_ABOUTKAP': mockApi.mockGetAboutKap,
    'TRIAL_LICENSE_FILE': mockApi.mockTrialLicenseFile,
    'CHECK_SSB': mockApi.mockCheckSSB,
    'IMPORT_SSB_DATABASE': mockApi.mockImportSSBDatabase
  },
  mutations: {
    'TOGGLE_LICENSE_DIALOG': mockApi.mockToggleLicenseDialog
  },
  getters: {
    supportUrl () {
      return 'https://support.kyligence.io/#/?lang=zh-cn'
    },
    systemActions () {
      return ["userGuide", "viewAllProjectJobs", "updateLicense"]
    }
  }
})

const wrapper = shallowMount(Help, {
  store,
  localVue,
  propsData: {
    isLogin: true
  },
  components: {
    'about_kap': aboutKap,
    'update_license': updateLicense
  },
  mocks: {
    handleSuccess: mockHandleSuccess,
    handleError: mockHandleError,
    $alert: mockAlert
  }
})

describe('Component Help', () => {
  it('computed', () => {
    expect(Object.keys(wrapper.vm.serverAboutKap)).toEqual(["msg", "code", "version", "ke.dates", "ke.license.category", "ke.license.info", "ke.license.isCloud", "ke.license.isEnterprise", "ke.license.isEvaluation", "ke.license.isTest", "ke.license.level", "ke.license.nodes", "ke.license.serviceEnd", "ke.license.statement", "ke.license.volume"])
    expect(JSON.stringify(wrapper.vm.userRules)).toEqual("{\"email\":[{\"required\":true,\"message\":\"Please enter your email.\",\"trigger\":\"blur\"},{\"type\":\"email\",\"message\":\"Please enter a valid email address.\",\"trigger\":\"blur\"},{\"trigger\":\"blur\"}],\"company\":[{\"required\":true,\"message\":\"Please enter your company name.\",\"trigger\":\"blur\"},{\"trigger\":\"blur\"}],\"username\":[{\"required\":true,\"message\":\"Please enter your name.\",\"trigger\":\"blur\"},{\"trigger\":\"blur\"}]}")
  })
  it('methods', async () => {
    await wrapper.vm.handleCommand('aboutkap')
    expect(mockApi.mockGetAboutKap).toBeCalled()
    expect(mockHandleError).toBeCalled()
    expect(wrapper.vm.aboutKapVisible).toBeTruthy()

    await wrapper.vm.handleCommand('updatelicense')
    expect(wrapper.vm.aboutKapVisible).toBeTruthy()

    await wrapper.vm.handleCommand('guide')
    expect(mockApi.mockCheckSSB).toBeCalled()
    expect(mockHandleSuccess).toBeCalled()
    expect(wrapper.vm.$store.state.system.guideConfig.guideModeCheckDialog).toBeTruthy()

    expect(mockHandleError).toBeCalled()

    wrapper.vm.$store._actions.CHECK_SSB = [jest.fn().mockImplementation(() => {
      return {
        then: (successCallback, errorCallback) => {
          successCallback && successCallback(false)
          errorCallback && errorCallback()
        }
      }
    })]
    await wrapper.vm.$nextTick()
    await wrapper.vm.handleCommand('guide')
    expect(wrapper.vm.isShowImportError).toBeTruthy()
    expect(wrapper.vm.importSSBvisible).toBeTruthy()
    

    wrapper.vm.startGuide()
    expect(wrapper.vm.importSSBvisible).toBeFalsy()
    expect(wrapper.vm.$store.state.system.guideConfig.guideModeCheckDialog).toBeTruthy()

    wrapper.vm.importLoading()
    jest.runAllTimers()
    expect(wrapper.vm.ST).toBe(1)
    expect(typeof wrapper.vm.percent).toBe('number')

    await wrapper.setData({percent: 100})
    wrapper.vm.importLoading()
    jest.runAllTimers()
    expect(wrapper.vm.ST).toBe(2)

    wrapper.vm.resetImportSSB()
    expect(wrapper.vm.importSSBvisible).toBeFalsy()
    expect(wrapper.vm.isImportSuccess).toBeFalsy()
    expect(wrapper.vm.isShowImportError).toBeFalsy()

    wrapper.vm.resetUpdate()
    expect(wrapper.vm.loadCheck).toBeFalsy()
    expect(wrapper.vm.updateLicenseVisible).toBeFalsy()

    wrapper.vm.closeLoginForm()
    expect(wrapper.vm.kyBotUploadVisible).toBeFalsy()

    wrapper.vm.closeLoginOpenKybot()
    expect(wrapper.vm.infoKybotVisible).toBeTruthy()
    expect(wrapper.vm.kyBotUploadVisible).toBeFalsy()

    wrapper.vm.$refs = {
      licenseEnter: {
        $emit: jest.fn()
      }
    }
    wrapper.vm.licenseForm()
    expect(wrapper.vm.loadCheck).toBeTruthy()
    expect(wrapper.vm.$refs.licenseEnter.$emit).toBeCalledWith('licenseFormValid')

    wrapper.vm.handleClose()
    expect(wrapper.vm.showLicenseCheck).toBeFalsy()

    wrapper.vm.licenseValidSuccess(true)
    expect(mockApi.mockToggleLicenseDialog.mock.calls[0][1]).toEqual(true)
    expect(wrapper.vm.updateLicenseVisible).toBeFalsy()
    expect(wrapper.vm.loadCheck).toBeFalsy()

    wrapper.vm.$store.state.system.serverAboutKap['code'] = '001'
    await wrapper.vm.$nextTick()
    wrapper.vm.licenseValidSuccess(true)
    expect(wrapper.vm.updateLicenseVisible).toBeFalsy()
    expect(wrapper.vm.showLicenseCheck).toBeTruthy()

    expect(wrapper.vm.license('LICENSE .')).toBe('LICENSE .')
    expect(wrapper.vm.license()).toBe('N/A')

    wrapper.vm.apply()
    expect(wrapper.vm.updateLicenseVisible).toBeFalsy()
    expect(wrapper.vm.applyLicense).toBeTruthy()
    expect(wrapper.vm.changeDialog).toBeTruthy()

    wrapper.vm.$refs.applyLicenseForm = {
      resetFields: jest.fn(),
      validate: jest.fn().mockImplementation((callback) => callback(true))
    }
    wrapper.vm.closeApplyLicense()
    expect(wrapper.vm.$refs.applyLicenseForm.resetFields).toBeCalled()
    expect(wrapper.vm.updateLicenseVisible).toBeTruthy()
    expect(wrapper.vm.applyLicense).toBeFalsy()

    await wrapper.vm.submitApply()
    expect(wrapper.vm.userMessage.category).toBe('4.x')
    // expect(wrapper.vm.applyLoading).toBeTruthy()
    expect(mockApi.mockTrialLicenseFile.mock.calls[0][1]).toEqual({"category": "4.x", "company": "", "email": "", "lang": "en", "product_type": "kap", "username": ""})
    expect(mockAlert).toBeCalledWith('Expired On:2019-07-30', 'Evaluation License', {"cancelConfirmButton": true, "type": "warning"})

    expect(mockHandleError).toBeCalled()

    const currentYear = new Date().getFullYear() + 1
    wrapper.vm.$store._actions.TRIAL_LICENSE_FILE = [jest.fn().mockImplementation(() => {
      return {
        then: (successCallback, errorCallback) => {
          successCallback && successCallback({
            'ke.dates': `2019-06-01, ${currentYear}-01-01`,
            'ke.license.category': "4.x",
            'ke.license.info': "Evaluation license for Kyligence Enterprise"
          })
          errorCallback && errorCallback()
        }
      }
    })]
    await wrapper.vm.$nextTick()
    await wrapper.vm.submitApply()
    expect(wrapper.vm.userMessage.category).toBe('4.x')
    expect(mockApi.mockTrialLicenseFile.mock.calls[0][1]).toEqual({"category": "4.x", "company": "", "email": "", "lang": "en", "product_type": "kap", "username": ""})
    expect(mockAlert).toBeCalledWith('Evaluation Period:2019-06-01, 2022-01-01', 'Evaluation License', {"cancelConfirmButton": true, "type": "success"})

    const callback = jest.fn()
    wrapper.vm.validateEmail(null, 'testcontenttestcontenttestcontenttestcontenttestcontenttestcontenttestcontenttestcontent', callback)
    expect(callback.mock.calls[0].toString()).toBe("Error: The maximum is 50 characters")

    wrapper.vm.validateEmail(null, 'mail.qq.com', callback)
    expect(callback.mock.calls[1].toString()).toBe('Error: Please enter a corporate email.')

    wrapper.vm.validateEmail(null, 'mail@kyligence.io', callback)
    expect(callback.mock.calls[2].toString()).toBe('')

    wrapper.vm.validateEmail(null, '', callback)
    expect(callback.mock.calls[3].toString()).toBe('')

    wrapper.vm.validateName(null, 'testcontenttestcontenttestcontenttestcontenttestcontenttestcontenttestcontenttestcontent', callback)
    expect(callback.mock.calls[5].toString()).toBe('Error: Only Chinese characters, letters, digits and space are supported. The maximum is 50 characters.')

    wrapper.vm.validateName(null, 'test', callback)
    expect(callback.mock.calls[6].toString()).toBe('')

    expect(wrapper.vm.lastTime('2021-03-17')).toBe(0)
  })
})
