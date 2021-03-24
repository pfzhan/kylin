import { shallowMount } from '@vue/test-utils'
import Vuex from 'vuex'
import { localVue } from '../../../../test/common/spec_common'
import * as business from '../../../util/business'
import License from '../license.vue'

jest.useFakeTimers()

const mockHandleError = jest.spyOn(business, 'handleError').mockRejectedValue(false)
const mockHandleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation(({data, code, msg}, callback) => {
  callback && callback(data, code, msg)
})

const mockApi = {
  mockSaveLicenseContent: jest.fn().mockImplementation(() => {
    return {
      then: (successCallback) => {
        successCallback && successCallback({
          data: {'ke.dates': '2019-06-01,2021-03-30'},
          code: '000',
          msg: ''
        })
      }
    }
  }),
  mockIsCloud: jest.fn().mockImplementation(() => {
    return {
      then: (successCallback, errorCallback) => {
        successCallback && successCallback({data: true})
        errorCallback && errorCallback()
      }
    }
  })
}

const store = new Vuex.Store({
  state: {
    system: {
      serverAboutKap: {
        msg: '',
        code: '000',
        version: '4',
        'ke.dates': '2019-06-01,2019-07-30',
        'ke.license.category': '4.x',
        'ke.license.info': 'Evaluation license for Kyligence Enterprise',
        'ke.license.isCloud': false,
        'ke.license.isEnterprise': false,
        'ke.license.isEvaluation': true,
        'ke.license.isTest': false,
        'ke.license.level': 'professional',
        'ke.license.nodes': 'Unlimited',
        'ke.license.serviceEnd': '2019-07-30',
        'ke.license.statement': 'Evaluation license for Kyligence Enterprise↵Category: 4.x↵SLA Service: NO↵Volume: 1↵Level: professional↵Insight License: 5 users; evaluation; 2019-06-01,2019-07-30↵',
        'ke.license.volume': '1099511627776'
      }
    }
  },
  actions: {
    'SAVE_LICENSE_CONTENT': mockApi.mockSaveLicenseContent,
    'IS_CLOUD': mockApi.mockIsCloud
  }
})

const  wrapper = shallowMount(License, {
  store,
  localVue,
  propsData: {
    updateLicenseVisible: ''
  },
  mocks: {
    handleSuccess: mockHandleSuccess,
    handleError: mockHandleError
  }
})

describe('Component License', () => {
  it('init', async () => {
    expect(mockApi.mockIsCloud.mock.calls[0][1]).toEqual()
    expect(mockHandleSuccess).toBeCalled()
    expect(wrapper.vm.isRunInCloud).toBeTruthy()
    expect(mockHandleError).toBeCalled()

    wrapper.vm.$refs = {
      licenseForm: {
        validate: jest.fn().mockImplementation((callback) => callback(true)),
        clearValidate: jest.fn(),
        validateField: jest.fn()
      },
      upload: {
        submit: jest.fn()
      }
    }
    await wrapper.vm.$emit('licenseFormValid')
    expect(wrapper.vm.$refs.upload.submit).toBeCalled()

    await wrapper.setData({addLicenseType: false})
    await wrapper.vm.$emit('licenseFormValid')
    expect(mockApi.mockSaveLicenseContent.mock.calls[0][1]).toEqual('')
    expect(mockHandleSuccess).toBeCalled()
    expect(wrapper.vm.$store.state.system.serverAboutKap).toEqual({'code': '000', 'ke.dates': '2019-06-01,2021-03-30', 'ke.license.category': '4.x', 'ke.license.info': 'Evaluation license for Kyligence Enterprise', 'ke.license.isCloud': false, 'ke.license.isEnterprise': false, 'ke.license.isEvaluation': true, 'ke.license.isTest': false, 'ke.license.level': 'professional', 'ke.license.nodes': 'Unlimited', 'ke.license.serviceEnd': '2019-07-30', 'ke.license.statement': 'Evaluation license for Kyligence Enterprise↵Category: 4.x↵SLA Service: NO↵Volume: 1↵Level: professional↵Insight License: 5 users; evaluation; 2019-06-01,2019-07-30↵', 'ke.license.volume': '1099511627776', 'msg': '', 'version': '4'})
    expect(wrapper.emitted().validSuccess).toEqual([[true]])
    expect(wrapper.vm.$refs.licenseForm.clearValidate).toBeCalled()

    wrapper.vm.$store._actions.SAVE_LICENSE_CONTENT = [jest.fn().mockImplementation(() => {
      return {
        then: (successCallback, errorCallback) => {
          errorCallback && errorCallback(false)
        }
      }
    })]
    await wrapper.vm.$emit('licenseFormValid')
    expect(mockHandleError).toBeCalled()
    expect(wrapper.emitted().validSuccess[1]).toEqual([false])

    await wrapper.setData({fileSizeError: true})
    await wrapper.vm.$emit('licenseFormValid')
    expect(wrapper.emitted().validSuccess[2]).toEqual([false])

    await wrapper.setData({fileSizeError: false})
  })
  it('computed', async () => {
    expect(wrapper.vm.actionUrl).toBe('/kylin/api/system/license/file')
    expect(wrapper.vm.uploadHeader).toEqual({'Accept-Language': 'en'})
    await wrapper.setData({lang: 'zh-cn'})
    expect(wrapper.vm.uploadHeader).toEqual({'Accept-Language': 'cn'})
    expect(wrapper.vm.hasLicense).toBe('2019-06-01,2021-03-30')
    expect(wrapper.vm.serverAboutKap).toEqual({'code': '000', 'ke.dates': '2019-06-01,2021-03-30', 'ke.license.category': '4.x', 'ke.license.info': 'Evaluation license for Kyligence Enterprise', 'ke.license.isCloud': false, 'ke.license.isEnterprise': false, 'ke.license.isEvaluation': true, 'ke.license.isTest': false, 'ke.license.level': 'professional', 'ke.license.nodes': 'Unlimited', 'ke.license.serviceEnd': '2019-07-30', 'ke.license.statement': 'Evaluation license for Kyligence Enterprise↵Category: 4.x↵SLA Service: NO↵Volume: 1↵Level: professional↵Insight License: 5 users; evaluation; 2019-06-01,2019-07-30↵', 'ke.license.volume': '1099511627776', 'msg': '', 'version': '4'})
    expect(wrapper.vm.licenseRange).toBe('2019-06-01 To 2021-03-30')

    wrapper.vm.$store.state.system.serverAboutKap = {'code': '000', 'ke.dates': '', 'ke.license.category': '4.x', 'ke.license.info': 'Evaluation license for Kyligence Enterprise', 'ke.license.isCloud': false, 'ke.license.isEnterprise': false, 'ke.license.isEvaluation': true, 'ke.license.isTest': false, 'ke.license.level': 'professional', 'ke.license.nodes': 'Unlimited', 'ke.license.serviceEnd': '', 'ke.license.statement': 'Evaluation license for Kyligence Enterprise↵Category: 4.x↵SLA Service: NO↵Volume: 1↵Level: professional↵Insight License: 5 users; evaluation; 2019-06-01,2019-07-30↵', 'ke.license.volume': '1099511627776', 'msg': '', 'version': '4'}
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.licenseRange).toBe('')

    wrapper.vm.$store.state.system.serverAboutKap = null
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.serverAboutKap).toEqual({})
  })
  it('methods', async () => {
    wrapper.vm.applyLicense()
    expect(wrapper.emitted().requestLicense).toEqual([[]])

    wrapper.vm.switchLicenseType()
    expect(wrapper.vm.licenseForm).toEqual({'fileList': [], 'licenseContent': ''})
    expect(wrapper.vm.fileSizeError).toBeFalsy()

    wrapper.vm.$store.state.system.serverAboutKap = {}
    await wrapper.vm.$nextTick()
    wrapper.vm.successFile({'code': '000', data: {'ke.dates': '2021-03-01,2021-03-30', 'ke.license.category': '4.x', 'ke.license.info': 'Evaluation license for Kyligence Enterprise', 'ke.license.isCloud': false, 'ke.license.isEnterprise': false, 'ke.license.isEvaluation': true, 'ke.license.isTest': false, 'ke.license.level': 'professional', 'ke.license.nodes': 'Unlimited', 'ke.license.serviceEnd': '2021-03-30', 'ke.license.statement': 'Evaluation license for Kyligence Enterprise↵Category: 4.x↵SLA Service: NO↵Volume: 1↵Level: professional↵Insight License: 5 users; evaluation; 2019-06-01,2019-07-30↵', 'ke.license.volume': '1099511627776', 'version': '4' }, 'msg': ''})
    jest.runAllTimers()
    expect(wrapper.vm.$store.state.system.serverAboutKap).toEqual({'code': '000', 'ke.dates': '2021-03-01,2021-03-30', 'ke.license.category': '4.x', 'ke.license.info': 'Evaluation license for Kyligence Enterprise', 'ke.license.isCloud': false, 'ke.license.isEnterprise': false, 'ke.license.isEvaluation': true, 'ke.license.isTest': false, 'ke.license.level': 'professional', 'ke.license.nodes': 'Unlimited', 'ke.license.serviceEnd': '2021-03-30', 'ke.license.statement': 'Evaluation license for Kyligence Enterprise↵Category: 4.x↵SLA Service: NO↵Volume: 1↵Level: professional↵Insight License: 5 users; evaluation; 2019-06-01,2019-07-30↵', 'ke.license.volume': '1099511627776', 'msg': '', 'version': '4'})
    expect(wrapper.emitted().validSuccess[3]).toEqual([true])

    wrapper.vm.errorFile({message: 'error files', status: '999'})
    expect(mockHandleError.mock.calls[2]).toEqual([{'data': {}, 'status': '999'}])
    expect(wrapper.emitted().validSuccess[4]).toEqual([false])

    wrapper.vm.errorFile({message: JSON.stringify({}), status: '999'})
    expect(mockHandleError.mock.calls[3]).toEqual([{'data': {}, 'status': '999'}])
    expect(wrapper.emitted().validSuccess[5]).toEqual([false])

    wrapper.vm.handleRemove()
    expect(wrapper.vm.licenseForm.fileList).toEqual([])
    expect(wrapper.vm.fileSizeError).toBeFalsy()

    const file = [{
      name: 'icomoon.svg',
      percentage: 0,
      raw: {
        lastModified: 1615969055984,
        lastModifiedDate: 'Wed Mar 17 2021 16:17:35 GMT+0800 (China Standard Time) {}',
        name: 'icomoon.svg',
        size: 121250,
        type: 'image/svg+xml',
        uid: 1616050006226,
        webkitRelativePath: ''
      },
      size: 121250,
      status: 'ready',
      uid: 1616050006226,
      url: 'blob:http://localhost:8080/20b66d89-e313-4557-9270-39e6b6ce78fd'
    }]

    wrapper.vm.changeFile(null, file)
    expect(wrapper.vm.licenseForm.fileList).toEqual(file)
    expect(wrapper.vm.$refs.licenseForm.validateField).toBeCalledWith('fileList')
    expect(wrapper.vm.fileSizeError).toBeFalsy()

    file.push({
      name: 'text.csv',
      percentage: 0,
      raw: {
        lastModified: 1615969056984,
        lastModifiedDate: 'Wed Mar 17 2021 16:17:35 GMT+0800 (China Standard Time) {}',
        name: 'text.csv',
        size: 5364130,
        type: 'csv',
        uid: 1616050006226,
        webkitRelativePath: ''
      },
      size: 5364130,
      status: 'ready',
      uid: 1616050007226,
      url: 'blob:http://localhost:8080/20b66d89-e313-4557-9270-39e6b6ce78fd'
    })
    wrapper.vm.changeFile(null, file)
    expect(wrapper.vm.licenseForm.fileList).toEqual([{'name': 'text.csv', 'percentage': 0, 'raw': {'lastModified': 1615969056984, 'lastModifiedDate': 'Wed Mar 17 2021 16:17:35 GMT+0800 (China Standard Time) {}', 'name': 'text.csv', 'size': 5364130, 'type': 'csv', 'uid': 1616050006226, 'webkitRelativePath': ''}, 'size': 5364130, 'status': 'ready', 'uid': 1616050007226, 'url': 'blob:http://localhost:8080/20b66d89-e313-4557-9270-39e6b6ce78fd'}])
    expect(wrapper.vm.$refs.licenseForm.validateField).toBeCalledWith('fileList')
    expect(wrapper.vm.fileSizeError).toBeTruthy()
    expect(wrapper.emitted().validSuccess[6]).toEqual([false])

    wrapper.vm.changeFile(null, [])
    expect(wrapper.vm.fileSizeError).toBeFalsy()

    await wrapper.setProps({updateLicenseVisible: true})
    expect(wrapper.vm.licenseForm).toEqual({'fileList': [], 'licenseContent': ''})
    expect(wrapper.vm.lang).toBe('en')

    localStorage.setItem('kystudio_lang', 'zh-cn')
    await wrapper.setProps({updateLicenseVisible: false})
    expect(wrapper.vm.licenseForm).toEqual({'fileList': [], 'licenseContent': ''})
    expect(wrapper.vm.lang).toBe('zh-cn')
  })
})
