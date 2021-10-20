import { shallowMount } from '@vue/test-utils'
import { localVue } from '../../../../test/common/spec_common'
import SettingModel from '../SettingModel/SettingModel.vue'
import kapPager from 'components/common/kap_pager.vue'
import Vuex from 'vuex'
import * as utils from '../../../util/index'
import * as business from '../../../util/business'

// const mockHandleSuccess = jest.spyOn(business, 'handleSuccess').mockImplementation((res, callback) => {
//   callback && callback()
// })
const mockHandleSuccessAsync = jest.spyOn(utils, 'handleSuccessAsync').mockImplementation((res) => {
  return Promise.resolve(res)
})
// const mockHandleError = jest.spyOn(business, 'handleError').mockImplementation()
const mockKapConfirm = jest.spyOn(business, 'kapConfirm').mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve()
  })
})

const settingValue = [
  {
    alias: 'test0001',
    auto_merge_enabled: null,
    auto_merge_time_ranges: null,
    config_last_modified: 0,
    config_last_modifier: null,
    model: '15df9e90-7560-4cf3-817b-70b4104a4f66',
    override_props: {},
    retention_range: null,
    volatile_range: null
  }
]
const mockApi = {
  mockLoadModelConfigList: jest.fn().mockImplementation(() => {
    return Promise.resolve({
      limit: 10,
      offset: 0,
      total_size: 15,
      value: settingValue
    })
  }),
  mockUpdateModelConfig: jest.fn().mockImplementation(() => {
    return {
      then: (callback, errorCallback) => {
        callback()
        errorCallback()
      }
    }
  })
}

const store = new Vuex.Store({
  getters: {
    currentSelectedProject: () => {
      return 'learn_kylin'
    },
    isAutoProject: () => {
      return true
    }
  },
  actions: {
    'LOAD_MODEL_CONFIG_LIST': mockApi.mockLoadModelConfigList,
    'UPDATE_MODEL_CONFIG': mockApi.mockUpdateModelConfig
  }
})

const wrapper = shallowMount(SettingModel, {
  store,
  localVue,
  // mocks: {
  //   kapConfirm: mockKapConfirm,
  //   handleError: mockHandleError,
  //   handleSuccess: mockHandleSuccess,
  //   handleSuccessAsync: mockHandleSuccessAsync
  // }
  components: {
    kapPager
  }
})

wrapper.vm.$refs = {
  form: {
    clearValidate: jest.fn(),
    validate: jest.fn().mockImplementation(() => Promise.resolve(true))
  }
}

describe('Component SettingModel', () => {
  it('init', () => {
    expect(mockApi.mockLoadModelConfigList.mock.calls[0][1]).toEqual({'model_name': '', 'page_offset': 0, 'page_size': 10, 'project': 'learn_kylin'})
    expect(mockHandleSuccessAsync).toBeCalled()
    expect(wrapper.vm.$data.modelList).toEqual([{'alias': 'test0001', 'auto_merge_enabled': null, 'auto_merge_time_ranges': null, 'config_last_modified': 0, 'config_last_modifier': null, 'model': '15df9e90-7560-4cf3-817b-70b4104a4f66', 'override_props': {}, 'retention_range': null, 'volatile_range': null}])
    expect(wrapper.vm.$data.modelListSize).toBe(15)
  })
  it('computed', async () => {
    expect(wrapper.vm.emptyText).toBe('No data')
    wrapper.setData({filter: {page_offset: 0, page_size: 10, model_name: 'model'}})
    expect(wrapper.vm.emptyText).toBe('No Results')
    expect(wrapper.vm.modelTableTitle).toBe('Index Group Name')
    expect(wrapper.vm.settingOption).toEqual(['spark.executor.cores', 'spark.executor.instances', 'spark.executor.memory', 'spark.sql.shuffle.partitions'])
    // expect(wrapper.vm.modelTableTitle.call(option)).toBe()
    // expect(wrapper.vm.settingOption.call(option)).toEqual(['Auto-merge', 'Volatile Range', 'Retention Threshold', 'spark.executor.cores', 'spark.executor.instances', 'spark.executor.memory', 'spark.sql.shuffle.partitions', 'is-base-cuboid-always-valid', 'customSettings'])

    expect(wrapper.vm.availableRetentionRange).toBe('')
    await wrapper.setData({activeRow: {auto_merge_time_ranges: ['HOUR']}})
    // await wrapper.update()
    expect(wrapper.vm.availableRetentionRange).toBe('HOUR')

    expect(wrapper.vm.optionDesc).toEqual({'Auto-merge': 'The system could auto-merge segment fragments over different merging threshold. Auto-merge will optimize storage to enhance query performance.', 'Retention Threshold': 'The segments within the retention threshold would be kept. The rest would be removed automatically.', 'Volatile Range': '"Auto-Merge" will not merge the latest segments defined in "Volatile Range". The default value is 0.', 'customSettings': "Besides the defined configurations, you can also add some advanced settings.<br/><i class=\"el-icon-ksd-alert\"></i>Note: It's highly recommended to use this feature with the support of Kyligence Service Team.", 'is-base-cuboid-always-valid': 'According to your business scenario, you can decide whether to add an index that contains dimensions and measures defined in all aggregate groups. The index can answer queries across multiple aggregate groups, but this will impact query performance. In addition to this, there are some storage and building costs by adding this index.', 'kylin.engine.spark-conf.spark.executor.cores': 'The number of cores to use on each executor.', 'kylin.engine.spark-conf.spark.executor.instances': 'The number of executors to use on each application.', 'kylin.engine.spark-conf.spark.executor.memory': 'The amount of memory to use per executor process.', 'kylin.engine.spark-conf.spark.sql.shuffle.partitions': 'The number of partitions to use when shuffling data for joins or aggregations.'})

    const options = {
      activeRow: {
        auto_merge_time_ranges: ['HOUR']
      },
      step: 'stepOne'
    }
    const callback = jest.fn()
    wrapper.vm.validateSettingItem.call(options, null, 'Retention Threshold', callback)
    expect(callback).toBeCalled()

    expect(wrapper.vm.modelSettingTitle).toBe('Add Model Setting')
    wrapper.setData({isEdit: true})
    expect(wrapper.vm.modelSettingTitle).toBe('Edit Model Setting')

    expect(wrapper.vm.isSubmit).toBeFalsy()
    await wrapper.setData({modelSettingForm: {...wrapper.vm.modelSettingForm, settingItem: 'Auto-merge', autoMerge: []}})
    // await wrapper.update()
    expect(wrapper.vm.isSubmit).toBeTruthy()
    await wrapper.setData({modelSettingForm: {...wrapper.vm.modelSettingForm, settingItem: 'Volatile Range', volatileRange: {volatile_range_number: ''}}})
    // await wrapper.update()
    expect(wrapper.vm.isSubmit).toBeTruthy()
    await wrapper.setData({modelSettingForm: {...wrapper.vm.modelSettingForm, settingItem: 'Retention Threshold', volatileRange: {retention_range_number: null}}})
    // await wrapper.update()
    expect(wrapper.vm.isSubmit).toBeTruthy()
    await wrapper.setData({modelSettingForm: {...wrapper.vm.modelSettingForm, settingItem: 'spark.executor.cores'}})
    // await wrapper.update()
    expect(wrapper.vm.isSubmit).toBeTruthy()
    await wrapper.setData({modelSettingForm: {...wrapper.vm.modelSettingForm, settingItem: 'is-base-cuboid-always-valid', 'is-base-cuboid-always-valid': ''}})
    // await wrapper.update()
    expect(wrapper.vm.isSubmit).toBeTruthy()
    await wrapper.setData({modelSettingForm: {...wrapper.vm.modelSettingForm, settingItem: 'customSettings', 'customSettings': [[]]}})
    // await wrapper.update()
    expect(wrapper.vm.isSubmit).toBeTruthy()
  })
  it('methods', async () => {
    wrapper.vm.handleClosed()
    expect(wrapper.vm.$data.isEdit).toBeFalsy()
    expect(wrapper.vm.$data.activeRow).toBeNull()
    expect(wrapper.vm.$data.modelSettingForm).toEqual({'autoMerge': [], 'customSettings': [[]], 'is-base-cuboid-always-valid': 0, 'name': '', 'retentionThreshold': {'retention_range_enabled': true, 'retention_range_number': 0, 'retention_range_type': ''}, 'settingItem': '', 'spark.executor.cores': null, 'spark.executor.instances': null, 'spark.executor.memory': null, 'spark.sql.shuffle.partitions': null, 'volatileRange': {'volatile_range_enabled': true, 'volatile_range_number': 0, 'volatile_range_type': ''}})
    expect(wrapper.vm.$refs.form.clearValidate).toBeCalled()

    wrapper.vm.addSettingItem(settingValue[0])
    expect(wrapper.vm.$data.modelSettingForm.name).toBe('test0001')
    expect(wrapper.vm.$data.activeRow).toEqual(settingValue[0])
    expect(wrapper.vm.$data.step).toBe('stepOne')
    expect(wrapper.vm.$data.editModelSetting).toBeTruthy()

    await wrapper.vm.removeAutoMerge(settingValue[0], 'retention_range')
    expect(mockKapConfirm.mock.calls[0][0]).toBe('Are you sure you want to delete the \"Retention Threshold\" setting?')
    expect(mockApi.mockUpdateModelConfig.mock.calls[0][1]).toEqual({'alias': 'test0001', 'auto_merge_enabled': null, 'auto_merge_time_ranges': null, 'config_last_modified': 0, 'config_last_modifier': null, 'model': '15df9e90-7560-4cf3-817b-70b4104a4f66', 'override_props': {}, 'project': 'learn_kylin', 'retention_range': null, 'volatile_range': null})

    await wrapper.vm.removeCustomSettingItem(settingValue[0], 'kylin.config.enginee.code')
    expect(mockKapConfirm.mock.calls[1][0]).toBe('Are you sure you want to delete custom setting item kylin.config.enginee.code？')
    expect(mockApi.mockUpdateModelConfig.mock.calls[1][1]).toEqual({'alias': 'test0001', 'auto_merge_enabled': null, 'auto_merge_time_ranges': null, 'config_last_modified': 0, 'config_last_modifier': null, 'model': '15df9e90-7560-4cf3-817b-70b4104a4f66', 'override_props': {}, 'project': 'learn_kylin', 'retention_range': null, 'volatile_range': null})

    await wrapper.vm.nextStep()
    expect(wrapper.vm.$refs.form.validate).toBeCalled()
    expect(wrapper.vm.$data.step).toBe('stepTwo')

    wrapper.vm.preStep()
    expect(wrapper.vm.$data.step).toBe('stepOne')

    const data = {
      alias: 'test0001',
      auto_merge_enabled: true,
      auto_merge_time_ranges: ['DAY', 'WEEK'],
      config_last_modified: 1598390155201,
      config_last_modifier: 'ADMIN',
      model: '15df9e90-7560-4cf3-817b-70b4104a4f66',
      override_props: {
        'kylin.engine.spark-conf.spark.executor.cores': '2',
        'kylin.engine.spark-conf.spark.executor.memory': '10g',
        'kylin.cube.aggrgroup.is-base-cuboid-always-valid': 'true'
      },
      retention_range: {
        retention_range_enabled: true,
        retention_range_number: 2,
        retention_range_type: 'WEEK'
      },
      volatile_range: {
        volatile_range_enabled: true,
        volatile_range_number: 2,
        volatile_range_type: 'DAY'
      }
    }

    wrapper.vm.editMergeItem(data)
    expect(wrapper.vm.$data.modelSettingForm.settingItem).toBe('Auto-merge')
    expect(wrapper.vm.$data.isEdit).toBeTruthy()
    expect(wrapper.vm.$data.step).toBe('stepTwo')
    expect(wrapper.vm.$data.editModelSetting).toBeTruthy()

    wrapper.vm.editVolatileItem(data)
    expect(wrapper.vm.$data.modelSettingForm.settingItem).toBe('Volatile Range')
    expect(wrapper.vm.$data.isEdit).toBeTruthy()
    expect(wrapper.vm.$data.step).toBe('stepTwo')
    expect(wrapper.vm.$data.editModelSetting).toBeTruthy()

    wrapper.vm.editRetentionItem(data)
    expect(wrapper.vm.$data.modelSettingForm.settingItem).toBe('Retention Threshold')
    expect(wrapper.vm.$data.isEdit).toBeTruthy()
    expect(wrapper.vm.$data.step).toBe('stepTwo')
    expect(wrapper.vm.$data.editModelSetting).toBeTruthy()

    wrapper.vm.editSparkItem(data, 'kylin.engine.spark-conf.spark.executor.cores')
    expect(wrapper.vm.$data.modelSettingForm.settingItem).toBe('spark.executor.cores')
    expect(wrapper.vm.$data.isEdit).toBeTruthy()
    expect(wrapper.vm.$data.step).toBe('stepTwo')
    expect(wrapper.vm.$data.editModelSetting).toBeTruthy()

    wrapper.vm.editCubeItem(data, 'kylin.cube.aggrgroup.is-base-cuboid-always-valid')
    expect(wrapper.vm.$data.modelSettingForm.settingItem).toBe('is-base-cuboid-always-valid')
    expect(wrapper.vm.$data.modelSettingForm['is-base-cuboid-always-valid']).toBe('true')
    expect(wrapper.vm.$data.isEdit).toBeTruthy()
    expect(wrapper.vm.$data.step).toBe('stepTwo')
    expect(wrapper.vm.$data.editModelSetting).toBeTruthy()

    wrapper.vm.editCustomSettingItem(data, 'kylin.engine.spark-conf.spark.executor.memory')
    expect(wrapper.vm.$data.modelSettingForm.settingItem).toBe('customSettings')
    expect(wrapper.vm.$data.modelSettingForm.customSettings).toEqual([['kylin.engine.spark-conf.spark.executor.memory', '10g']])
    expect(wrapper.vm.$data.isEdit).toBeTruthy()
    expect(wrapper.vm.$data.step).toBe('stepTwo')
    expect(wrapper.vm.$data.editModelSetting).toBeTruthy()

    wrapper.vm.currentChange(1, 20)
    expect(wrapper.vm.$data.filter).toEqual({'model_name': 'model', 'page_offset': 1, 'page_size': 20})
    expect(mockApi.mockLoadModelConfigList).toBeCalled()

    wrapper.vm.searchModels()
    expect(wrapper.vm.$data.filter.page_offset).toBe(0)
    expect(mockApi.mockLoadModelConfigList).toBeCalled()

    wrapper.vm.addCustomSetting()
    expect(wrapper.vm.$data.modelSettingForm.customSettings.length).toBe(2)
    expect(wrapper.vm.$data.modelSettingForm.customSettings[0]).toEqual(['kylin.engine.spark-conf.spark.executor.memory', '10g'])

    wrapper.vm.removeCustomSetting(0)
    expect(wrapper.vm.$data.modelSettingForm.customSettings.length).toBe(1)

    const setting = {
      name: 'test0001',
      settingItem: '',
      autoMerge: ['DAY', 'WEEK'],
      volatileRange: {
        volatile_range_enabled: true,
        volatile_range_number: 2,
        volatile_range_type: 'DAY'
      },
      retentionThreshold: {
        retention_range_enabled: true,
        retention_range_number: 2,
        retention_range_type: 'WEEK'
      },
      'spark.executor.cores': 2,
      'spark.executor.instances': 10,
      'spark.executor.memory': '10g',
      'spark.sql.shuffle.partitions': 6,
      'is-base-cuboid-always-valid': 'false',
      customSettings: [['spark.sql.shuffle.partitions', 10]]
    }
    await wrapper.setData({modelSettingForm: {...setting, settingItem: 'Auto-merge'}})
    // await wrapper.update()
    await wrapper.vm.submit()
    expect(wrapper.vm.$data.activeRow).toEqual({'alias': 'test0001', 'auto_merge_enabled': true, 'auto_merge_time_ranges': ['DAY', 'WEEK'], 'config_last_modified': 1598390155201, 'config_last_modifier': 'ADMIN', 'model': '15df9e90-7560-4cf3-817b-70b4104a4f66', 'override_props': {'kylin.cube.aggrgroup.is-base-cuboid-always-valid': 'true', 'kylin.engine.spark-conf.spark.executor.cores': '2', 'kylin.engine.spark-conf.spark.executor.memory': '10g'}, 'retention_range': {'retention_range_enabled': true, 'retention_range_number': 2, 'retention_range_type': 'WEEK'}, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': 2, 'volatile_range_type': 'DAY'}})
    expect(mockApi.mockUpdateModelConfig).toBeCalled()

    await wrapper.setData({modelSettingForm: {...setting, settingItem: 'customSettings'}})
    // await wrapper.update()
    await wrapper.vm.submit()
    expect(wrapper.vm.$data.activeRow).toEqual({'alias': 'test0001', 'auto_merge_enabled': true, 'auto_merge_time_ranges': ['DAY', 'WEEK'], 'config_last_modified': 1598390155201, 'config_last_modifier': 'ADMIN', 'model': '15df9e90-7560-4cf3-817b-70b4104a4f66', 'override_props': {'kylin.cube.aggrgroup.is-base-cuboid-always-valid': 'true', 'kylin.engine.spark-conf.spark.executor.cores': '2', 'kylin.engine.spark-conf.spark.executor.memory': '10g', 'spark.sql.shuffle.partitions': 10}, 'retention_range': {'retention_range_enabled': true, 'retention_range_number': 2, 'retention_range_type': 'WEEK'}, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': 2, 'volatile_range_type': 'DAY'}})
    expect(mockApi.mockUpdateModelConfig).toBeCalled()

    await wrapper.setData({modelSettingForm: {...setting, settingItem: 'Volatile Range'}})
    // await wrapper.update()
    await wrapper.vm.submit()
    expect(wrapper.vm.$data.activeRow).toEqual({'alias': 'test0001', 'auto_merge_enabled': true, 'auto_merge_time_ranges': ['DAY', 'WEEK'], 'config_last_modified': 1598390155201, 'config_last_modifier': 'ADMIN', 'model': '15df9e90-7560-4cf3-817b-70b4104a4f66', 'override_props': {'kylin.cube.aggrgroup.is-base-cuboid-always-valid': 'true', 'kylin.engine.spark-conf.spark.executor.cores': '2', 'kylin.engine.spark-conf.spark.executor.memory': '10g', 'spark.sql.shuffle.partitions': 10}, 'retention_range': {'retention_range_enabled': true, 'retention_range_number': 2, 'retention_range_type': 'WEEK'}, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': 2, 'volatile_range_type': 'DAY'}})
    expect(mockApi.mockUpdateModelConfig).toBeCalled()

    await wrapper.setData({modelSettingForm: {...setting, settingItem: 'Retention Threshold'}})
    // await wrapper.update()
    await wrapper.vm.submit()
    expect(wrapper.vm.$data.activeRow).toEqual({'alias': 'test0001', 'auto_merge_enabled': true, 'auto_merge_time_ranges': ['DAY', 'WEEK'], 'config_last_modified': 1598390155201, 'config_last_modifier': 'ADMIN', 'model': '15df9e90-7560-4cf3-817b-70b4104a4f66', 'override_props': {'kylin.cube.aggrgroup.is-base-cuboid-always-valid': 'true', 'kylin.engine.spark-conf.spark.executor.cores': '2', 'kylin.engine.spark-conf.spark.executor.memory': '10g', 'spark.sql.shuffle.partitions': 10}, 'retention_range': {'retention_range_enabled': true, 'retention_range_number': 2, 'retention_range_type': 'WEEK'}, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': 2, 'volatile_range_type': 'DAY'}})
    expect(mockApi.mockUpdateModelConfig).toBeCalled()

    await wrapper.setData({modelSettingForm: {...setting, settingItem: 'spark.executor.memory'}})
    // await wrapper.update()
    await wrapper.vm.submit()
    expect(wrapper.vm.$data.activeRow).toEqual({'alias': 'test0001', 'auto_merge_enabled': true, 'auto_merge_time_ranges': ['DAY', 'WEEK'], 'config_last_modified': 1598390155201, 'config_last_modifier': 'ADMIN', 'model': '15df9e90-7560-4cf3-817b-70b4104a4f66', 'override_props': {'kylin.cube.aggrgroup.is-base-cuboid-always-valid': 'true', 'kylin.engine.spark-conf.spark.executor.cores': '2', 'kylin.engine.spark-conf.spark.executor.memory': '10gg', 'spark.sql.shuffle.partitions': 10}, 'retention_range': {'retention_range_enabled': true, 'retention_range_number': 2, 'retention_range_type': 'WEEK'}, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': 2, 'volatile_range_type': 'DAY'}})
    expect(mockApi.mockUpdateModelConfig).toBeCalled()

    await wrapper.setData({modelSettingForm: {...setting, settingItem: 'is-base-cuboid-always-valid'}})
    // await wrapper.update()
    await wrapper.vm.submit()
    expect(wrapper.vm.$data.activeRow).toEqual({'alias': 'test0001', 'auto_merge_enabled': true, 'auto_merge_time_ranges': ['DAY', 'WEEK'], 'config_last_modified': 1598390155201, 'config_last_modifier': 'ADMIN', 'model': '15df9e90-7560-4cf3-817b-70b4104a4f66', 'override_props': {'kylin.cube.aggrgroup.is-base-cuboid-always-valid': false, 'kylin.engine.spark-conf.spark.executor.cores': '2', 'kylin.engine.spark-conf.spark.executor.memory': '10gg', 'spark.sql.shuffle.partitions': 10}, 'retention_range': {'retention_range_enabled': true, 'retention_range_number': 2, 'retention_range_type': 'WEEK'}, 'volatile_range': {'volatile_range_enabled': true, 'volatile_range_number': 2, 'volatile_range_type': 'DAY'}})
    expect(mockApi.mockUpdateModelConfig).toBeCalled()
  })
})

