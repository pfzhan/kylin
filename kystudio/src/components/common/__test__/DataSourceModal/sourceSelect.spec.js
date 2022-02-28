import { shallowMount } from '@vue/test-utils'
import Vuex from 'vuex'
import { localVue } from '../../../../../test/common/spec_common'
import SourceSelect from '../../DataSourceModal/SourceSelect/SourceSelect'
import DataSourceModalStore from '../../DataSourceModal/store'
import { states } from './mock'

const mockSourceTypes = {
  HIVE: 9,
  RDBMS: 16,
  KAFKA: 1,
  RDBMS2: 8,
  CSV: 13
}
const dataSourceModal = {
  ...DataSourceModalStore,
  ...{
    state: states
  }
}
const store = new Vuex.Store({
  state: {
    config: {
      platform: 'iframe'
    },
    datasource: {
      dataSource: {
        test_shardby: []
      }
    },
    project: {
      allProject: [{
        create_time: 1629282052044,
        create_time_utc: 1629282052044,
        default_database: 'TPCH_FLAT_ORC_100',
        description: null,
        keytab: null,
        last_modified: 1629441584322,
        maintain_model_type: 'MANUAL_MAINTAIN',
        mvcc: 11,
        name: 'test_shardby',
        override_kylin_properties: {'kylin.metadata.semi-automatic-mode': 'true', 'kylin.query.metadata.expose-computed-column': 'true', 'kylin.source.default': '9'},
        owner: 'ADMIN',
        permission: 'ADMINISTRATION',
        principal: null,
        segment_config: {
          auto_merge_enabled: false,
          auto_merge_time_ranges: ['WEEK', 'MONTH', 'QUARTER', 'YEAR'],
          volatile_range: {
            volatile_range_enabled: false,
            volatile_range_number: 0,
            volatile_range_type: 'DAY'
          },
          retention_range: {
            etention_range_enabled: false,
            retention_range_number: 1,
            retention_range_type: 'MONTH'
          },
          create_empty_segment_enabled: false},
        status: 'ENABLED',
        uuid: '1364b43c-4ad1-4d79-9c2b-625f3c24b15b',
        version: '4.0.0.0'
      }],
      selected_project: 'test_shardby'
    }
  },
  modules: {
    DataSourceModal: dataSourceModal
  }
})
const wrapper = shallowMount(SourceSelect, {
  store,
  localVue,
  propsData: {
    sourceType: 9
  },
  mocks: {
    sourceTypes: mockSourceTypes
  }
})

describe('Component SourceSelect', () => {
  it('init', () => {
    expect(wrapper.vm.sourceType).toBe(9)
  })
  it('methods', () => {
    expect(wrapper.vm.getSourceClass([9])).toEqual({active: true, 'is-disabled': false})
    wrapper.vm.clickHandler(9)
    expect(wrapper.emitted()).toEqual({'input': [[9]]})

    wrapper.vm.clickHandler()
    expect(wrapper.emitted().input[1][0]).toBe('')

    expect(wrapper.vm.disabledSelectDataSource([9])).toBeFalsy()
    expect(wrapper.vm.disabledSelectDataSource([8])).toBeFalsy()
    expect(wrapper.vm.disabledSelectDataSource()).toBeFalsy()
  })
  it('computed', async () => {
    wrapper.vm.$store.state.datasource.dataSource = {
      test_shardby: [
        {
          database: 'SSB',
          columns: [],
          table: 'SSB.CUSTOMER',
          source_type: 9
        },
        {
          database: 'KAFKA',
          columns: [],
          table: 'KAFKA.LINEORDER',
          source_type: 1
        }
      ]
    }
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.isOpenJDBCSource).toBe(false)
    expect(wrapper.vm.disabledSelectDataSource([9])).toBeFalsy()
    expect(wrapper.vm.disabledSelectDataSource([8])).toBeTruthy()

    wrapper.vm.$store.state.datasource.dataSource = {
      test_shardby: [{
        database: 'SSB',
        columns: [],
        table: 'SSB.CUSTOMER',
        source_type: 8
      }]
    }
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.disabledSelectDataSource([1])).toBeTruthy()

    expect(wrapper.vm.getSourceClass()).toEqual({'active': false, 'is-disabled': false})

    wrapper.vm.$store.state.project.selected_project = 'project'
    wrapper.vm.$store.state.project.allProject.splice(1, 0, {
      name: 'project',
      override_kylin_properties: {'kylin.source.jdbc.source.enable': 'true', 'kylin.source.jdbc.source.name': 'JDBC'}
    })
    await wrapper.vm.$nextTick()
    expect(wrapper.vm.showJDBCEnter).toBeTruthy()
  })
})
