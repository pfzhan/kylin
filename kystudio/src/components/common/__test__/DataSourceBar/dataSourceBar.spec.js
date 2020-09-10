import { shallow } from 'vue-test-utils'
import { localVue } from '../../../../../test/common/spec_common'
import Vuex from 'vuex'
import * as utils from '../../../../util'
import * as business from '../../../../util/business'
import * as handlers from '../../DataSourceBar/handler'
import DataSourceBar from '../../DataSourceBar/index.vue'
import TreeList from '../../TreeList'

// jest.setTimeout(50000)

// localVue.use()
const fetchDBandTables = jest.fn().mockResolvedValue({code: '000', data: {databases: [{dbname: 'DB1', size: 3, tables: [{ name: 'TABLE1_NEW', database: 'DB1', foreign_key: [], primary_key: [], columns: [] }]}]}, msg: ''})
const fetchDatabases = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    resolve(['DB1', 'SSB'])
  })
})
const updateTopTable = jest.fn().mockImplementation()
// const freshTreeOrder = jest.spyOn(handlers, 'freshTreeOrder').mockImplementation()

const handleSuccessAsync = jest.spyOn(utils, 'handleSuccessAsync').mockImplementation((res, callback) => {
  return new Promise((resolve) => (resolve(res.data || res)))
})
const mockKapConfirm = jest.fn().mockImplementation(() => {
  return new Promise((resolve, reject) => {
    reject(new Error(''))
  })
})
const handleError = jest.spyOn(business, 'handleError').mockImplementation(() => {})
const mockCacheDatasource = jest.fn()
const treeListEmit = jest.fn()
// const getDatasourceObj = jest.spyOn(dataSourceHandler, 'getDatasourceObj')

const types = {
  FETCH_DB_AND_TABLES: 'FETCH_DB_AND_TABLES',
  FETCH_DATABASES: 'FETCH_DATABASES',
  UPDATE_TOP_TABLE: 'UPDATE_TOP_TABLE',
  CACHE_DATASOURCE: 'CACHE_DATASOURCE',
  CALL_MODAL: 'CALL_MODAL'
}
const DataSourceModal = {
  namespaced: true,
  state: {},
  actions: {
    [types.CALL_MODAL]: jest.fn()
  }
}
const DetailDialogModal = {
  namespaced: true,
  state: {}
}
const store = new Vuex.Store({
  state: {
    config: {
      platform: 'pc'
    },
    capacity: {
      maintenance_mode: true
    },
    project: {
      projectDefaultDB: 'default'
    }
  },
  getters: {
    currentProjectData () {
      return {
        override_kylin_properties: {
          'kylin.source.default': '9'
        }
      }
    },
    isAdminRole: () => (false),
    isProjectAdmin: () => (true)
  },
  actions: {
    [types.FETCH_DB_AND_TABLES]: fetchDBandTables,
    [types.FETCH_DATABASES]: fetchDatabases,
    [types.UPDATE_TOP_TABLE]: updateTopTable
  },
  mutations: {
    [types.CACHE_DATASOURCE]: mockCacheDatasource
  },
  modules: {
    DataSourceModal,
    DetailDialogModal
  }
})

const wrapper = shallow(DataSourceBar, {
  localVue,
  store,
  mocks: {
    handleSuccessAsync: handleSuccessAsync,
    handleError: handleError,
    // freshTreeOrder: freshTreeOrder,
    kapConfirm: mockKapConfirm
  },
  propsData: {
    projectName: 'abc'
    // ignoreNodeTypes: ['column']
  },
  components: {
    TreeList

  }
})

wrapper.vm.$refs = {
  treeList: {
    $emit: treeListEmit
  }
}

describe('Component DataSourceBar', async () => {
  it('init', () => {
    wrapper.vm.$emit('filter', {target: {value: 'name'}})
    expect(wrapper.emitted().filter[0]).toEqual([{'target': {'value': 'name'}}])
    expect(wrapper.vm.$data.datasources[0].id).toEqual(9)
    expect(wrapper.vm.$data.isLoadingTreeData).toBe(false)
    expect(fetchDBandTables).toBeCalled()
    expect(handleSuccessAsync).toBeCalled()
    expect(wrapper.vm.$data.databaseSizeObj).toEqual({'DB1': 3})
    expect(wrapper.vm.$data.treeKey).toContain('pageTree')
    expect(wrapper.find('.header').exists()).toBeTruthy()
    // expect(wrapper.vm.$data.isLoadingTreeData).toBeFalsy()
  })
  it('computed events', async () => {
    expect(wrapper.vm.emptyText).toEqual('No data')
    wrapper.setData({ filterText: 'data' })
    await wrapper.update()
    expect(wrapper.vm.emptyText).toEqual('No Results')
    expect(wrapper.vm.showAddDatasourceBtn).toBeFalsy()
    wrapper.vm.$store.state.config.platform = 'iframe'
    await wrapper.update()
    expect(wrapper.vm.showAddDatasourceBtn).toBeFalsy()

    expect(wrapper.vm.showTreeFilter).toBeTruthy()
    wrapper.setData({ filterText: '' })
    await wrapper.update()
    expect(wrapper.vm.showTreeFilter).toBeTruthy()
    expect(wrapper.vm.dataSourceStyle).toEqual({})
    wrapper.setProps({isShowFilter: false})
    await wrapper.update()
    expect(wrapper.vm.showTreeFilter).toBeFalsy()
    wrapper.setProps({ isShowDragWidthBar: true })
    await wrapper.update()
    expect(wrapper.vm.dataSourceStyle).toEqual({'width': '250px'})
    expect(wrapper.vm.databaseArray[0].type).toBe('database')
    expect(wrapper.vm.tableArray).toBeInstanceOf(Object)
    expect(wrapper.vm.columnArray).toEqual([])
    expect(wrapper.vm.ignoreColumnTree).toBeFalsy()
    expect(wrapper.vm.isShowBtnLoad).toBeFalsy()
  })
  it('methods events', async (done) => {
    wrapper.vm.freshDatasourceTitle()
    expect(wrapper.vm.$data.datasources[0].label).toEqual('Source : Object Storage')
    wrapper.vm.$store.state.config.platform = 'iframe'
    await wrapper.update()
    expect(wrapper.vm.$data.datasources[0].label).toEqual('Source : Object Storage')
    const pagination = {
      page_offset: 0,
      isLoading: false
    }
    wrapper.vm.addPagination({ pagination })
    expect(pagination.page_offset).toBe(1)
    wrapper.vm.clearPagination({ pagination })
    expect(pagination.page_offset).toBe(0)
    wrapper.vm.showLoading(pagination)
    expect(pagination.isLoading).toBeTruthy()
    wrapper.vm.hideLoading(pagination)
    expect(pagination.isLoading).toBeFalsy()
    const that = {
      tableArray: [{ label: 'hive', isSelected: false }],
      dataSourceSelectedLabel: 'hive'
    }
    DataSourceBar.options.methods.recoverySelectedTable.call(that)
    expect(that.tableArray).toEqual([{'isSelected': true, 'label': 'hive'}])
    that.dataSourceSelectedLabel = 'greenplum'
    DataSourceBar.options.methods.recoverySelectedTable.call(that)
    expect(that.tableArray).toEqual([{'isSelected': true, 'label': 'hive'}])

    const list = {children: [{type: 'column'}]}
    const len = DataSourceBar.options.methods.getChildrenCount(list)
    expect(len).toBe(1)

    wrapper.vm.handleDrag('item', 'nodes')
    expect(wrapper.emitted().drag).toEqual([['item', 'nodes']])

    wrapper.setData({ filterText: 'content' })
    await wrapper.update()
    wrapper.vm.reloadTables()
    expect(wrapper.vm.$data.filterText).toBe('content')

    wrapper.vm.handleClick({type: 'datasource'}, 'nodes')
    expect(wrapper.emitted().click).toBe()
    wrapper.setProps({isShowSelected: true})
    await wrapper.update()
    wrapper.vm.handleClick({type: 'table'}, 'nodes')
    expect(wrapper.vm.$data.isSwitchSource).toBeFalsy()
    expect(wrapper.emitted().click[0]).toEqual([{'type': 'table'}, 'nodes'])
    wrapper.setProps({isShowSelected: false})
    await wrapper.update()
    wrapper.vm.handleClick({type: 'column'}, 'nodes')
    expect(wrapper.emitted().click[1]).toEqual([{'type': 'column'}, 'nodes'])

    const _data = {database: 'ssb', label: '01', isTopSet: false}
    wrapper.vm.handleToggleTop(_data)
    expect(_data.isTopSet).toBeFalsy()

    wrapper.vm.handleSwitchSource()
    expect(wrapper.vm.$data.isSwitchSource).toBeTruthy()
    expect(wrapper.emitted()['show-source'][0]).toEqual([true])

    const _options = {
      tableArray: [{ id: 1, label: 'DB1', isSelected: false }],
      isShowSelected: true,
      dataSourceSelectedLabel: 'DB1',
      datasources: [],
      handleClick: jest.fn()
    }
    DataSourceBar.options.methods.resetSourceTableSelect.call(_options, false)
    expect(_options.tableArray).toEqual([{'id': 1, 'isSelected': true, 'label': 'DB1'}])
    DataSourceBar.options.methods.resetSourceTableSelect.call(_options, true)
    expect(_options.tableArray).toEqual([{'id': 1, 'isSelected': false, 'label': 'DB1'}])

    _options.dataSourceSelectedLabel = 'ssb'
    DataSourceBar.options.methods.setSelectedTable.call(_options, {id: 1})
    expect(_options.tableArray).toEqual([{'id': 1, 'isSelected': true, 'label': 'DB1'}])
    expect(_options.dataSourceSelectedLabel).toBe('DB1')

    expect(wrapper.vm.selectFirstTable()).toBeNull()
    expect(DataSourceBar.options.methods.selectFirstTable.call(_options)).toBeTruthy()
    expect(_options.handleClick).toBeCalled()

    wrapper.vm.freshAutoCompleteWords()
    expect(wrapper.emitted()['autoComplete'][0]).toEqual([[]])
    expect(wrapper.vm.$data.allWords).toBeInstanceOf(Array)
    // expect(freshTreeOrder).toBeCalled()

    const _options1 = {
      ...wrapper,
      callDataSourceModal: jest.fn().mockImplementation(() => {
        return new Promise((resolve) => {
          resolve({loaded: ['SSB', 'DB1'], failed: ['DEFAULT.PART']})
        })
      }),
      callGlobalDetailDialog: jest.fn().mockReturnValue(),
      $t: jest.fn(),
      $message: jest.fn(),
      handleResultModalClosed: jest.fn()
    }
    DataSourceBar.options.methods.toImportDataSource.call(_options1, 'add', 'abc')
    setTimeout(() => {
      expect(_options1.callGlobalDetailDialog).toHaveBeenCalledWith({'details': [{'list': ['DEFAULT.PART'], 'title': 'undefined (1)'}], 'msg': undefined, 'needCallbackWhenClose': true, 'showCopyBtn': true, 'theme': 'plain-mult'})
      done()
    })

    _options1.callDataSourceModal = jest.fn().mockImplementation(() => {
      return new Promise(resolve => resolve({loaded: ['SSB', 'DB1'], failed: []}))
    })
    DataSourceBar.options.methods.toImportDataSource.call(_options1, 'add', 'abc')
    setTimeout(() => {
      expect(_options1.$message).toBeCalled()
      done()
    })

    _options1.callDataSourceModal = jest.fn().mockImplementation(() => {
      return new Promise(resolve => resolve({loaded: ['SSB', 'DB1'], failed: ['PART']}))
    })
    DataSourceBar.options.methods.toImportDataSource.call(_options1, 'add', 'abc')
    setTimeout(() => {
      expect(_options1.callGlobalDetailDialog).toBeCalled()
      done()
    }, 1200)

    wrapper.vm.importDataSource()
    expect(wrapper.find('.el-message-box')).not.toBeUndefined()
    wrapper.vm.$store.state.capacity.maintenance_mode = false
    await wrapper.update()
    wrapper.vm.importDataSource()
    expect(wrapper.findAll('.el-message-box').length).toBe(0)

    wrapper.vm.handleLoadMore({parent: {label: 'SSB', id: 1000}})
    expect(handleSuccessAsync).toBeCalled()
    wrapper.setData({ filterText: 's' })
    await wrapper.update()
    wrapper.vm.handleLoadMore({parent: {label: 'SSB', id: 1000}})
    expect(handleSuccessAsync).toBeCalled()
    // wrapper.destroy()

    wrapper.vm.refreshTables()
    expect(fetchDBandTables).toBeCalled()

    wrapper.vm.cacheDatasourceInStore()
    expect(mockCacheDatasource).toBeCalled()

    wrapper.vm.loadTables({ tableName: 'table', databaseId: 1, isReset: false })
    expect(handleSuccessAsync).toBeCalled()

    wrapper.vm.loadDataBases()
    expect(wrapper.vm.$data.datasources.length).toBe(1)
  })

  it('close modal', (done) => {
    const _options2 = {
      loadDataBases: jest.fn().mockResolvedValue(),
      reloadTables: jest.fn().mockResolvedValue(),
      loadedTables: ['SSB'],
      failedTables: [],
      selectFirstTable: jest.fn(),
      $emit: jest.fn()
    }
    DataSourceBar.options.methods.handleResultModalClosed.call(_options2)
    setTimeout(() => {
      expect(_options2.loadedTables).toEqual(['SSB'])
      expect(_options2.failedTables).toEqual([])
      expect(_options2.loadDataBases).toBeCalled()
      done()
    }, 200)
    _options2.loadDataBases = jest.fn().mockRejectedValue()
    DataSourceBar.options.methods.handleResultModalClosed.call(_options2)
    setTimeout(() => {
      expect(handleError).toBeCalled()
      done()
    }, 200)
  })
})

describe('DataSourceBar handlers', () => {
  it('events', () => {
    expect(handlers.getTableObj(wrapper.vm, 'SSB', {root_fact: true, lookup: false, uuid: '', name: 'KYLIN', increment_loading: false, top: true, columns: [], segment_range: {date_range_start: 103113938123, date_range_end: 103123938123}}, false)).toBeInstanceOf(Object)
    let data = {
      datasources: [{
        children: [
          { label: 'DEFAULT', children: [{isTopSet: true, isCentral: true, label: 'prices'}, {isTopSet: false, isCentral: false, label: 'prices1'}] },
          { label: 'SSB', children: [{isTopSet: false, isCentral: true, label: 'date'}, {isTopSet: false, isCentral: false, label: 'date1'}] },
          { label: 'TEST', children: [{isTopSet: true, isCentral: false, label: 'comment'}, {isTopSet: true, isCentral: false, label: 'comment1'}] },
          { label: 'TEST_1', children: [{isTopSet: true, isCentral: true, label: 'CC'}] }
        ]
      }],
      $store: {
        state: {
          project: {
            projectDefaultDB: 'DEFAULT'
          }
        }
      }
    }
    handlers.freshTreeOrder(data)
    expect(data.datasources).toBeInstanceOf(Array)
    expect(handlers.getWordsData({type: '', label: '', id: 4})).toEqual({meta: '', caption: '', value: '', id: 4, scope: 1})
    expect(handlers.getTableDBWordsData({type: 'table', database: 'SSB', label: 'SALES', id: 4})).toEqual({meta: 'table', caption: 'SSB.SALES', value: 'SSB.SALES', id: 4, scope: 1})
    expect(handlers.getFirstTableData([{children: [{ children: [{dbname: 'DB1', size: 3, tables: []}] }]}])).toEqual({"dbname": "DB1", "size": 3, "tables": []})
    // {label: 'P_LINEORDER', database: 'SSB', datasource: 9, columns: [{cardinality: null, datatype: "varchar(4096)", id: "2", max_value: null, min_value: null, name: "D_DATE", null_count: null}]}
    expect(handlers.getTableObj({hideFactIcon: false, foreignKeys: [], primaryKeys: []}, { datasource: 9, label: 'P_LINEORDER' }, { root_fact: false, lookup: true, columns: [{cardinality: null, datatype: "varchar(4096)", id: "2", max_value: null, min_value: null, name: "D_DATE", null_count: null}] }, false).__data).toBeTruthy()
    wrapper.vm.$store.state.config.platform = 'iframe'
    expect(handlers.getDatasourceObj(wrapper.vm, 9)).toBeInstanceOf(Object)
    expect(handlers.render.column.render((res) => res, { node: null, data: {label: 'TEST', tags: ['FK', 'PK']}, store: {}})).toEqual('div')
    expect(handlers.render.table.render.call(wrapper.vm, wrapper.vm.$createElement, { node: null, data: {label: 'TEST', tags: ['F', 'L', 'N'], dateRange: true, isTopSet: true, isHideFactIcon: false}, store: {}})).toBeInstanceOf(Object)
    expect(handlers.render.database.render(wrapper.vm.$createElement, { node: null, data: { label: 'TEST', isDefaultDB: true }, store: {} })).toBeInstanceOf(Object)
    expect(handlers.render.datasource.render.call(wrapper.vm, wrapper.vm.$createElement, { node: null, data: {sourceType: 9, label: 'SSB'}, store: {} })).toBeInstanceOf(Object)
  })
})
