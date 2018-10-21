<template>
  <aside class="data-source-bar">
    <section class="header clearfix" v-if="isShowActionGroup">
      <div class="header-text font-medium">
        {{$t('kylinLang.common.dataSource')}}
      </div>
      <div class="header-icons">
        <i class="el-icon-ksd-add_data_source" v-if="isShowLoadSource" @click="importDataSource(sourceTypes.NEW, currentProjectData)"></i>
        <i class="el-icon-ksd-table_setting" v-if="isShowSettings" @click="importDataSource(sourceTypes.SETTING, currentProjectData)"></i>
      </div>
    </section>

    <section class="body">
      <div v-if="isShowBtnLoad" class="btn-group">
        <el-button plain size="medium" type="primary" icon="el-icon-ksd-load" @click="importDataSource(sourceTypes.NEW, currentProjectData)">
          {{$t('kylinLang.common.dataSource')}}
        </el-button>
      </div>
      <TreeList
        :data="datasources"
        :placeholder="$t('searchTable')"
        :default-expanded-keys="defaultExpandedKeys"
        :draggable-node-types="draggableNodeTypes"
        :is-expand-all="isExpandAll"
        :is-show-filter="isShowFilter"
        :is-expand-on-click-node="isExpandOnClickNode"
        :on-filter="handleFilter"
        @click="handleClick"
        @drag="handleDrag"
        @load-more="handleLoadMore">
      </TreeList>
    </section>
  </aside>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import { sourceTypes } from '../../../config'
import TreeList from '../TreeList/index.vue'
import locales from './locales'
import { getDatasourceObj, getDatabaseObj, getTableObj, getFirstTableData, getWordsData, freshTreeOrder } from './handler'
import { handleSuccessAsync } from '../../../util'

@Component({
  props: {
    projectName: {
      type: String
    },
    expandNodeTypes: {
      type: Array,
      default: () => []
    },
    searchableNodeTypes: {
      type: Array,
      default: () => []
    },
    draggableNodeTypes: {
      type: Array,
      default: () => []
    },
    clickableNodeTypes: {
      type: Array,
      default: () => ['database', 'table', 'column']
    },
    isShowActionGroup: {
      type: Boolean,
      default: true
    },
    isShowLoadSource: {
      type: Boolean,
      default: false
    },
    isShowSettings: {
      type: Boolean,
      default: false
    },
    isShowSelected: {
      type: Boolean,
      default: false
    },
    isExpandOnClickNode: {
      type: Boolean,
      default: true
    },
    isShowFilter: {
      type: Boolean,
      default: true
    },
    isExpandAll: {
      type: Boolean,
      default: false
    }
  },
  components: {
    TreeList
  },
  computed: {
    ...mapGetters([
      'isAdminRole',
      'isProjectAdmin',
      'currentProjectData'
    ])
  },
  methods: {
    ...mapActions('DataSourceModal', {
      callDataSourceModal: 'CALL_MODAL'
    }),
    ...mapActions({
      fetchDatabases: 'FETCH_DATABASES',
      fetchTables: 'FETCH_TABLES',
      updateTopTable: 'UPDATE_TOP_TABLE'
    })
  },
  locales
})
export default class DataSourceBar extends Vue {
  filterText = ''
  datasources = []
  sourceTypes = sourceTypes
  allWords = []
  defaultExpandedKeys = []
  draggableNodeKeys = []
  timer = null

  get databaseArray () {
    const allData = this.datasources.reduce((databases, datasource) => [...databases, ...datasource.children], [])
    return allData.filter(data => !['isMore', 'isLoading'].includes(data.type))
  }
  get tableArray () {
    const allData = this.databaseArray.reduce((tables, database) => [...tables, ...database.children], [])
    return allData.filter(data => !['isMore', 'isLoading'].includes(data.type))
  }
  get columnArray () {
    return this.tableArray.reduce((columns, table) => [...columns, ...table.children], [])
  }
  get currentSourceTypes () {
    const projectProperies = this.currentProjectData['override_kylin_properties']
    return projectProperies && projectProperies['kylin.source.default']
      ? [+projectProperies['kylin.source.default']]
      : []
  }
  get foreignKeys () {
    return this.tableArray.reduce((foreignKeys, table) => {
      const currentFK = table.__data.foreign_key.map(foreignKey => `${table.datasource}.${table.database}.${foreignKey}`)
      return [...foreignKeys, ...currentFK]
    }, [])
  }
  get isShowBtnLoad () {
    return (this.isAdminRole || this.isProjectAdmin) && !this.datasources.length
  }
  get primaryKeys () {
    return this.tableArray.reduce((primaryKeys, table) => {
      const currentPK = table.__data.primary_key.map(primaryKey => `${table.datasource}.${table.database}.${primaryKey}`)
      return [...primaryKeys, ...currentPK]
    }, [])
  }
  @Watch('columnArray')
  onTreeDataChange () {
    this.freshAutoCompleteWords()
    this.defaultExpandedKeys = this.allWords
      .filter(word => this.expandNodeTypes.includes(word.meta))
      .map(word => word.caption)
  }
  @Watch('projectName')
  @Watch('currentSourceTypes')
  onProjectChange (oldValue, newValue) {
    if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
      this.initTree()
    }
  }
  mounted () {
    this.initTree()
  }
  addPagination (data) {
    data.pagination.pageOffset++
  }
  clearPagination (data) {
    data.pagination.pageOffset = 0
  }
  showLoading (data) {
    data.isLoading = true
  }
  hideLoading (data) {
    data.isLoading = false
  }
  async initTree () {
    await this.loadDatasources()
    await this.loadDataBases()
    await this.loadTables({ isReset: true })
    this.selectFirstTable()
    freshTreeOrder(this)
  }
  async loadDatasources () {
    this.datasources = this.currentSourceTypes.map(sourceType => getDatasourceObj(this, sourceType))
  }
  async loadDataBases () {
    // 分数据源，请求database
    const responses = await Promise.all(this.datasources.map(({ projectName, sourceType }) => {
      return this.fetchDatabases({ projectName, sourceType })
    }))
    const results = await handleSuccessAsync(responses)
    // 组装database进datasource
    this.datasources.forEach((datasource, index) => {
      for (const resultDatabse of results[index]) {
        datasource.children.push(getDatabaseObj(this, datasource, resultDatabse))
      }
    })
  }
  async loadTables (params) {
    const { tableName = null, databaseId = null, isReset = false } = params || {}
    const currentDatabases = this.databaseArray.filter(database => {
      return database.id === databaseId || !databaseId
    })

    const responses = await Promise.all(currentDatabases.map((database) => {
      const { projectName, label: databaseName, pagination } = database
      isReset ? this.clearPagination(database) : null
      return this.fetchTables({ projectName, databaseName, tableName, isExt: true, ...pagination })
    }))
    const results = await handleSuccessAsync(responses)

    currentDatabases.forEach((database, index) => {
      const { size, tables: resultTables } = results[index]
      const tables = resultTables.map(resultTable => getTableObj(this, database, resultTable))
      database.children = !isReset ? [...database.children, ...tables] : tables
      database.isMore = size && size > this.getChildrenCount(database)
      database.isHidden = !this.getChildrenCount(database)
      this.addPagination(database)
      this.hideLoading(database)
    })
  }
  getChildrenCount (data) {
    return data.children.filter(data => !['isMore', 'isLoading'].includes(data.type)).length
  }
  handleDrag (data, node) {
    this.$emit('drag', data, node)
  }
  handleFilter (filterText) {
    clearInterval(this.timer)

    return new Promise(async resolve => {
      this.timer = setTimeout(async () => {
        const requests = this.databaseArray.map(async database => {
          const tableName = filterText
          const databaseId = database.id
          this.clearPagination(database)

          await this.loadTables({ databaseId, tableName, isReset: true })
        })
        await Promise.all(requests)
        this.filterText = filterText
        this.selectFirstTable()
        resolve()
      }, 1000)
    })
  }
  async handleLoadMore (data, node) {
    const { id: databaseId } = data.parent
    const tableName = this.filterText
    await this.loadTables({ databaseId, tableName })
  }
  handleClick (data, node) {
    if (this.clickableNodeTypes.includes(data.type)) {
      if (this.isShowSelected) {
        this.setSelectedTable(data)
      }
      this.$emit('click', data, node)
    }
  }
  async handleToggleTop (data, node, event) {
    event && event.stopPropagation()
    event && event.preventDefault()

    const { projectName } = this
    const tableFullName = `${data.database}.${data.label}`
    const isTopSet = !data.isTopSet

    await this.updateTopTable({ projectName, tableFullName, isTopSet })
    data.isTopSet = isTopSet
    freshTreeOrder(this)
  }
  setSelectedTable (data) {
    for (const table of this.tableArray) {
      table.isSelected = data.id === table.id
    }
  }
  selectFirstTable () {
    if (this.isShowSelected && this.tableArray.length) {
      this.handleClick(getFirstTableData(this.datasources))
    }
  }
  freshAutoCompleteWords () {
    const datasourceWords = this.datasources.map(datasource => getWordsData(datasource))
    const databaseWords = this.databaseArray.map(database => getWordsData(database))
    const tableWords = this.tableArray.map(table => getWordsData(table))
    const columnWords = this.columnArray.map(column => getWordsData(column))
    this.allWords = [...datasourceWords, ...databaseWords, ...tableWords, ...columnWords]
    this.$emit('autoComplete', [...databaseWords, ...tableWords, ...columnWords])
  }
  async importDataSource (sourceType, project, event) {
    event && event.stopPropagation()
    event && event.preventDefault()

    const isSubmit = await this.callDataSourceModal({ sourceType, project })
    isSubmit && this.loadTables({ isReset: true })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.data-source-bar {
  height: 100%;
  .header,
  .body {
    padding: 20px;
    width: 250px;
    box-sizing: border-box;
  }
  .header {
    font-size: 16px;
    color: #263238;
    border-bottom: 1px solid @line-split-color;
  }
  .header-text {
    float: left;
  }
  .header-icons {
    float: right;
    position: relative;
    transform: translateY(4px);
    i {
      margin-right: 4px;
    }
    i:last-child {
      margin-right: 0;
    }
  }
  .body {
    height: calc(~"100% - 63px");
    overflow: auto;
  }
  .body .btn-group {
    text-align: center;
  }
  .body .btn-group .el-button {
    width: 100%;
  }
  // datasource tree样式
  .el-tree {
    margin-bottom: 20px;
    *[draggable="true"] {
      cursor: move;
    }
    .left {
      float: left;
      margin-right: 4px;
    }
    .right {
      position: absolute;
      right: 10px;
      top: 50%;
      transform: translateY(-50%);
    }
    .tree-icon {
      margin-right: 4px;
      &:last-child {
        margin-right: 0;
      }
    }
    .tree-item {
      position: relative;
      width: calc(~'100% - 24px');
      .top {
        display: none;
        font-size: 13px;
        position: relative;
        top: -1px;
      }
      &:hover .top {
        display: inline;
      }
      .table {
        padding-right: 30px;
        line-height: 36px;
      }
      .table.has-range:hover {
        padding-right: 45px;
      }
      .column {
        padding-right: 10px;
      }
      > div {
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
      }
    }
    .datasource {
      color: #263238;
    }
    .el-tree-node .el-tree-node__content:hover > .tree-item {
      color: #087AC8;
    }
    .table-date-tip {
      color: #8E9FA8;
      &:hover {
        color: #087AC8;
      }
    }
    .table-action {
      color: #000000;
      &:hover {
        color: #087AC8;
      }
    }
    .table-tag {
      display: inline-block;
      width: 14px;
      height: 14px;
      line-height: 14px;
      margin-right: 2px;
      font-style: normal;
    }
    .column-tag {
      display: inline-block;
      font-size: 16px;
      color: #087AC8;
      margin-right: 2px;
      font-style: normal;
    }
    & > .el-tree-node {
      border: 1px solid #CFD8DC;
      overflow: hidden;
      margin-bottom: 10px;
      & > .el-tree-node__content {
        padding: 10px 9px !important; // important用来去掉el-tree的内联样式
        height: auto;
        background: #E2ECF1;
        &:hover > .tree-item > span {
          color: #263238;
        }
      }
      // datasource的样式
      & > .el-tree-node__content {
        cursor: default;
        & > .tree-item {
          width: 100%;
          i {
            cursor: pointer;
          }
          .right {
            right: 0;
          }
        }
      }
      & > .el-tree-node__content .el-tree-node__expand-icon {
        display: none;
      }
      & > .el-tree-node__children {
        margin-left: -18px;
      }
      & > .el-tree-node__children > .el-tree-node {
        border-top: 1px solid #CFD8DC;
      }
    }
  }
}
</style>
