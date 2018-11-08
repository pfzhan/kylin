<template>
  <div class="source-hive clearfix">
    <TreeList
      class="table-tree"
      :data="treeData"
      :is-show-filter="true"
      :is-show-resize-bar="true"
      :on-filter="handleFilter"
      :filter-white-list-types="['datasource', 'database']"
      @resize="handleResize"
      @click="handleClickNode"
      @node-expand="handleNodeExpand"
      @load-more="handleLoadMore"
    />
    <div class="content" :style="contentStyle">
      <arealabel
        splitChar=","
        placeholder=" "
        :selectedlabels="selectedTables"
        :allowcreate="true"
        :datamap="{label: 'label', value: 'value'}"
        @refreshData="handleAddTable"
        @removeTag="handleRemoveTable"
        @validateFail="handleValidateFail">
      </arealabel>
      <div class="tips" v-html="sourceType === sourceTypes['HIVE'] ? $t('loadHiveTip') : $t('loadTip')"></div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import TreeList from '../../TreeList'
import { sourceTypes } from '../../../../config'
import { getDatabaseTree, getTableTree } from './handler'
import { handleSuccessAsync, handleError } from '../../../../util'
import arealabel from '../../area_label.vue'

@Component({
  props: {
    selectedTables: {
      default: () => []
    },
    sourceType: Number
  },
  components: {
    TreeList,
    arealabel
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'selectedProjectDatasource'
    ])
  },
  methods: {
    ...mapActions({
      fetchDatabase: 'LOAD_HIVEBASIC_DATABASE',
      fetchTables: 'LOAD_HIVE_TABLES'
    })
  },
  locales
})
export default class SourceHive extends Vue {
  treeData = []
  databases = []
  contentStyle = {
    marginLeft: null,
    width: null
  }
  sourceTypes = sourceTypes
  timer = null
  isDatabaseError = false
  get selectedTableOptions () {
    return this.selectedTables.map(tableId => ({
      label: tableId,
      value: tableId
    }))
  }
  @Watch('selectedTables')
  onSelectedTablesChange () {
    // 刷新table或者db的选中状态
    for (const database of this.databases) {
      database.isSelected = this.selectedTables.includes(database.id)
      if (database.isSelected) {
        for (const table of database.children) {
          table.isSelected = true
          table.clickable = false
        }
      } else {
        for (const table of database.children) {
          if (!table.isLoaded) {
            table.isSelected = this.selectedTables.includes(table.id)
            table.clickable = true
          }
        }
      }
    }
  }
  setNextPagination (pagination) {
    pagination.pageOffset++
  }
  clearPagination (pagination) {
    pagination.pageOffset = 0
  }
  hideNodeLoading (data) {
    data.isLoading = false
  }
  constructor () {
    super()
    this.getDatabaseTree = getDatabaseTree.bind(this)
  }
  async mounted () {
    const sourceType = this.sourceType
    this.treeData = [{
      id: sourceType === sourceTypes['HIVE'] ? 'Hive Table' : 'Table',
      label: sourceType === sourceTypes['HIVE'] ? 'Hive Table' : 'Table',
      type: 'datasource',
      children: []
    }]
    await this.loadDatabase()
  }
  async loadDatabase () {
    try {
      const projectName = this.currentSelectedProject
      const sourceType = this.sourceType
      const res = await this.fetchDatabase({ projectName, sourceType })
      this.databases = this.getDatabaseTree(await handleSuccessAsync(res))
      this.treeData[0].children = this.databases
      this.isDatabaseError = false
    } catch (e) {
      this.isDatabaseError = true
      handleError(e)
      console.log(e)
    }
  }
  async loadTables ({database, tableName = '', isTableReset = false}) {
    const projectName = this.currentSelectedProject
    const sourceType = this.sourceType
    const databaseName = database.id
    const pagination = database.pagination
    const response = await this.fetchTables({ projectName, sourceType, databaseName, tableName, ...pagination })
    const { size, tables } = await handleSuccessAsync(response)
    getTableTree(database, { size, tables }, isTableReset)
    this.setNextPagination(pagination)
    this.$emit('input', { selectedTables: [...this.selectedTables] })
  }
  handleFilter (tableName) {
    clearInterval(this.timer)

    return new Promise(async resolve => {
      this.timer = setTimeout(async () => {
        const requests = this.databases.map(async database => {
          const { pagination } = database
          this.clearPagination(pagination)

          await this.loadTables({ database, tableName, isTableReset: true })
        })
        await Promise.all(requests)
        this.treeData = [...this.treeData]
        this.onSelectedTablesChange()
        resolve()
      }, 1000)
    })
  }
  async handleClickNode (data, node, event, isSelectDatabase = false) {
    if ((data.type === 'table' && data.clickable || isSelectDatabase)) {
      this.selectedTables.includes(data.id)
        ? this.handleRemoveTable(data.id)
        : this.handleAddTable(data.id)
      if (isSelectDatabase) {
        event.preventDefault()
        event.stopPropagation()
      }
    }
    if (data.type === 'datasource' && this.isDatabaseError) {
      await this.loadDatabase()
    }
  }
  handleResize (treeWidth) {
    this.contentStyle.marginLeft = `${treeWidth}px`
    this.contentStyle.width = `${this.$el.clientWidth - treeWidth}px`
  }
  async handleNodeExpand (data) {
    if (data.isLoading) {
      if (data.type === 'database') {
        await this.loadTables({ database: data })
      }
      this.hideNodeLoading(data)
    }
  }
  async handleLoadMore (data) {
    const database = this.databases.find(database => database.id === data.parent.id)
    this.loadTables({ database })
  }
  handleAddTable (addTableId) {
    const selectedTables = addTableId instanceof Array ? addTableId : [...this.selectedTables, addTableId]
    this.$emit('input', { selectedTables })
  }
  handleRemoveTable (removeTableId) {
    const selectedTables = this.selectedTables.filter(tableId => tableId !== removeTableId)
    this.$emit('input', { selectedTables })
  }
  handleValidateFail () {
    this.$message(this.$t('selectedHiveValidateFailText'))
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.source-hive {
  .table-tree {
    width: 218px;
    float: left;
    border-right: 1px solid #cfd8dc;
    .el-tree-node__content {
      position: relative;
      &:hover {
        .select-all {
          display: block;
        }
      }
    }
    .select-all {
      display: none;
      position: absolute;
      top: 0;
      right: 10px;
      line-height: 36px;
      font-size: 12px;
      &:hover {
        color: #0988de;
      }
    }
  }
  .filter-tree {
    min-height: 220px;
    max-height: 400px;
    overflow: auto;
  }
  .content {
    margin-left: 218px;
    padding: 20px;
    box-sizing: border-box;
  }
  .filter-box {
    padding: 10px 10px 0 10px;
    box-sizing: border-box;
  }
  .tips {
    margin-top: 22px;
  }
}
</style>
