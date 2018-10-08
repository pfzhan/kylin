<template>
  <div class="source-hive clearfix">
    <TreeList
      class="table-tree"
      :data="treeData"
      :is-show-filter="true"
      :is-show-resize-bar="true"
      :on-filter="handleFilter"
      @resize="handleResize"
      @click="handleClickNode"
      @node-expand="handleNodeExpand"
      @load-more="handleLoadMore"
    />
    <div class="content" :style="contentStyle">
      <arealabel
        splitChar=","
        placeholder=" "
        :validateRegex="tableRegex"
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
import { handleSuccessAsync } from '../../../../util'
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
  tableRegex = /^\s*;?(\w+\.\w+)\s*(,\s*\w+\.\w+)*;?\s*$/
  timer = null
  get selectedTableOptions () {
    return this.selectedTables.map(tableId => ({
      label: tableId,
      value: tableId
    }))
  }
  @Watch('selectedTables')
  onSelectedTablesChange () {
    for (const database of this.databases) {
      for (const table of database.children) {
        table.isSelected = this.selectedTables.includes(table.id)
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
  async mounted () {
    await this.loadDatabase()
  }
  async loadDatabase () {
    const projectName = this.currentSelectedProject
    const sourceType = this.sourceType
    const res = await this.fetchDatabase({ projectName, sourceType })
    this.databases = getDatabaseTree(await handleSuccessAsync(res))
    this.treeData = [{
      id: sourceType === sourceTypes['HIVE'] ? 'Hive Table' : 'Table',
      label: sourceType === sourceTypes['HIVE'] ? 'Hive Table' : 'Table',
      type: 'text',
      children: this.databases
    }]
  }
  async loadTables ({database, tableName = '', isTableReset = false}) {
    const projectName = this.currentSelectedProject
    const sourceType = this.sourceType
    const databaseName = database.id
    const pagination = database.pagination
    const res = await this.fetchTables({ projectName, sourceType, databaseName, tableName, ...pagination })
    // hard code size
    const data = { size: 7, tables: await handleSuccessAsync(res) }
    getTableTree(database, data, isTableReset)
    this.setNextPagination(pagination)
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

        this.onSelectedTablesChange()
        resolve()
      }, 1000)
    })
  }
  handleClickNode (data) {
    if (data.type === 'table') {
      const isSelected = this.selectedTables.includes(data.id)
      if (isSelected) {
        this.handleRemoveTable(data.id)
      } else {
        this.handleAddTable(data.id)
      }
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
  handleLoadMore (data) {
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
