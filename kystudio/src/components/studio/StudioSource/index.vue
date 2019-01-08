<template>
  <div class="studio-source">
    <div class="table-layout clearfix"> 
      <!-- 数据源导航栏 -->
      <DataSourceBar
        class="layout-left"
        ref="datasource-bar"
        :project-name="currentSelectedProject"
        :is-show-load-source="true"
        :is-show-settings="false"
        :is-show-selected="true"
        :is-expand-on-click-node="false"
        :is-first-select="true"
        :is-show-source-switch="true"
        :expand-node-types="['datasource', 'database']"
        :searchable-node-types="['table', 'column']"
        @click="handleClick"
        @show-source="handleShowSourcePage"
        @tables-loaded="handleTablesLoaded">
      </DataSourceBar>
      <!-- Source Table展示 -->
      <div class="layout-right">
        <template v-if="selectedTable">
          <!-- Source Table标题信息 -->
          <div class="table-header">
            <h1 class="table-name" :title="selectedTable.fullName">{{selectedTable.fullName}}</h1>
            <h2 class="table-update-at">{{$t('updateAt')}} {{selectedTable.updateAt | timestamp2GmtDate}}</h2>
            <div class="table-actions">
              <el-button size="small" @click="handleDelete">{{$t('delete')}}</el-button>
            </div>
          </div>
          <!-- Source Table详细信息 -->
          <el-tabs class="table-details" v-model="viewType">
            <el-tab-pane :label="$t('general')" :name="viewTypes.DATA_LOAD" v-if="isAutoProject">
              <TableDataLoad :project="currentProjectData" :table="selectedTable" @fresh-tables="handleFreshTable"></TableDataLoad>
            </el-tab-pane>
            <el-tab-pane :label="$t('columns')" :name="viewTypes.COLUMNS">
              <TableColumns :table="selectedTable"></TableColumns>
            </el-tab-pane>
          </el-tabs>
        </template>
        <!-- Table空页 -->
        <div class="empty-page" v-if="!selectedTable">
          <EmptyData />
        </div>
        <transition name="slide">
          <SourceManagement v-if="isShowSourcePage" :project="currentProjectData" @fresh-tables="handleFreshTable"></SourceManagement>
        </transition>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { viewTypes } from './handler'
import DataSourceBar from '../../common/DataSourceBar/index.vue'
import TableDataLoad from './TableDataLoad/TableDataLoad.vue'
import TableColumns from './TableColumns/TableColumns.vue'
import SourceManagement from './SourceManagement/SourceManagement.vue'
import EmptyData from '../../common/EmptyData/EmptyData.vue'
import { handleSuccessAsync, handleError } from '../../../util'
import { getFormattedTable } from '../../../util/UtilTable'

@Component({
  components: {
    EmptyData,
    DataSourceBar,
    TableDataLoad,
    TableColumns,
    SourceManagement
  },
  computed: {
    ...mapGetters([
      'isAutoProject',
      'currentProjectData',
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      fetchTables: 'FETCH_TABLES',
      importTable: 'LOAD_HIVE_IN_PROJECT',
      fetchChangeTypeInfo: 'FETCH_CHANGE_TYPE_INFO',
      deleteTable: 'DELETE_TABLE'
    })
  },
  locales
})
export default class StudioSource extends Vue {
  selectedTableData = null
  viewType = viewTypes.DATA_LOAD
  viewTypes = viewTypes
  isShowSourcePage = false
  get selectedTable () {
    return this.selectedTableData ? getFormattedTable(this.selectedTableData) : null
  }
  handleShowSourcePage (value) {
    this.isShowSourcePage = value
  }
  async handleClick (data = {}) {
    if (data.type !== 'table') return

    try {
      const tableName = data.label
      const databaseName = data.database
      this.handleShowSourcePage(false)
      await this.fetchTableDetail({ tableName, databaseName })
    } catch (e) {
      handleError(e)
    }
  }
  showDeleteTableConfirm (modelCount, modelSize) {
    const storageSize = Vue.filter('dataSize')(modelSize)
    const contentVal = { modelCount, storageSize }
    const confirmTitle = this.$t('kylinLang.common.notice')
    const confirmMessage1 = modelCount || modelSize ? this.$t('affactUnloadInfo', contentVal) : ''
    const confirmMessage2 = this.$t('unloadTable')
    const confirmMessage = _render(this.$createElement)
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const confirmParams = { confirmButtonText, cancelButtonText, type: 'warning' }
    return this.$confirm(confirmMessage, confirmTitle, confirmParams)

    function _render (h) {
      return (
        <div>
          <p class="break-all">{confirmMessage1}</p>
          <p>{confirmMessage2}</p>
        </div>
      )
    }
  }
  async handleDelete () {
    try {
      const projectName = this.currentSelectedProject
      const databaseName = this.selectedTable.database
      const tableName = this.selectedTable.name
      const { modelCount, modelSize } = await this._getAffectedModelCountAndSize()

      await this.showDeleteTableConfirm(modelCount, modelSize)
      await this.deleteTable({ projectName, databaseName, tableName })

      this.$message({ type: 'success', message: this.$t('unloadSuccess') })
      await this.handleFreshTable({ isSetToDefault: true })
    } catch (e) {
      handleError(e)
    }
  }
  async handleFreshTable (options = {}) {
    try {
      const { isSetToDefault } = options
      const tableName = this.selectedTable.name
      const databaseName = this.selectedTable.database
      await this.$refs['datasource-bar'].loadTables({ isReset: true })

      isSetToDefault
        ? this.$refs['datasource-bar'].selectFirstTable()
        : await this.fetchTableDetail({ tableName, databaseName })
    } catch (e) {
      handleError(e)
    }
  }
  async fetchTableDetail ({ tableName, databaseName }) {
    try {
      const projectName = this.currentSelectedProject
      const res = await this.fetchTables({ projectName, databaseName, tableName, isExt: true, isFuzzy: false })
      const tableDetail = await handleSuccessAsync(res)

      this.selectedTableData = tableDetail.tables[0]
    } catch (e) {
      handleError(e)
    }
  }
  handleTablesLoaded () {
    this._showDataRangeConfrim()
  }
  _showDataRangeConfrim () {
    const confirmTitle = this.$t('kylinLang.common.notice')
    const confirmMessage = this.$t('remindLoadRange')
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const type = 'warning'
    return this.$alert(confirmMessage, confirmTitle, { confirmButtonText, type })
  }
  async _getAffectedModelCountAndSize () {
    const projectName = this.currentSelectedProject
    const tableName = this.selectedTable.fullName
    const isSelectFact = !this.selectedTable.__data.increment_loading
    const response = await this.fetchChangeTypeInfo({ projectName, tableName, isSelectFact })
    const result = await handleSuccessAsync(response)
    return { modelCount: result.models.length, modelSize: result.byte_size }
  }
  mounted () {
    if (!this.isAutoProject) {
      this.viewType = viewTypes.COLUMNS
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.studio-source {
  height: 100%;
  background: white;
  .layout-right {
    padding: 20px 20px 0 20px;
    min-height: 100%;
    box-sizing: border-box;
    position: relative;
  }
  .table-name {
    font-size: 16px;
    color: #263238;
    margin-bottom: 15px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .table-details {
    padding-bottom: 20px;
  }
  .table-update-at {
    font-size: 12px;
    color: #8E9FA8;
    font-weight: normal;
  }
  .table-header {
    padding-right: 300px;
    position: relative;
    margin-bottom: 12px;
  }
  .table-actions {
    position: absolute;
    top: 50%;
    right: 0;
    transform: translateY(-14px);
  }
  .el-tabs__nav {
    margin-left: 0;
  }
  .el-tabs__item:not(.is-active) {
    font-size: 14px;
    color: #455A64;
  }
  .empty-page {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
  }
  .center {
    text-align: center;
    &:first-child {
      margin-bottom: 20px;
    }
  }
  .el-tabs__header {
    margin-bottom: 10px;
  }
  .el-tabs__content {
    padding: 20px 0;
  }
  .slide-enter-active, .slide-leave-active {
    transition: transform .5s;
    transform: translateX(0);
  }
  .slide-enter, .slide-leave-to {
    transform: translateX(-100%);
  }
}
</style>
