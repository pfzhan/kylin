<template>
  <div class="studio-source">
    <div class="table-layout clearfix"> 
      <!-- 数据源导航栏 -->
      <DataSourceBar
        :ignore-node-types="['column']"
        v-guide.datasourceTree
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
            <div class="table-actions ky-no-br-space">
              <el-button size="small" @click="sampleTable">{{$t('sample')}}</el-button>
              <el-button size="small" :loading="reloadBtnLoading" plain @click="handleReload">{{$t('reload')}}</el-button>
              <el-button size="small" @click="handleDelete" plain>{{$t('delete')}}</el-button>
            </div>
          </div>
          <!-- Source Table详细信息 -->
          <el-tabs class="table-details" type="card" v-model="viewType">
            <el-tab-pane :label="$t('general')" :name="viewTypes.DATA_LOAD" v-if="isAutoProject">
              <TableDataLoad :project="currentProjectData" :table="selectedTable" @fresh-tables="handleFreshTable"></TableDataLoad>
            </el-tab-pane>
            <el-tab-pane :label="$t('columns')" :name="viewTypes.COLUMNS">
              <TableColumns :table="selectedTable"></TableColumns>
            </el-tab-pane>
            <el-tab-pane :label="$t('sampling')" :name="viewTypes.SAMPLING">
              <TableSamples :table="selectedTable"></TableSamples>
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
      <el-dialog
        class="sample-dialog"
        @close="resetSampling"
        :title="$t('sampleDialogTitle')" width="480px" :visible.sync="sampleVisible" :close-on-press-escape="false" :close-on-click-modal="false">
        <div class="sample-desc">{{sampleDesc}}</div>
        <div class="sample-desc">
          {{$t('sampleDesc1')}}<el-input size="small" class="ksd-mrl-5" style="width: 110px;" :class="{'is-error': errorMsg}" v-number="samplingRows" v-model="samplingRows" @input="handleSamplingRows"></el-input>{{$t('sampleDesc2')}}
          <div class="error-msg" v-if="errorMsg">{{errorMsg}}</div>
        </div>
        <span slot="footer" class="dialog-footer ky-no-br-space">
          <el-button @click="cancelSample" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" plain @click="submitSample" size="medium" :disabled="!!errorMsg" :loading="sampleLoading">{{$t('kylinLang.common.submit')}}</el-button>
        </span>
      </el-dialog>
    </div>
    <ReloadTable></ReloadTable>
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
import TableSamples from './TableSamples/TableSamples.vue'
import SourceManagement from './SourceManagement/SourceManagement.vue'
import ReloadTable from './TableReload/reload.vue'
import EmptyData from '../../common/EmptyData/EmptyData.vue'
import { handleSuccessAsync, handleError } from '../../../util'
import { getFormattedTable } from '../../../util/UtilTable'

@Component({
  components: {
    EmptyData,
    DataSourceBar,
    TableDataLoad,
    TableColumns,
    TableSamples,
    SourceManagement,
    ReloadTable
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
      deleteTable: 'DELETE_TABLE',
      submitSampling: 'SUBMIT_SAMPLING',
      getReloadInfluence: 'GET_RELOAD_INFLUENCE'
    }),
    ...mapActions('ReloadTableModal', {
      callReloadModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class StudioSource extends Vue {
  selectedTableData = null
  viewType = viewTypes.DATA_LOAD
  viewTypes = viewTypes
  isShowSourcePage = false
  reloadBtnLoading = false
  sampleVisible = false
  sampleLoading = false
  samplingRows = 20000000
  errorMsg = ''
  get selectedTable () {
    return this.selectedTableData ? getFormattedTable(this.selectedTableData) : null
  }
  get sampleDesc () {
    return this.$t('sampleDesc', {tableName: this.selectedTable && this.selectedTable.fullName})
  }
  handleShowSourcePage (value) {
    this.isShowSourcePage = value
  }
  handleSamplingRows (samplingRows) {
    if (samplingRows < 10000) {
      this.errorMsg = this.$t('minNumber')
    } else if (samplingRows > 20000000) {
      this.errorMsg = this.$t('maxNumber')
    } else {
      this.errorMsg = ''
    }
  }
  resetSampling () {
    this.samplingRows = 20000000
    this.sampleLoading = false
    this.errorMsg = ''
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
    const tableName = this.selectedTable.name
    const storageSize = Vue.filter('dataSize')(modelSize)
    const contentVal = { tableName, storageSize }
    const confirmTitle = this.$t('unloadTableTitle')
    const confirmMessage1 = modelSize ? this.$t('affactUnloadInfo', contentVal) : ''
    const confirmMessage2 = this.$t('unloadTable', contentVal)
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
  sampleTable () {
    this.sampleVisible = true
  }
  cancelSample () {
    this.sampleVisible = false
  }
  async submitSample () {
    this.sampleLoading = true
    try {
      await this.submitSampling({ project: this.currentSelectedProject, qualifiedTableName: this.selectedTable.fullName, rows: this.samplingRows })
      this.$message({ type: 'success', message: this.$t('kylinLang.common.submitSuccess') })
      this.sampleVisible = false
      this.sampleLoading = false
    } catch (e) {
      handleError(e)
      this.sampleVisible = false
      this.sampleLoading = false
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
  async handleReload () {
    try {
      const projectName = this.currentSelectedProject
      const databaseName = this.selectedTable.database
      const tableName = this.selectedTable.name
      let fullTableName = databaseName + '.' + tableName
      this.reloadBtnLoading = true
      const res = await this.getReloadInfluence({
        project: projectName,
        table: fullTableName
      })
      this.reloadBtnLoading = false
      const influenceDetail = await handleSuccessAsync(res)
      const isSubmit = await this.callReloadModal({
        checkData: influenceDetail,
        tableName: fullTableName
      })
      if (isSubmit) {
        this.handleFreshTable()
      }
    } catch (e) {
      this.reloadBtnLoading = false
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
    if (this.isAutoProject) {
      this._showDataRangeConfrim()
    }
  }
  _showDataRangeConfrim () {
    const confirmTitle = this.$t('remindLoadRangeTitle')
    const confirmMessage = this.$t('remindLoadRange')
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const confirmButtonClass = 'guideTipSetPartitionConfitmBtn'
    const type = 'warning'
    return this.$alert(confirmMessage, confirmTitle, { confirmButtonText, type, confirmButtonClass })
  }
  async _getAffectedModelCountAndSize () {
    const projectName = this.currentSelectedProject
    const tableName = this.selectedTable.fullName
    const isIncrement = this.selectedTable.__data.increment_loading
    const response = await this.fetchChangeTypeInfo({ projectName, tableName, isSelectFact: isIncrement })
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
    font-size: 14px;
    color: #263238;
    margin-bottom: 5px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .table-details {
    padding-bottom: 20px;
  }
  .table-update-at {
    font-size: 12px;
    color: @text-disabled-color;
    font-weight: normal;
  }
  .table-header {
    padding-right: 300px;
    position: relative;
    margin-bottom: 15px;
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
  .slide-enter-active, .slide-leave-active {
    transition: transform .5s;
    transform: translateX(0);
  }
  .slide-enter, .slide-leave-to {
    transform: translateX(-100%);
  }
}
.sample-dialog {
  .error-msg {
    color: @color-danger;
    font-size: 12px;
  }
  .is-error .el-input__inner{
    border-color: @color-danger;
  }
}
</style>
