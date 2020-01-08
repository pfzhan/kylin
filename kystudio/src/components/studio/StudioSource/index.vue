<template>
  <div class="studio-source">
    <div class="table-layout clearfix">
      <!-- 数据源导航栏 -->
      <div class="layout-left">
        <DataSourceBar
          :ignore-node-types="['column']"
          v-guide.datasourceTree
          ref="datasource-bar"
          :project-name="currentSelectedProject"
          :is-show-load-source="true"
          :is-show-load-table="datasourceActions.includes('loadSource')"
          :is-show-settings="false"
          :is-show-selected="true"
          :is-expand-on-click-node="false"
          :is-first-select="true"
          :is-show-source-switch="datasourceActions.includes('sourceManagement')"
          :is-show-drag-width-bar="true"
          :expand-node-types="['datasource', 'database']"
          :searchable-node-types="['table', 'column']"
          @click="handleClick"
          @show-source="handleShowSourcePage"
          @tables-loaded="handleTablesLoaded">
        </DataSourceBar>
      </div>

      <!-- Source Table展示 -->
      <div class="layout-right">
        <template v-if="selectedTable">
          <!-- Source Table标题信息 -->
          <div class="table-header">
            <h1 class="table-name" :title="selectedTable.fullName">{{selectedTable.fullName}}</h1>
            <h2 class="table-update-at">{{$t('updateAt')}} {{selectedTable.updateAt | toGMTDate}}</h2>
            <div class="table-actions ky-no-br-space">
              <el-button size="small" @click="sampleTable" v-if="datasourceActions.includes('sampleSourceTable')">{{$t('sample')}}</el-button>
              <el-button size="small" :loading="reloadBtnLoading" plain @click="handleReload" v-if="datasourceActions.includes('reloadSourceTable')">{{$t('reload')}}</el-button>
              <el-button size="small" :loading="delBtnLoading" v-if="datasourceActions.includes('delSourceTable')" @click="handleDelete" plain>{{$t('delete')}}</el-button>
            </div>
          </div>
          <!-- Source Table详细信息 -->
          <el-tabs class="table-details" type="card" v-model="viewType">
            <el-tab-pane :label="$t('general')" :name="viewTypes.DATA_LOAD" v-if="isAutoProject">
              <TableDataLoad :project="currentProjectData" :table="selectedTable" :isShowLoadData="datasourceActions.includes('loadData')" @fresh-tables="handleFreshTable"></TableDataLoad>
            </el-tab-pane>
            <el-tab-pane :label="$t('columns')" :name="viewTypes.COLUMNS" >
              <TableColumns :table="selectedTable" v-if="viewType === viewTypes.COLUMNS"></TableColumns>
            </el-tab-pane>
            <el-tab-pane :label="$t('sampling')" :name="viewTypes.SAMPLE">
              <TableSamples :table="selectedTable" v-if="viewType === viewTypes.SAMPLE"></TableSamples>
            </el-tab-pane>
          </el-tabs>
        </template>
        <!-- Table空页 -->
        <div class="empty-page" v-if="!selectedTable">
          <kap-empty-data />
        </div>
        <transition name="slide">
          <SourceManagement v-if="isShowSourcePage" :project="currentProjectData" @fresh-tables="handleFreshTable"></SourceManagement>
        </transition>
      </div>
      <el-dialog
        class="sample-dialog"
        @close="resetSampling"
        limited-area
        :title="$t('sampleDialogTitle')" width="480px" :visible.sync="sampleVisible" :close-on-press-escape="false" :close-on-click-modal="false">
        <div class="sample-desc">{{sampleDesc}}</div>
        <div class="sample-desc" style="margin-top: 3px;">
          {{$t('sampleDesc1')}}<el-input size="small" class="ksd-mrl-5" style="width: 110px;" :class="{'is-error': errorMsg}" v-number="samplingRows" v-model="samplingRows" @input="handleSamplingRows"></el-input>{{$t('sampleDesc2')}}
          <div class="error-msg" v-if="errorMsg">{{errorMsg}}</div>
        </div>
        <span slot="footer" class="dialog-footer ky-no-br-space">
          <el-button plain @click="cancelSample" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button @click="submitSample" size="medium" :disabled="!!errorMsg" :loading="sampleLoading">{{$t('kylinLang.common.submit')}}</el-button>
        </span>
      </el-dialog>

      <el-dialog
        :visible.sync="isDelAllDepVisible"
        width="720px"
        :close-on-press-escape="false"
        :close-on-click-modal="false">
        <span slot="title">{{$t('unloadTableTitle')}}</span>
        <el-alert :show-background="false" :closable="false" show-icon type="warning" style="padding:0">
          <span slot="title" class="ksd-fs-14" v-html="delTabelConfirmMessage"></span>
        </el-alert>
        <span slot="footer" class="dialog-footer ky-no-br-space">
          <el-button plain size="medium" @click="isDelAllDepVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button plain size="medium" :loading="delAllLoading" @click="handelDeleteTable(true)">{{$t('deleteAll')}}</el-button>
          <el-button size="medium" :loading="delLoading" @click="handelDeleteTable(false)">{{$t('deleteTable')}}</el-button>
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
import { handleSuccessAsync, handleError } from '../../../util'
import { kapConfirm } from '../../../util/business'
import { getFormattedTable } from '../../../util/UtilTable'

@Component({
  components: {
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
      'currentSelectedProject',
      'datasourceActions'
    ])
  },
  methods: {
    ...mapActions({
      fetchTables: 'FETCH_TABLES',
      importTable: 'LOAD_HIVE_IN_PROJECT',
      prepareUnload: 'PREPARE_UNLOAD',
      deleteTable: 'DELETE_TABLE',
      submitSampling: 'SUBMIT_SAMPLING',
      hasSamplingJob: 'HAS_SAMPLING_JOB',
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
  delBtnLoading = false
  samplingRows = 20000000
  errorMsg = ''
  isDelAllDepVisible = false
  delTabelConfirmMessage = ''
  delLoading = false
  delAllLoading = false
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
    if (samplingRows && samplingRows < 10000) {
      this.errorMsg = this.$t('minNumber')
    } else if (samplingRows && samplingRows > 20000000) {
      this.errorMsg = this.$t('maxNumber')
    } else if (!samplingRows) {
      this.errorMsg = this.$t('invalidType')
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
  showDeleteTableConfirm (hasModel, hasJob) {
    const tableName = this.selectedTable.name
    const contentVal = { tableName }
    const confirmTitle = this.$t('unloadTableTitle')
    const confirmMessage1 = this.$t('affactUnloadInfo', contentVal)
    const confirmMessage2 = this.$t('unloadTable', contentVal)
    let confirmMessage = ''
    if (hasJob && !hasModel) {
      confirmMessage = confirmMessage1
    } else if (!hasJob && !hasModel) {
      confirmMessage = confirmMessage2
    }
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const confirmParams = { confirmButtonText, cancelButtonText, type: 'warning' }
    return this.$confirm(confirmMessage, confirmTitle, confirmParams)
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
      const res = await this.hasSamplingJob({ project: this.currentSelectedProject, qualified_table_name: this.selectedTable.fullName })
      const isHasSamplingJob = await handleSuccessAsync(res)
      if (!isHasSamplingJob) {
        this.toSubmitSample()
      } else {
        this.sampleVisible = false
        this.sampleLoading = false
        kapConfirm(this.$t('confirmSampling', {table_name: this.selectedTable.fullName})).then(() => {
          this.toSubmitSample()
        })
      }
    } catch (e) {
      handleError(e)
      this.sampleVisible = false
      this.sampleLoading = false
    }
  }
  async toSubmitSample () {
    try {
      const res = await this.submitSampling({ project: this.currentSelectedProject, qualified_table_name: this.selectedTable.fullName, rows: this.samplingRows })
      await handleSuccessAsync(res)
      this.$message({ type: 'success', message: this.$t('samplingTableJobBeginTips', {tableName: this.selectedTable.fullName}) })
      this.sampleVisible = false
      this.sampleLoading = false
    } catch (e) {
      handleError(e)
      this.sampleVisible = false
      this.sampleLoading = false
    }
  }
  async handleDelete () {
    const projectName = this.currentSelectedProject
    const databaseName = this.selectedTable.database
    const tableName = this.selectedTable.name
    const { hasModel, hasJob, modelSize } = await this._getAffectedModelCountAndSize()
    if (!hasModel) {
      this.delBtnLoading = true
      try {
        await this.showDeleteTableConfirm(hasModel, hasJob)
        await this.deleteTable({ projectName, databaseName, tableName })
        this.$message({ type: 'success', message: this.$t('unloadSuccess') })
        await this.handleFreshTable({ isSetToDefault: true })
        this.delBtnLoading = false
      } catch (e) {
        this.delBtnLoading = false
        handleError(e)
      }
    } else {
      const storageSize = Vue.filter('dataSize')(modelSize)
      const contentVal = { tableName, storageSize }
      this.delTabelConfirmMessage = this.$t('dropTabelDepen', contentVal)
      this.isDelAllDepVisible = true
    }
  }
  async handelDeleteTable (cascade) {
    if (cascade) {
      this.delAllLoading = true
    } else {
      this.delLoading = true
    }
    try {
      const projectName = this.currentSelectedProject
      const databaseName = this.selectedTable.database
      const tableName = this.selectedTable.name
      await this.deleteTable({ projectName, databaseName, tableName, cascade })
      this.$message({ type: 'success', message: this.$t('unloadSuccess') })
      await this.handleFreshTable({ isSetToDefault: true })
      this.isDelAllDepVisible = false
      this.delAllLoading = false
      this.delLoading = false
    } catch (e) {
      this.isDelAllDepVisible = false
      this.delAllLoading = false
      this.delLoading = false
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
        this.fetchTableDetail({ tableName, databaseName })
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
      let isHaveFirstTable = true
      await this.$refs['datasource-bar'].reloadTables()
      isSetToDefault
        ? isHaveFirstTable = this.$refs['datasource-bar'].selectFirstTable()
        : await this.fetchTableDetail({ tableName, databaseName })
      if (!isHaveFirstTable) {
        this.selectedTableData = null
      }
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
    const databaseName = this.selectedTable.database
    const tableName = this.selectedTable.name
    const response = await this.prepareUnload({projectName, databaseName, tableName})
    const result = await handleSuccessAsync(response)
    return { hasModel: result.has_model, hasJob: result.has_job, modelSize: result.storage_size }
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
  .layout-left {
    z-index:8;
  }
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
    .el-tabs__content {
      min-height: calc(~'100vh - 196px');
    }
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
  .sample-desc {
    color: @text-normal-color;
    word-break: break-word;
  }
  .error-msg {
    color: @color-danger;
    font-size: 12px;
  }
  .is-error .el-input__inner{
    border-color: @color-danger;
  }
}
</style>
