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
        :expand-node-types="['datasource', 'database']"
        :searchable-node-types="['table', 'column']"
        @click="handleClick">
      </DataSourceBar>
      <!-- Source Table展示 -->
      <div class="layout-right">
        <template v-if="selectedTable">
          <!-- Source Table标题信息 -->
          <div class="table-header">
            <h1 class="table-name">{{selectedTable.database}}.{{selectedTable.name}}</h1>
            <h2 class="table-update-at">{{$t('updateAt')}} {{updateAt}}</h2>
            <div class="table-actions">
              <el-button size="small" icon="el-icon-ksd-table_resure" @click="handleReload">{{$t('reload')}}</el-button>
              <!-- <el-button size="small" icon="el-icon-ksd-download" @click="handleSampling">{{$t('sampling')}}</el-button> -->
              <el-button size="small" icon="el-icon-ksd-download" @click="handleUnload">{{$t('unload')}}</el-button>
            </div>
          </div>
          <!-- Source Table详细信息 -->
          <div class="table-details">
            <el-tabs v-model="viewType">
              <el-tab-pane :label="$t('dataLoad')" :name="viewTypes.DATA_LOAD">
                <TableDataLoad :project-name="currentSelectedProject" :table="selectedTable" @fresh-tables="handleFreshTable"></TableDataLoad>
              </el-tab-pane>
              <el-tab-pane :label="$t('columns')" :name="viewTypes.COLUMNS">
                <TableColumns :table="selectedTable"></TableColumns>
              </el-tab-pane>
              <!-- <el-tab-pane :label="$t('sample')" :name="viewTypes.SAMPLE">
                <TableSamples :table="selectedTable"></TableSamples>
              </el-tab-pane> -->
              <el-tab-pane :label="$t('statistics')" :name="viewTypes.STATISTICS" v-if="isShowStatistic">
                <TableStatistics :table="selectedTable"></TableStatistics>
              </el-tab-pane>
              <el-tab-pane :label="$t('extendInformation')" :name="viewTypes.EXTEND_INFORMATION">
                <TableExtInfo :table="selectedTable"></TableExtInfo>
              </el-tab-pane>
              <el-tab-pane :label="$t('kylinLang.dataSource.kafkaCluster')" :name="viewTypes.KAFKA" v-if="isShowKafka">
                <el-button type="primary" plain size="medium" icon="edit" @click="handleKafkaClick" class="ksd-fright">{{$t('kylinLang.common.edit')}}</el-button>
                <ViewKafka ref="addkafkaForm" @validSuccess="kafkaValidSuccess" :streamingData="currentStreamingTableData" :tableName="currentStreamingTable" ></ViewKafka>
              </el-tab-pane>
              <!-- <el-tab-pane :label="$t('access')" :name="viewTypes.ACCESS">
                <Access></Access>
              </el-tab-pane> -->
            </el-tabs>
          </div>
        </template>
        <!-- Table空页 -->
        <div class="empty-page" v-if="!selectedTable">
          <el-row class="center"><img :src="emptyImg" /></el-row>
          <el-row class="center">{{$t('emptyTable')}}</el-row>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions, mapMutations } from 'vuex'
import { Component } from 'vue-property-decorator'

import emptyImg from './empty.svg'
import locales from './locales'
import { viewTypes, getSelectedTableDetail } from './handler'
import DataSourceBar from '../../common/DataSourceBar/index.vue'
import TableDataLoad from './TableDataLoad/TableDataLoad.vue'
import TableColumns from './TableColumns/index.vue'
import TableSamples from './TableSamples/index.vue'
import TableStatistics from './TableStatistics/index.vue'
import TableExtInfo from './TableExtInfo/index.vue'
import ViewKafka from '../../kafka/view_kafka.vue'
import Access from '../../datasource/access.vue'
import { sourceTypes } from '../../../config'
import { handleSuccessAsync, transToGmtTime } from '../../../util'

@Component({
  components: {
    DataSourceBar,
    TableDataLoad,
    TableColumns,
    TableSamples,
    TableStatistics,
    TableExtInfo,
    ViewKafka,
    Access
  },
  computed: {
    ...mapGetters([
      'currentProjectData',
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapMutations({
      setCurrentTableData: 'SET_CURRENT_TABLE'
    }),
    ...mapActions({
      fetchTables: 'FETCH_TABLES',
      importTable: 'LOAD_HIVE_IN_PROJECT',
      deleteTable: 'DELETE_TABLE'
    })
  },
  locales
})
export default class StudioSource extends Vue {
  viewType = ''
  selectedTable = null
  tableDetail = null

  viewTypes = viewTypes
  emptyImg = emptyImg

  get isShowStatistic () {
    const { source_type: sourceType } = this.selectedTable
    return sourceType === 0 || sourceType === 8 || sourceType === 16
  }
  get isShowKafka () {
    return this.selectedTable.source_type === 1
  }
  get updateAt () {
    return transToGmtTime(this.selectedTable.last_modified, this)
  }

  async mounted () {
    this.viewType = viewTypes.DATA_LOAD
  }
  handleClick (data) {
    this.fetchTableDetail(data)
  }
  handleKafkaClick () {
    const { currentProjectData: project } = this
    this.callDataSourceModal({ sourceType: sourceTypes.KAFKA, project })
  }
  handleReload () {
    this.$confirm(this.$t('reloadTable'), this.$t('kylinLang.common.notice'), {
      confirmButtonText: this.$t('kylinLang.common.ok'),
      cancelButtonText: this.$t('kylinLang.common.cancel'),
      type: 'warning'
    }).then(() => {
      const databaseName = this.selectedTable.database
      const tableName = this.selectedTable.name

      const projectName = this.currentSelectedProject
      const sourceType = this.selectedTable.source_type
      const tableFullName = `${databaseName}.${tableName}`
      return this.importTable({ projectName, sourceType, tableNames: [tableFullName] })
    }).then(() => {
      const databaseName = this.selectedTable.database
      const tableName = this.selectedTable.name

      this.$message({
        type: 'success',
        message: this.$t('reloadSuccess')
      })
      return this.fetchTableDetail({ label: tableName, database: databaseName, type: 'table' })
    }).catch(() => {})
  }
  handleSampling () {
    this.$refs['SampleModal'].showModal()
  }
  handleUnload () {
    this.$confirm(this.$t('unloadTable'), this.$t('kylinLang.common.notice'), {
      confirmButtonText: this.$t('kylinLang.common.ok'),
      cancelButtonText: this.$t('kylinLang.common.cancel'),
      type: 'warning'
    }).then(() => {
      const projectName = this.currentSelectedProject
      const databaseName = this.selectedTable.database
      const tableName = this.selectedTable.name
      return this.deleteTable({ projectName, databaseName, tableName })
    }).then(() => {
      this.$message({
        type: 'success',
        message: this.$t('unloadSuccess')
      })
      return this.handleFreshTable()
    }).catch(() => {})
  }
  async handleFreshTable () {
    await this.$refs['datasource-bar'].loadTables({ isReset: true })

    const { name, database } = this.selectedTable
    await this.fetchTableDetail({ label: name, database, type: 'table' })
  }
  async fetchTableDetail (data) {
    if (data.type === 'table') {
      const projectName = this.currentSelectedProject
      const tableName = data.label
      const databaseName = data.database
      const res = await this.fetchTables({ projectName, databaseName, tableName, isExt: true, isFuzzy: true })
      const tableDetail = await handleSuccessAsync(res)

      this.selectedTable = getSelectedTableDetail(tableDetail.tables[0])
      this.setCurrentTableData({ tableData: this.selectedTable })
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
  .el-tabs__content {
    overflow: visible;
  }
}
</style>
