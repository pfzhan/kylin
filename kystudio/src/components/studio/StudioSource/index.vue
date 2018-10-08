<template>
  <div class="studio-source">
    <div class="table-layout clearfix"> 
      <!-- 数据源导航栏 -->
      <DataSourceBar
        class="layout-left"
        :is-show-load-source="true"
        :is-show-settings="false"
        :is-expand-on-click-node="false"
        :expand-node-types="['datasource', 'database']"
        :searchable-node-types="['table', 'column']"
        :datasource="datasource"
        @click="handleClick"
        @source-update="handleSourceUpdate"
        @click-more="handleClickMore">
      </DataSourceBar>
      <!-- Source Table展示 -->
      <div class="layout-right">
        <template v-if="selectedTable">
          <!-- Source Table标题信息 -->
          <div class="table-header">
            <h1 class="table-name">{{selectedTable.database}}.{{selectedTable.name}}</h1>
            <h2 class="table-update-at">{{$t('updateAt')}} {{updateAt}}</h2>
            <div class="table-actions">
              <el-button size="small" icon="el-icon-ksd-table_assign" type="primary" @click="handleChangeType">{{$t('changeType')}}</el-button>
              <el-button size="small" icon="el-icon-ksd-table_resure" @click="handleReload">{{$t('reload')}}</el-button>
              <!-- <el-button size="small" icon="el-icon-ksd-download" @click="handleSampling">{{$t('sampling')}}</el-button> -->
              <el-button size="small" icon="el-icon-ksd-download" @click="handleUnload">{{$t('unload')}}</el-button>
            </div>
          </div>
          <!-- Source Table详细信息 -->
          <div class="table-details">
            <el-tabs v-model="viewType">
              <el-tab-pane :label="$t('dataLoad')" :name="viewTypes.DATA_LOAD">
                <TableDataLoad :project="currentProjectData" :table="selectedTable" @on-data-range-change="handleFreshTable"></TableDataLoad>
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
          <!-- Source Table动作弹框 -->
          <CentralSettingModal ref="CentralSettingModal" :table="selectedTable" @submit="handleFreshTable" />
          <ReloadModal ref="ReloadModal" :table="selectedTable" />
          <SampleModal ref="SampleModal" :table="selectedTable" />
        </template>
        <!-- Table空页 -->
        <template v-if="!selectedTable">

        </template>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions, mapMutations } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { viewTypes, getSelectedTableDetail } from './handler'
import DataSourceBar from '../../common/DataSourceBar/index.vue'
import TableDataLoad from './TableDataLoad/index.vue'
import TableColumns from './TableColumns/index.vue'
import TableSamples from './TableSamples/index.vue'
import TableStatistics from './TableStatistics/index.vue'
import TableExtInfo from './TableExtInfo/index.vue'
import ReloadModal from './ReloadModal/index.vue'
import SampleModal from './SampleModal/index.vue'
import CentralSettingModal from './CentralSettingModal/index.vue'
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
    ReloadModal,
    SampleModal,
    CentralSettingModal,
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
      fetchDatabases: 'LOAD_DATABASE',
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      loadTableExt: 'LOAD_DATASOURCE_EXT'
    })
  },
  locales
})
export default class StudioSource extends Vue {
  viewType = ''
  datasource = []
  selectedTable = null
  tableDetail = null

  viewTypes = viewTypes

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
    await this.handleSourceUpdate()
    this.viewType = viewTypes.DATA_LOAD

    if (this.datasource[0]) {
      await this.fetchTableDetail({ label: this.datasource[0].name, type: 'table' })
    }
    // const resp = await this.fetchDatabases({projectName: this.currentSelectedProject})
    // console.log(resp)
  }
  handleClick (data) {
    this.fetchTableDetail(data)
  }
  handleKafkaClick () {
    const { currentProjectData: project } = this
    this.callDataSourceModal({ sourceType: sourceTypes.KAFKA, project })
  }
  handleClickMore (data, node) {
    console.log(data, node)
  }
  handleReload () {
    this.$refs['ReloadModal'].showModal()
  }
  handleSampling () {
    this.$refs['SampleModal'].showModal()
  }
  handleUnload () {
  }
  handleChangeType () {
    this.$refs['CentralSettingModal'].showModal()
  }
  async handleSourceUpdate () {
    const res = await this.loadDataSourceByProject({project: this.currentSelectedProject, isExt: true})
    this.datasource = await handleSuccessAsync(res)
  }
  async handleFreshTable () {
    await this.handleSourceUpdate()
    await this.fetchTableDetail({ label: this.selectedTable.name, type: 'table' })
  }
  async fetchTableDetail (data) {
    const { label: selectedTableName, type } = data

    if (type === 'table') {
      const project = this.currentSelectedProject
      const tableInfo = this.datasource.find(table => table.name === selectedTableName)
      const tableName = `${tableInfo.database}.${tableInfo.name}`
      const res = await this.loadTableExt({tableName, project})
      const tableDetail = await handleSuccessAsync(res)

      this.selectedTable = getSelectedTableDetail(tableInfo, tableDetail[0])
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
}
</style>
