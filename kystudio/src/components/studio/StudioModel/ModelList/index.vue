<template>
  <div class="mode-list" :class="{'full-cell': showFull}">
    <div class="ksd-title-label ksd-mt-20" v-if="!isAutoProject">{{$t('kylinLang.model.modelList')}}</div>
    <div class="ksd-title-label ksd-mt-20" v-else>{{$t('kylinLang.model.indexGroup')}}</div>
    <div>
      <div  class="ksd-mtb-10 ksd-fright">
        <el-input :placeholder="isAutoProject ? $t('kylinLang.common.pleaseFilterByIndexGroupName') : $t('kylinLang.common.pleaseFilterByModelName')" style="width:200px" size="medium" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'" v-model="filterArgs.model_name" v-global-key-event.enter.debounce="searchModels" @clear="searchModels()" class="show-search-btn" >
        </el-input>
      </div>
      <div class="ky-no-br-space model-list-header">
        <el-button v-guide.addModelBtn icon="el-icon-ksd-add_2" type="primary" size="medium" plain class="ksd-mtb-10" id="addModel" v-if="datasourceActions.includes('modelActions')" @click="showAddModelDialog">
          <span>{{$t('kylinLang.common.model')}}</span>
        </el-button>
        <el-button type="primary" icon="el-icon-ksd-add_2" v-if="$store.state.project.isSemiAutomatic&&datasourceActions.includes('modelActions')" size="medium" plain class="ksd-mtb-10 ksd-ml-10" @click="showGenerateModelDialog">
          <span>{{$t('kylinLang.model.generateModel')}}</span>
        </el-button>
      </div>
      <div class="filter-tags" v-show="filterTags.length">
        <div class="filter-tags-layout"><el-tag size="small" closable v-for="(item, index) in filterTags" :key="index" @close="handleClose(item)">{{`${$t(item.source)}：${$t(item.label)}`}}</el-tag></div>
        <span class="clear-all-filters" @click="clearAllTags">{{$t('clearAll')}}</span>
      </div>
      <el-table class="model_list_table"
        v-guide.scrollModelTable
        v-scroll-shadow
        :data="modelArray"
        border
        :empty-text="emptyText"
        tooltip-effect="dark"
        :expand-row-keys="expandedRows"
        :row-key="renderRowKey"
        :row-class-name="setRowClass"
        @expand-change="expandRow"
        :default-sort = "{prop: 'gmtTime', order: 'descending'}"
        @sort-change="onSortChange"
        :cell-class-name="renderColumnClass"
        ref="modelListTable"
        style="width: 100%">
        <el-table-column width="34" type="expand">
          <template slot-scope="props" v-if="props.row.status !== 'BROKEN'">
            <transition name="full-model-slide-fade">
              <div :class="renderFullExpandClass(props.row)">
                <div  v-if="!showFull" class="row-action" @click="toggleShowFull(props.$index, props.row)"><span class="tip-text">{{$t('fullScreen')}}</span><i class="el-icon-ksd-full_screen_1 full-model-box"></i></div>
                <div v-else class="row-action"  @click="toggleShowFull(props.$index, props.row)"><span class="tip-text">{{$t('exitFullScreen')}}</span><i class="el-icon-ksd-collapse_1 full-model-box" ></i></div>
                <el-tabs class="el-tabs--default model-detail-tabs" type="card" v-model="props.row.tabTypes">
                  <el-tab-pane :label="$t('segment')" name="first">
                    <ModelSegment ref="segmentComp" :model="props.row" :isShowSegmentActions="datasourceActions.includes('segmentActions')" v-if="props.row.tabTypes === 'first'" @purge-model="model => handleCommand('purge', model)" />
                  </el-tab-pane>
                  <el-tab-pane :label="$t('index')" name="second">
                    <ModelAggregate
                      :model="props.row"
                      :project-name="currentSelectedProject"
                      :isShowEditAgg="datasourceActions.includes('editAggGroup')"
                      :isShowBulidIndex="datasourceActions.includes('bulidIndex')"
                      :isShowTableIndexActions="datasourceActions.includes('tableIndexActions')"
                      @loadModels="loadModelsList"
                      v-if="props.row.tabTypes === 'second'" />
                  </el-tab-pane>
                  <!-- <el-tab-pane :label="$t('tableIndex')" name="third">
                    <TableIndex
                      :modelDesc="props.row"
                      :isShowTableIndexActions="datasourceActions.includes('tableIndexActions')"
                      :isShowBulidIndex="datasourceActions.includes('bulidIndex')"
                      v-if="props.row.tabTypes === 'third'" />
                  </el-tab-pane> -->
                  <el-tab-pane label="JSON" name="forth">
                    <ModelJson v-if="props.row.tabTypes === 'forth'" :model="props.row.uuid"/>
                  </el-tab-pane>
                  <el-tab-pane label="SQL" name="fifth">
                    <ModelSql v-if="props.row.tabTypes === 'fifth'" :model="props.row.uuid"/>
                  </el-tab-pane>
                </el-tabs>
              </div>
            </transition>
          </template>
        </el-table-column>
        <el-table-column
        min-width="209px"
        show-overflow-tooltip
        prop="alias"
          :label="modelTableTitle">
        </el-table-column>
        <el-table-column
          prop="fact_table"
          show-overflow-tooltip
          min-width="129px"
          :label="$t('kylinLang.common.fact')">
          <template slot-scope="scope">
            <span :class="{'is-disabled': scope.row.root_fact_table_deleted}">{{scope.row.fact_table}}</span>
          </template>
        </el-table-column>
        <el-table-column
          header-align="right"
          align="right"
          prop="usage"
          sortable="custom"
          show-overflow-tooltip
          width="120px"
          :render-header="renderUsageHeader"
          :label="$t('usage')">
        </el-table-column>
        <el-table-column
          header-align="right"
          align="right"
          sortable="custom"
          prop="expansionrate"
          show-overflow-tooltip
          width="170px"
          :render-header="renderExpansionRateHeader">
          <template slot-scope="scope">
              <span v-if="scope.row.expansion_rate !== '-1'">{{scope.row.expansion_rate}}%</span>
              <span v-else class="is-disabled">{{$t('tentative')}}</span>
          </template>
        </el-table-column>
         <el-table-column
          header-align="right"
          align="right"
          prop="recommendations_count"
          sortable="recommendations_count"
          width="200px"
          :render-header="renderAdviceHeader"
          v-if="$store.state.project.isSemiAutomatic && datasourceActions.includes('accelerationActions')">
          <template slot-scope="scope">
            <span v-if="scope.row.status !== 'BROKEN' && ('visible' in scope.row && scope.row.visible)" class="recommend-btn" @click="openRecommendDialog(scope.row)">{{scope.row.recommendations_count}}</span>
            <span v-else>{{scope.row.recommendations_count}}</span>
          </template>
         </el-table-column>
         <el-table-column
          header-align="right"
          align="right"
          prop="storage"
          show-overflow-tooltip
          width="120px"
          sortable="custom"
          :label="$t('storage')">
          <template slot-scope="scope">
            {{scope.row.storage|dataSize}}
          </template>
        </el-table-column>
        <el-table-column
          prop="gmtTime"
          show-overflow-tooltip
          sortable="custom"
          width="178px"
          :label="$t('dataLoadTime')">
        </el-table-column>
        <el-table-column
          prop="status"
          show-overflow-tooltip
          width="110"
          :filters="statusList.map(item => ({text: item, value: item}))"
          :filtered-value="filterArgs.status"
          :label="$t('status')"
          filter-icon="el-icon-ksd-filter"
          :show-multiple-footer="false"
          :filter-change="(v) => filterContent(v, 'status')">
          <template slot-scope="scope">
            <el-tag size="mini" :type="getModelStatusTagType[scope.row.status]">{{scope.row.status}}</el-tag>
          </template>
        </el-table-column>
        <el-table-column
          v-if="!isAutoProject"
          prop="owner"
          show-overflow-tooltip
          width="100"
          :label="$t('kylinLang.model.ownerGrid')">
        </el-table-column>
        <el-table-column
        width="96px"
        class-name="ky-hover-icon"
        v-if="!isAutoProject"
        :label="$t('kylinLang.common.action')">
          <template slot-scope="scope">
            <template v-if="'visible' in scope.row && !scope.row.visible">
              <common-tip :content="$t('authorityDetails')">
                <i class="el-icon-ksd-lock ksd-fs-14" @click="showNoAuthorityContent(scope.row)"></i>
              </common-tip>
            </template>
            <template v-else>
              <common-tip :content="$t('kylinLang.common.edit')" v-if="scope.row.status !== 'BROKEN' && datasourceActions.includes('modelActions')">
                <i class="el-icon-ksd-table_edit ksd-fs-14" @click="handleEditModel(scope.row.alias)"></i>
              </common-tip>
              <common-tip :content="$t('kylinLang.common.repair')" v-if="scope.row.broken_reason === 'SCHEMA' && datasourceActions.includes('modelActions')">
                <i class="el-icon-ksd-fix_tool ksd-fs-14" @click="handleEditModel(scope.row.alias)"></i>
              </common-tip>
              <common-tip :content="$t('build')" v-if="scope.row.status !== 'BROKEN'&&datasourceActions.includes('loadData')">
                <i class="el-icon-ksd-data_range ksd-fs-14" v-guide.setDataRangeBtn @click="setModelBuldRange(scope.row)"></i>
              </common-tip>
              <common-tip :content="$t('kylinLang.common.moreActions')" v-if="datasourceActions.includes('modelActions')">
                <el-dropdown @command="(command) => {handleCommand(command, scope.row)}" :id="scope.row.name" trigger="click" >
                  <span class="el-dropdown-link" >
                      <i class="el-icon-ksd-table_others ksd-fs-14"></i>
                  </span>
                  <el-dropdown-menu slot="dropdown"  :uuid='scope.row.uuid' >
                    <!-- 数据检测移动至project 级别处理， -->
                    <!-- <el-dropdown-item command="dataCheck">{{$t('datacheck')}}</el-dropdown-item> -->
                    <!-- 设置partition -->
                    <el-dropdown-item command="recommendations" v-if="scope.row.status !== 'BROKEN' && $store.state.project.isSemiAutomatic && datasourceActions.includes('accelerationActions')">{{$t('recommendations')}}</el-dropdown-item>
                    <el-dropdown-item command="dataLoad" v-if="scope.row.status !== 'BROKEN'">{{$t('modelPartitionSet')}}</el-dropdown-item>
                    <!-- <el-dropdown-item command="favorite" disabled>{{$t('favorite')}}</el-dropdown-item> -->
                    <el-dropdown-item command="importMDX" divided disabled v-if="scope.row.status !== 'BROKEN'">{{$t('importMdx')}}</el-dropdown-item>
                    <el-dropdown-item command="exportTDS" disabled v-if="scope.row.status !== 'BROKEN'">{{$t('exportTds')}}</el-dropdown-item>
                    <el-dropdown-item command="exportMDX" disabled v-if="scope.row.status !== 'BROKEN'">{{$t('exportMdx')}}</el-dropdown-item>
                    <el-dropdown-item command="rename" divided v-if="scope.row.status !== 'BROKEN'">{{$t('rename')}}</el-dropdown-item>
                    <el-dropdown-item command="clone" v-if="scope.row.status !== 'BROKEN'">{{$t('kylinLang.common.clone')}}</el-dropdown-item>
                    <el-dropdown-item command="delete">{{$t('delete')}}</el-dropdown-item>
                    <el-dropdown-item command="purge" v-if="scope.row.status !== 'BROKEN'">{{$t('purge')}}</el-dropdown-item>
                    <el-dropdown-item command="offline" v-if="scope.row.status !== 'OFFLINE' && scope.row.status !== 'BROKEN'">{{$t('offLine')}}</el-dropdown-item>
                    <el-dropdown-item command="online" v-if="scope.row.status !== 'ONLINE' && scope.row.status !== 'BROKEN'">{{$t('onLine')}}</el-dropdown-item>
                  </el-dropdown-menu>
                </el-dropdown>
              </common-tip>
            </template>
          </template>
        </el-table-column>
      </el-table>
      <!-- 分页 -->
      <kap-pager class="ksd-center ksd-mtb-10" ref="pager" :curPage="filterArgs.page_offset+1" :totalSize="modelsPagerRenderData.totalSize"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
    </div>
    <!-- 模型检查 -->
    <ModelCheckDataModal/>
    <!-- 模型构建 -->
    <ModelBuildModal @refreshModelList="loadModelsList" ref="modelBuildComp"/>
    <!--  数据分区设置 -->
    <ModelPartitionModal/>
    <!-- 模型重命名 -->
    <ModelRenameModal/>
    <!-- 模型克隆 -->
    <ModelCloneModal/>
    <!-- 模型添加 -->
    <ModelAddModal/>
    <!-- 模型优化建议 -->
    <ModelRecommendModal/>
    <!-- 推荐模型 -->
    <UploadSqlModel v-on:reloadModelList="loadModelsList"/>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { NamedRegex } from '../../../../config'
import { ModelStatusTagType } from '../../../../config/model.js'
import locales from './locales'
import { handleError, kapConfirm, kapMessage, handleSuccess } from 'util/business'

import { objectClone } from 'util'
import TableIndex from '../TableIndex/index.vue'
import ModelSegment from './ModelSegment/index.vue'
import ModelAggregate from './ModelAggregate/index.vue'
import ModelRenameModal from './ModelRenameModal/rename.vue'
import ModelCloneModal from './ModelCloneModal/clone.vue'
import ModelAddModal from './ModelAddModal/addmodel.vue'
import ModelCheckDataModal from './ModelCheckData/checkdata.vue'
import ModelBuildModal from './ModelBuildModal/build.vue'
import ModelPartitionModal from './ModelPartitionModal/index.vue'
import ModelJson from './ModelJson/modelJson.vue'
import ModelSql from './ModelSql/ModelSql.vue'
import ModelRecommendModal from './ModelRecommendModal/index.vue'
import { mockSQL } from './mock'
import '../../../../util/fly.js'
import UploadSqlModel from '../../../common/UploadSql/UploadSql.vue'
@Component({
  beforeRouteEnter (to, from, next) {
    next(vm => {
      if (to.params.addIndex) {
        vm.currentEditModel = from.params.modelName
        // vm.showFull = true
      }
      if (to.params.modelAlias) {
        // vm.currentEditModel = to.params.modelAlias
        vm.filterArgs.model_name = to.params.modelAlias
        vm.filterArgs.exact = true
      }
      // onSortChange 中project有值时会 loadmodellist, 达到初始化数据的目的
      vm.filterArgs.project = vm.currentSelectedProject
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'modelsPagerRenderData',
      'briefMenuGet',
      'isAutoProject',
      'datasourceActions'
    ])
  },
  methods: {
    ...mapActions({
      loadModels: 'LOAD_MODEL_LIST',
      delModel: 'DELETE_MODEL',
      checkModelName: 'CHECK_MODELNAME',
      purgeModel: 'PURGE_MODEL',
      disableModel: 'DISABLE_MODEL',
      enableModel: 'ENABLE_MODEL',
      updataModel: 'UPDATE_MODEL',
      getModelJson: 'GET_MODEL_JSON',
      getModelByModelName: 'LOAD_MODEL_INFO'
    }),
    ...mapActions('ModelRenameModal', {
      callRenameModelDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelCloneModal', {
      callCloneModelDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelAddModal', {
      callAddModelDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelCheckDataModal', {
      checkModelData: 'CALL_MODAL'
    }),
    ...mapActions('ModelBuildModal', {
      callModelBuildDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelPartitionModal', {
      callModelPartitionDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelRecommendModal', {
      callModelRecommendDialog: 'CALL_MODAL'
    }),
    ...mapActions('UploadSqlModel', {
      showUploadSqlDialog: 'CALL_MODAL'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  components: {
    TableIndex,
    ModelSegment,
    ModelAggregate,
    ModelRenameModal,
    ModelCloneModal,
    ModelAddModal,
    ModelCheckDataModal,
    ModelBuildModal,
    ModelPartitionModal,
    ModelJson,
    ModelSql,
    ModelRecommendModal,
    UploadSqlModel
  },
  locales
})
export default class ModelList extends Vue {
  mockSQL = mockSQL
  filterArgs = {
    page_offset: 0,
    page_size: 10,
    exact: false,
    model_name: '',
    sort_by: 'last_modify',
    reverse: true,
    status: []
  }
  statusList = ['ONLINE', 'OFFLINE', 'BROKEN']
  currentEditModel = null
  showFull = false
  showSearchResult = false
  searchLoading = false
  modelArray = []
  expandedRows = []
  filterTags = []
  prevExpendContent = []
  showGenerateModelDialog () {
    this.showUploadSqlDialog({
      isGenerateModel: true
    })
  }
  changeCurrentType (res) {
  }
  setRowClass (res) {
    const {row} = res
    return 'visible' in row && !row.visible ? 'no-authority-model' : ''
  }
  get emptyText () {
    return this.filterArgs.model_name ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get modelTableTitle () {
    return this.isAutoProject ? this.$t('kylinLang.model.indexGroupName') : this.$t('kylinLang.model.modelNameGrid')
  }
  getModelStatusTagType = ModelStatusTagType
  renderFullExpandClass (row) {
    return (row.showModelDetail || this.currentEditModel === row.alias) ? 'full-cell-content' : ''
  }
  renderUsageHeader (h, { column, $index }) {
    let modelMode = this.isAutoProject ? 'indexGroup' : 'model'
    return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
      <span>{this.$t('usage')}</span>&nbsp;
      <common-tip placement="top" content={this.$t('usageTip', {mode: this.$t(modelMode)})}>
       <span class='el-icon-ksd-what'></span>
      </common-tip>
    </span>)
  }
  renderAdviceHeader (h, { column, $index }) {
    return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
      <span>{this.$t('recommendations')}</span>&nbsp;
      <common-tip placement="top" content={this.$t('recommendationsTip')}>
       <span class='el-icon-ksd-what'></span>
      </common-tip>
    </span>)
  }
  renderExpansionRateHeader (h, { column, $index }) {
    return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
      <span>{this.$t('expansionRate')}</span>&nbsp;
      <common-tip placement="top" content={this.$t('expansionRateTip')}>
       <span class='el-icon-ksd-what'></span>
      </common-tip>
    </span>)
  }
  expandRow (row, expandedRows) {
    this.expandedRows = expandedRows && expandedRows.map((m) => {
      return Object.prototype.toString.call(m) === '[object Object]' ? m.alias : m
    }) || []
    this.currentEditModel = null
  }
  renderRowKey (row) {
    return row.alias
  }
  renderColumnClass ({row, column, rowIndex, columnIndex}) {
    if ((row.status === 'BROKEN' || ('visible' in row && !row.visible)) && columnIndex === 0) {
      return 'broken-column'
    }
  }
  checkName (rule, value, callback) {
    if (!NamedRegex.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  }
  _showFullDataLoadConfirm (storage, modelName) {
    const storageSize = Vue.filter('dataSize')(storage)
    const contentVal = { modelName, storageSize }
    const confirmTitle = this.$t('fullLoadDataTitle')
    const confirmMessage1 = this.$t('fullLoadDataContent1', contentVal)
    const confirmMessage2 = this.$t('fullLoadDataContent2', contentVal)
    const confirmMessage3 = this.$t('fullLoadDataContent3', contentVal)
    const confirmMessage = _render(this.$createElement)
    const confirmButtonText = this.$t('kylinLang.common.ok')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    return this.$confirm(confirmMessage, confirmTitle, { confirmButtonText, cancelButtonText, type })

    function _render (h) {
      return (
        <div>
          <p class="break-all">{confirmMessage1}</p>
          <p>{confirmMessage2}</p>
          <p>{confirmMessage3}</p>
        </div>
      )
    }
  }
  async setModelBuldRange (modelDesc) {
    if (modelDesc.partition_desc && modelDesc.partition_desc.partition_date_column) {
      await this.callModelBuildDialog({
        modelDesc: modelDesc
      })
    } else {
      let storage = modelDesc.storage
      await this._showFullDataLoadConfirm(storage, modelDesc.alias).then(() => {
        this.$refs.modelBuildComp.$emit('buildModel', {
          start: null,
          end: null,
          modelId: modelDesc.uuid
        })
      })
    }
    this.refreshSegment()
  }
  async refreshSegment () {
    this.$refs.segmentComp && await this.$refs.segmentComp.$emit('refresh')
    this.prevExpendContent = this.modelArray.filter(item => this.expandedRows.includes(item.alias))
    this.$nextTick(() => {
      this.setModelExpand()
    })
  }
  // 还原模型列表展开状态
  setModelExpand () {
    if (!this.$refs.modelListTable) return
    let obj = {}
    this.prevExpendContent.forEach(item => {
      obj[item.alias] = item
    })
    this.modelArray.forEach(it => {
      (it.alias in obj) && (it.tabTypes = obj[it.alias].tabTypes)
    })
    this.$refs.modelListTable.store.states.expandRows = []
    this.expandedRows.length && this.expandedRows.forEach(item => {
      this.$refs.modelListTable.toggleRowExpansion(item)
    })
  }
  async openRecommendDialog (modelDesc) {
    const isSubmit = await this.callModelRecommendDialog({modelDesc: modelDesc})
    isSubmit && this.loadModelsList()
  }
  async handleCommand (command, modelDesc) {
    if (command === 'dataCheck') {
      this.checkModelData({
        modelDesc: modelDesc
      }).then((isSubmit) => {
        if (isSubmit) {
          this.loadModelsList()
        }
      })
    } else if (command === 'recommendations') {
      this.openRecommendDialog(modelDesc)
    } else if (command === 'dataLoad') {
      this.getModelByModelName({model_name: modelDesc.alias, project: this.currentSelectedProject}).then((response) => {
        handleSuccess(response, (data) => {
          if (data && data.value && data.value.length) {
            this.modelData = data.value[0]
            this.modelData.project = this.currentSelectedProject
            let cloneModelDesc = objectClone(this.modelData)
            this.callModelPartitionDialog({
              modelDesc: cloneModelDesc
            }).then((res) => {
              if (res.isSubmit) {
                modelDesc.project = this.currentSelectedProject
                this.handleSaveModel(cloneModelDesc)
              }
            })
          }
        })
      }, (res) => {
        handleError(res)
      })
    } else if (command === 'rename') {
      const isSubmit = await this.callRenameModelDialog(objectClone(modelDesc))
      isSubmit && this.loadModelsList()
    } else if (command === 'delete') {
      kapConfirm(this.$t('delModelTip', {modelName: modelDesc.alias}), null, this.$t('delModelTitle')).then(() => {
        this.handleDrop(modelDesc)
      })
    } else if (command === 'purge') {
      return kapConfirm(this.$t('pergeModelTip', {modelName: modelDesc.alias}), {type: 'warning'}, this.$t('pergeModelTitle')).then(() => {
        this.handlePurge(modelDesc).then(() => {
          this.refreshSegment()
        })
      })
    } else if (command === 'clone') {
      const isSubmit = await this.callCloneModelDialog(objectClone(modelDesc))
      isSubmit && this.loadModelsList()
    } else if (command === 'offline') {
      kapConfirm(this.$t('disableModelTip', {modelName: modelDesc.alias}), null, this.$t('disableModelTitle')).then(() => {
        this.handleDisableModel(objectClone(modelDesc))
      })
    } else if (command === 'online') {
      kapConfirm(this.$t('enableModelTip', {modelName: modelDesc.alias}), null, this.$t('enableModelTitle')).then(() => {
        this.handleEnableModel(objectClone(modelDesc))
      })
    }
  }
  handleSaveModel (modelDesc) {
    // 如果未选择partition 把partition desc 设置为null
    if (!(modelDesc && modelDesc.partition_desc && modelDesc.partition_desc.partition_date_column)) {
      modelDesc.partition_desc = null
    }
    this.updataModel(modelDesc).then(() => {
      kapMessage(this.$t('kylinLang.common.saveSuccess'))
      this.loadModelsList()
    }, (res) => {
      handleError(res)
    })
  }
  handleModel (action, modelDesc, successTip) {
    return this[action]({modelId: modelDesc.uuid, project: this.currentSelectedProject}).then(() => {
      kapMessage(successTip)
      this.loadModelsList()
    }, (res) => {
      handleError(res)
    })
  }
  // 禁用model
  handleDisableModel (modelDesc) {
    this.handleModel('disableModel', modelDesc, this.$t('disableModelSuccessTip'))
  }
  // 启用model
  handleEnableModel (modelDesc) {
    this.handleModel('enableModel', modelDesc, this.$t('enabledModelSuccessTip'))
  }
  // 删除model
  handleDrop (modelDesc) {
    this.handleModel('delModel', modelDesc, this.$t('deleteModelSuccessTip'))
  }
  // 清理model
  async handlePurge (modelDesc) {
    return this.handleModel('purgeModel', modelDesc, this.$t('purgeModelSuccessTip'))
  }
  // 编辑model
  handleEditModel (modelName) {
    this.$router.push({name: 'ModelEdit', params: { modelName: modelName, action: 'edit' }})
  }
  @Watch('modelsPagerRenderData')
  onModelChange (modelsPagerRenderData) {
    this.modelArray = []
    modelsPagerRenderData.list.forEach(item => {
      this.$set(item, 'showModelDetail', false)
      this.modelArray.push({
        ...item,
        tabTypes: this.currentEditModel === item.alias ? 'second' : 'first'
      })
    })
  }
  onSortChange ({ column, prop, order }) {
    this.filterArgs.sort_by = prop
    if (prop === 'gmtTime') {
      this.filterArgs.sort_by = 'last_modify'
    }
    this.filterArgs.reverse = !(order === 'ascending')
    if (this.filterArgs.project) {
      this.pageCurrentChange(0, this.filterArgs.page_size)
    }
  }
  // 全屏查看模型附属信息
  toggleShowFull (index, row) {
    var scrollBoxDom = document.getElementById('scrollContent')
    if (!this.showFull && scrollBoxDom) {
      // 展开时记录下展开时候的scrollbar 的top距离，搜索的时候复原该位置
      row.hisScrollTop = scrollBoxDom.scrollTop
    }
    this.$nextTick(() => {
      this.$set(row, 'showModelDetail', !this.showFull)
      this.showFull = !this.showFull
      this.$nextTick(() => {
        if (scrollBoxDom) {
          if (this.showFull) {
            scrollBoxDom.scrollTop = 0
          } else {
            scrollBoxDom.scrollTop = row.hisScrollTop
          }
          this.currentEditModel = null
        }
      })
    })
  }
  // 加载模型列表
  loadModelsList () {
    this.prevExpendContent = this.modelArray.filter(item => this.expandedRows.includes(item.alias))
    return this.loadModels(this.filterArgs).then(() => {
      if (this.filterArgs.model_name || this.modelsPagerRenderData.list.length) {
        this.showSearchResult = true
      } else {
        this.showSearchResult = false
      }
      this.$nextTick(() => {
        this.expandedRows = this.currentEditModel ? [this.currentEditModel] : this.expandedRows
        this.setModelExpand()
      })
    }).catch((res) => {
      handleError(res)
    })
  }
  // 分页
  pageCurrentChange (size, count) {
    this.filterArgs.page_offset = size
    this.filterArgs.page_size = count
    this.loadModelsList()
  }
  // 搜索模型
  searchModels () {
    if (this.filterArgs.exact) {
      this.filterArgs.exact = false
    }
    this.filterArgs.page_offset = 0
    this.searchLoading = true
    this.loadModelsList().then(() => {
      this.searchLoading = false
    }).finally((res) => {
      this.searchLoading = false
    })
  }
  showAddModelDialog () {
    this.callAddModelDialog()
  }
  // 查询状态过滤回调函数
  filterContent (val, type) {
    const maps = {
      status: 'status'
    }

    this.filterTags = this.filterTags.filter((item, index) => item.key !== type || item.key === type && val.includes(item.label))
    const list = this.filterTags.filter(it => it.key === type).map(it => it.label)
    val.length && val.forEach(item => {
      if (!list.includes(item)) {
        this.filterTags.push({label: item, source: maps[type], key: type})
      }
    })
    this.filterArgs[type] = val
    this.pageCurrentChange(0, this.filterArgs.page_size)
  }
  // 删除单个筛选条件
  handleClose (tag) {
    const index = this.filterArgs[tag.key].indexOf(tag.label)

    index > -1 && this.filterArgs[tag.key].splice(index, 1)
    this.filterTags = this.filterTags.filter(item => item.key !== tag.key || item.key === tag.key && tag.label !== item.label)
    this.pageCurrentChange(0, this.filterArgs.page_size)
  }
  // 清除所有筛选条件
  clearAllTags () {
    this.filterArgs.status.splice(0, this.filterArgs.status.length)
    this.filterTags = []
    this.pageCurrentChange(0, this.filterArgs.page_size)
  }
  // 展示model无权限的相关table和columns信息
  showNoAuthorityContent (row) {
    const { unauthorized_tables, unauthorized_columns } = row
    let details = []
    if (unauthorized_tables && unauthorized_tables.length) {
      details.push({title: `Table (${unauthorized_tables.length})`, list: unauthorized_tables})
    }
    if (unauthorized_columns && unauthorized_columns.length) {
      details.push({title: `Columns (${unauthorized_columns.length})`, list: unauthorized_columns})
    }
    this.callGlobalDetailDialog({
      theme: 'plain-mult',
      title: this.$t('kylinLang.model.authorityDetail'),
      msg: this.$t('kylinLang.model.authorityMsg', {modelName: row.name}),
      showCopyBtn: true,
      showIcon: false,
      showDetailDirect: true,
      details,
      showDetailBtn: false,
      dialogType: 'error',
      customClass: 'no-acl-model',
      showCopyTextLeftBtn: true
    })
  }
}
</script>
<style lang="less">
@import '../../../../assets/styles/variables.less';
.mode-list{
  .broken-column {
    .cell {
      display: none;
    }
  }
  .model-list-header {
    height: 50px;
  }
  .full-model-slide-fade-enter-active {
    transition: all .3s ease;
  }
  .full-model-slide-fade-leave-active {
    transition: all .3s cubic-bezier(1.0, 0.5, 0.8, 1.0);
  }
  .full-model-slide-fade-enter, .full-model-slide-fade-leave-to {
    transform: translateY(10px);
    opacity: 0;
  }
  .row-action {
    position: absolute;
    right:0;
    text-align: right;
    z-index: 2;
    cursor: pointer;
    color: @text-normal-color;
    &:hover {
      color: @base-color;
      .tip-text {
        color: @base-color;
      }
    }
    .tip-text {
      top:10px;
      color: @text-normal-color;
    }
  }
  .notice-box {
    position:relative;
    .el-alert{
      background-color:@base-color-9;
      a {
        text-decoration: underline;
        color:@base-color-1;
      }
    }
    .tip-toggle-btnbox {
      position:absolute;
      top:4px;
      right:10px;
    }
  }
  .model_list_table {
    .recommend-btn {
      color: @base-color;
      cursor: pointer;
    }
    span.is-disabled {
      color: @text-disabled-color;
    }
    .el-table__expanded-cell {
      background-color: #fbfbfb;
      padding-bottom:0;
      &:hover {
        background-color: @breadcrumbs-bg-color;
      }
      .full-cell-content {
        position: relative;
      }
      .full-model-box {
        vertical-align:middle;
        font-size: 20px;
        margin-left:10px;
        z-index: 10;
      }
    }
    .el-table__row.no-authority-model {
      background-color: #f5f5f5;
      color: @text-disabled-color;
      // pointer-events: none;
    }
    .el-icon-ksd-filter {
      position: relative;
      font-size: 17px;
      top: 2px;
      left: 5px;
      &:hover,
      &.filter-open {
        color: @base-color;
      }
    }
    .cell.highlight {
      .el-icon-ksd-filter {
        color: @base-color;
      }
    }
    .ky-hover-icon {
      .cell {
        .tip_box {
          margin-left: 10px;
          &:first-child {
            margin-left: 0;
          }
        }
      }
    }
    .el-icon-ksd-lock {
      color: @text-title-color;
    }
  }
  margin-left: 20px;
  margin-right: 20px;
  .row-action {
    right:20px;
  }
  &.full-cell {
    margin: 0 20px;
    position: relative;
    .segment-settings {
      display: block;
    }
    .segment-actions .left {
      display: block;
    }
    .model_list_table {
      position: static !important;
      border: none;
      td {
        position: static !important;
      }
      .el-table__body-wrapper {
        position: static !important;
        .el-table__expanded-cell {
          padding: 0;
          .full-cell-content {
            z-index: 999;
            position: absolute;
            padding-top: 10px;
            background: @breadcrumbs-bg-color;
            top: 0px;
            height: 100vh;
            width: calc(~'100% + 40px');
            padding-right: 20px;
            padding-left: 20px;
            margin-left: -20px;
            border-top: 1px solid #CFD8DC;
            &.hidden-cell {
              display: none;
            }
            .full-model-box {
              top: 20px;
              right: 20px;
            }
          }
        }
      }
    }
  }
  .el-tabs__nav {
    margin-left: 0;
  }
  .el-tabs__content {
    overflow: initial;
  }
  .filter-tags {
    margin-bottom: 10px;
    padding: 0px 5px 5px;
    box-sizing: border-box;
    position: relative;
    background: @background-disabled-color;
    .filter-tags-layout {
      display: inline-block;
      width: calc(~'100% - 80px');
    }
    .el-tag {
      margin-left: 5px;
      margin-top: 5px;
    }
    .clear-all-filters {
      position: absolute;
      top: 8px;
      right: 10px;
      font-size: 14px;
      color: @base-color;
      cursor: pointer;
    }
  }
}
.no-acl-model {
  .dialog-detail {
    .dialog-detail-scroll {
      max-height: 200px;
    }
  }
}
</style>
