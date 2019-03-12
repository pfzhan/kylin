<template>
  <div class="mode-list" :class="{'full-cell': showFull}">
    <div class="ksd-title-label ksd-mt-20" v-if="!isAutoProject">{{$t('kylinLang.model.modelList')}}</div>
    <div class="ksd-title-label ksd-mt-20" v-else>{{$t('kylinLang.model.indexGroup')}}</div>
    <div v-if="showSearchResult">
      <div  class="ksd-mb-10 ksd-fright ksd-mt-10">
        <el-input :placeholder="$t('kylinLang.common.pleaseFilterByModelName')" style="width:200px" size="medium" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'" v-model="filterArgs.model"  @input="searchModels" class="show-search-btn" >
        </el-input>
      </div>
      <el-button v-guide.addModelBtn icon="el-icon-plus" type="primary" size="medium" plain class="ksd-mb-10 ksd-mt-10" id="addModel" v-visible="!isAutoProject && (isAdmin || hasPermissionOfProject())" @click="showAddModelDialog"><span>{{$t('kylinLang.common.model')}}</span></el-button>
      <el-table class="model_list_table"
        :data="modelArray"
        border
        tooltip-effect="dark"
        :expand-row-keys=[currentExtandRow]
        :row-key="renderRowKey"
        :default-sort = "{prop: 'gmtTime', order: 'descending'}"
        @sort-change="onSortChange"
        :cell-class-name="renderColumnClass"
        style="width: 100%">
        <el-table-column  width="34" type="expand" >
          <template slot-scope="props" v-if="props.row.status !== 'BROKEN'">
            <transition name="full-model-slide-fade">
              <div class="cell-content" v-if="props.row.showModelDetail">
                <div  v-if="!showFull" class="row-action" @click="toggleShowFull(props.$index, props.row)"><span class="tip-text">{{$t('fullScreen')}}</span><i class="el-icon-ksd-full_screen_1 full-model-box"></i></div>
                <div v-else class="row-action"  @click="toggleShowFull(props.$index, props.row)"><span class="tip-text">{{$t('exitFullScreen')}}</span><i class="el-icon-ksd-collapse_1 full-model-box" ></i></div>
                <el-tabs class="el-tabs--default model-detail-tabs" v-model="props.row.tabTypes">
                  <el-tab-pane :label="$t('segment')" name="first">
                    <ModelSegment :model="props.row" v-if="props.row.tabTypes === 'first'" @purge-model="model => handleCommand('purge', model)" />
                  </el-tab-pane>
                  <el-tab-pane :label="$t('aggregate')" name="second">
                    <ModelAggregate :model="props.row" :project-name="currentSelectedProject" v-if="props.row.tabTypes === 'second'" />
                  </el-tab-pane>
                  <el-tab-pane :label="$t('tableIndex')" name="third">
                    <TableIndex :modelDesc="props.row" v-if="props.row.tabTypes === 'third'"></TableIndex>
                  </el-tab-pane>
                  <el-tab-pane label="JSON" name="forth">
                    <ModelJson v-if="props.row.tabTypes === 'forth'" :model="props.row.uuid"/>
                  </el-tab-pane>
                </el-tabs>
              </div>
            </transition>
          </template>
        </el-table-column>
        <el-table-column
        header-align="center"
        min-width="229px"
        show-overflow-tooltip
        prop="alias"
          :label="modelTableTitle">
        </el-table-column>
        <el-table-column
          header-align="center"
          prop="fact_table"
          show-overflow-tooltip
          min-width="229px"
          :label="$t('kylinLang.common.fact')">
        </el-table-column>
        <el-table-column
          header-align="center"
          prop="usage"
          show-overflow-tooltip
          width="80px"
          :label="$t('usage')">
        </el-table-column>
         <el-table-column
          header-align="center"
          prop="storage"
          show-overflow-tooltip
          width="77px"
          :label="$t('storage')">
          <template slot-scope="scope">
            {{scope.row.storage|dataSize}}
          </template>
        </el-table-column>
        <el-table-column
          header-align="center"
          prop="gmtTime"
          show-overflow-tooltip
          sortable="custom"
          width="150px"
          :label="$t('dataLoadTime')">
        </el-table-column>
        <el-table-column
          header-align="center"
          prop="status"
          show-overflow-tooltip
          width="94"
          :label="$t('status')">
          <template slot-scope="scope">
        <el-tag size="small" :type="scope.row.status === 'OFFLINE' ? 'info' : scope.row.status === 'BROKEN'? 'danger' : 'success'">{{scope.row.status}}</el-tag>
      </template>
        </el-table-column>
        <el-table-column
          header-align="center"
          v-if="!isAutoProject"
          prop="owner"
          show-overflow-tooltip
          width="100"
          :label="$t('kylinLang.model.ownerGrid')">
        </el-table-column>
        <el-table-column class="ksd-center"
        header-align="center"
        width="96px"
        v-if="!isAutoProject"
        :label="$t('kylinLang.common.action')">
          <template slot-scope="scope">
            <span v-if="!(isAdmin || hasPermissionOfProject())"> N/A</span>
             <div v-show="isAdmin || hasPermissionOfProject()">
              <common-tip :content="$t('kylinLang.common.edit')"><i class="el-icon-ksd-table_edit ksd-fs-14" v-if="scope.row.status !== 'BROKEN'" @click="handleEditModel(scope.row.alias)"></i></common-tip>
              <common-tip :content="$t('build')" v-if="scope.row.management_type!=='TABLE_ORIENTED'" class="ksd-ml-10"><i class="el-icon-ksd-data_range ksd-fs-14" @click="setModelBuldRange(scope.row)"></i></common-tip>
              <common-tip :content="$t('kylinLang.common.moreActions')" class="ksd-ml-10">
                <el-dropdown @command="(command) => {handleCommand(command, scope.row)}" :id="scope.row.name" trigger="click" >
                  <span class="el-dropdown-link" >
                      <i class="el-icon-ksd-table_others ksd-fs-14"></i>
                  </span>
                 <el-dropdown-menu slot="dropdown"  :uuid='scope.row.uuid' >
                    <!-- 数据检测移动至project 级别处理， -->
                    <!-- <el-dropdown-item command="dataCheck">{{$t('datacheck')}}</el-dropdown-item> -->
                    <!-- 设置partition -->
                    <el-dropdown-item command="dataLoad" v-if="scope.row.status !== 'BROKEN'">{{$t('modelPartitionSet')}}</el-dropdown-item>
                    <!-- <el-dropdown-item command="favorite" disabled>{{$t('favorite')}}</el-dropdown-item> -->
                    <el-dropdown-item command="importMDX" divided disabled v-if="scope.row.status !== 'BROKEN'">{{$t('importMdx')}}</el-dropdown-item>
                    <el-dropdown-item command="exportTDS" disabled v-if="scope.row.status !== 'BROKEN'">{{$t('exportTds')}}</el-dropdown-item>
                    <el-dropdown-item command="exportMDX" disabled v-if="scope.row.status !== 'BROKEN'">{{$t('exportMdx')}}</el-dropdown-item>
                    <el-dropdown-item command="rename" divided v-if="scope.row.status !== 'BROKEN'">{{$t('rename')}}</el-dropdown-item>
                    <el-dropdown-item command="clone" v-if="scope.row.status !== 'BROKEN'">{{$t('kylinLang.common.clone')}}</el-dropdown-item>
                    <el-dropdown-item command="delete">{{$t('delete')}}</el-dropdown-item>
                    <el-dropdown-item command="purge" v-if="scope.row.management_type!=='TABLE_ORIENTED'">{{$t('purge')}}</el-dropdown-item>
                    <el-dropdown-item command="offline" v-if="scope.row.status !== 'OFFLINE' && scope.row.status !== 'BROKEN'">{{$t('offLine')}}</el-dropdown-item>
                    <el-dropdown-item command="online" v-if="scope.row.status !== 'ONLINE' && scope.row.status !== 'BROKEN'">{{$t('onLine')}}</el-dropdown-item>
                    </el-dropdown-menu>
                  </el-dropdown>
                </common-tip>
              </div>
          </template>
        </el-table-column>
      </el-table>
      <!-- 分页 -->
      <kap-pager class="ksd-center ksd-mt-20 ksd-mb-20" ref="pager"  :totalSize="modelsPagerRenderData.totalSize"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
    </div>
    <div class="ksd-null-pic-text" v-if="!showSearchResult">
      <img src="../../../../assets/img/no_model.png">
      <p v-if="!isAutoProject && (isAdmin || hasPermissionOfProject())">{{$t('noModel')}}</p>
      <div>
      <el-button v-guide.addModelBtn size="medium" type="primary" icon="el-icon-plus"  v-if="!isAutoProject && (isAdmin || hasPermissionOfProject())" @click="showAddModelDialog">{{$t('kylinLang.common.model')}}</el-button>
       </div>
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
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { permissions, NamedRegex } from '../../../../config'
import locales from './locales'
import { handleError, hasRole, hasPermission, kapConfirm, kapMessage } from 'util/business'
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
import { mockSQL } from './mock'
import '../../../../util/fly.js'
@Component({
  beforeRouteEnter (to, from, next) {
    next(vm => {
      if (from.params.modelName) {
        vm.currentEditModel = from.params.modelName
      }
      if (to.params.addIndex) {
        vm.showFull = true
      }
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'modelsPagerRenderData',
      'briefMenuGet',
      'isAutoProject'
    ]),
    currentExtandRow () {
      return [this.currentEditModel]
    }
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
      getModelJson: 'GET_MODEL_JSON'
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
    ModelJson
  },
  locales
})
export default class ModelList extends Vue {
  mockSQL = mockSQL
  filterArgs = {
    pageOffset: 0,
    pageSize: 10,
    exact: false,
    model: '',
    sortBy: 'last_modify',
    reverse: true
  }
  currentEditModel = null
  showFull = false
  showSearchResult = false
  searchLoading = false
  modelArray = []
  get modelTableTitle () {
    return this.isAutoProject ? this.$t('kylinLang.model.indexGroupName') : this.$t('kylinLang.model.modelNameGrid')
  }
  renderRowKey (row) {
    return row.alias
  }
  renderColumnClass ({row, column, rowIndex, columnIndex}) {
    if (row.status === 'BROKEN' && columnIndex === 0) {
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
  setModelBuldRange (modelDesc) {
    if (modelDesc.partition_desc && modelDesc.partition_desc.partition_date_column) {
      this.callModelBuildDialog({
        modelDesc: modelDesc
      })
    } else {
      let storage = modelDesc.storage
      this._showFullDataLoadConfirm(storage, modelDesc.alias).then(() => {
        this.$refs.modelBuildComp.$emit('buildModel', {
          start: null,
          end: null,
          modelId: modelDesc.uuid
        })
      })
    }
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
    } else if (command === 'dataLoad') {
      let cloneModelDesc = objectClone(modelDesc)
      this.callModelPartitionDialog({
        modelDesc: cloneModelDesc
      }).then((res) => {
        if (res.isSubmit) {
          modelDesc.project = this.currentSelectedProject
          this.handleSaveModel(cloneModelDesc)
        }
      })
    } else if (command === 'rename') {
      const isSubmit = await this.callRenameModelDialog(objectClone(modelDesc))
      isSubmit && this.loadModelsList()
    } else if (command === 'delete') {
      kapConfirm(this.$t('delModelTip')).then(() => {
        this.handleDrop(modelDesc)
      })
    } else if (command === 'purge') {
      kapConfirm(this.$t('pergeModelTip')).then(() => {
        this.handlePurge(modelDesc)
      })
    } else if (command === 'clone') {
      const isSubmit = await this.callCloneModelDialog(objectClone(modelDesc))
      isSubmit && this.loadModelsList()
    } else if (command === 'offline') {
      kapConfirm(this.$t('disbaleModelTip')).then(() => {
        this.handleDisableModel(objectClone(modelDesc))
      })
    } else if (command === 'online') {
      kapConfirm(this.$t('enableModelTip')).then(() => {
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
    this[action]({modelId: modelDesc.uuid, project: this.currentSelectedProject}).then(() => {
      kapMessage(successTip)
      this.loadModelsList()
    }, (res) => {
      handleError(res)
    })
  }
  // 禁用model
  handleDisableModel (modelDesc) {
    this.handleModel('disableModel', modelDesc, this.$t('disbaleModelSuccessTip'))
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
  handlePurge (modelDesc) {
    this.handleModel('purgeModel', modelDesc, this.$t('purgeModelSuccessTip'))
  }
  // 编辑model
  handleEditModel (modelName) {
    this.$router.push({name: 'ModelEdit', params: { modelName: modelName, action: 'edit' }})
  }
  @Watch('modelsPagerRenderData')
  onModelChange (modelsPagerRenderData) {
    this.modelArray = []
    modelsPagerRenderData.list.forEach(item => {
      this.$set(item, 'showModelDetail', true)
      this.modelArray.push({
        ...item,
        tabTypes: this.currentEditModel === item.alias ? 'second' : 'first'
      })
    })
  }
  onSortChange ({ column, prop, order }) {
    if (prop === 'gmtTime') {
      this.filterArgs.sortBy = 'last_modify'
      this.filterArgs.reverse = !(order === 'ascending')
    }
    this.loadModelsList()
  }
  // 全屏查看模型附属信息
  toggleShowFull (index, row) {
    var scrollBoxDom = document.getElementById('scrollContent')
    this.$set(row, 'showModelDetail', false)
    if (!this.showFull && scrollBoxDom) {
      // 展开时记录下展开时候的scrollbar 的top距离，搜索的时候复原该位置
      row.hisScrollTop = scrollBoxDom.scrollTop
    }
    this.$nextTick(() => {
      this.$set(row, 'showModelDetail', true)
      this.showFull = !this.showFull
      this.$nextTick(() => {
        if (scrollBoxDom) {
          if (this.showFull) {
            this.currentEditModel = row.alias
            scrollBoxDom.scrollTop = 0
          } else {
            scrollBoxDom.scrollTop = row.hisScrollTop
          }
        }
      })
    })
  }
  // 加载模型列表
  loadModelsList () {
    return this.loadModels(this.filterArgs).then(() => {
      if (this.filterArgs.model || this.modelsPagerRenderData.list.length) {
        this.showSearchResult = true
      } else {
        this.showSearchResult = false
      }
    }).catch((res) => {
      handleError(res)
    })
  }
  // 分页
  pageCurrentChange (size, count) {
    this.filterArgs.pageOffset = size
    this.filterArgs.pageSize = count
    this.loadModelsList()
  }
  // 搜索模型
  searchModels () {
    clearTimeout(this.ST)
    this.ST = setTimeout(() => {
      this.searchLoading = true
      this.loadModelsList().then(() => {
        this.searchLoading = false
      }).finally((res) => {
        this.searchLoading = false
      })
    }, 500)
  }
  showAddModelDialog () {
    this.callAddModelDialog()
  }
  hasPermissionOfProject () {
    return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
  }
  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }
  created () {
    this.filterArgs.project = this.currentSelectedProject
    if (this.filterArgs.project) {
      this.loadModelsList()
    }
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
  .model-detail-tabs {
    &>.el-tabs__header{
      margin-bottom:0;
    }
  }
  .row-action {
    position: absolute;
    right:0;
    top:8px;
    width:300px;
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
  .model_list_table .el-table__expanded-cell {
    background-color: @breadcrumbs-bg-color;
    padding: 20px;
    padding-bottom:0;
    &:hover {
      background-color: @breadcrumbs-bg-color;
    }
    .cell-content {
      position: relative;
      .full-model-box {
        vertical-align:middle;
        font-size: 20px;
        margin-left:10px;
        z-index: 10;
      }
    }
  }
  margin-left: 20px;
  margin-right: 20px;
  &.full-cell {
    .row-action {
      right:20px;
      top:20px;
    }
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
          .cell-content {
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
}
</style>
