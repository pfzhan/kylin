<template>
  <div class="mode-list" :class="{'full-cell': showFull}">
    <div class="notice-box" v-if="!!reachThreshold">
      <el-alert
      type="warning"
      :closable="false"
      show-icon>
       <p slot="title"><span v-html="$t('speedTip', {queryCount: modelSpeedEvents ,modelCount: modelSpeedModelsCount})"></span><a @click="applySpeed">{{$t('apply')}}</a></p>
      </el-alert>
      <div class="tip-toggle-btnbox">
        <el-button-group>
          <el-button icon="el-icon-arrow-left" size="mini"></el-button>
          <el-button icon="el-icon-arrow-right" size="mini"></el-button>
        </el-button-group>
      </div>
    </div>
    <div class="ky-list-title ksd-mt-20">{{$t('kylinLang.model.modelList')}}</div>
    <div v-if="showSearchResult">
      <div  class="ksd-mb-14 ksd-fright ksd-mt-8">
        <el-input :placeholder="$t('kylinLang.common.pleaseFilterByModelName')" style="width:400px" size="medium" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'" v-model="filterArgs.model"  @input="searchModels" class="show-search-btn" >
        </el-input>
      </div>
      <el-button icon="el-icon-plus" type="primary" size="medium" plain class="ksd-mb-14 ksd-mt-8" id="addModel" v-visible="isAdmin || hasPermissionOfProject()" @click="showAddModelDialog"><span>{{$t('kylinLang.common.model')}}</span></el-button>
      <el-table class="model_list_table"
        :data="modelArray"
        border
        tooltip-effect="dark"
        @sort-change="onSortChange"
        style="width: 100%">
        <el-table-column type="expand" min-width="30">
          <template slot-scope="props">
            <div class="cell-content" :class="{'hidden-cell': props.$index !== activeIndex}">
              <i class="el-icon-ksd-full_screen ksd-fright full-model-box" v-if="!showFull" @click="toggleShowFull(props.$index)"></i>
              <i class="el-icon-ksd-collapse ksd-fright full-model-box" v-else @click="showFull = false"></i>
              <el-tabs activeName="first" class="el-tabs--default model-detail-tabs" v-model="props.row.tabTypes">
                <el-tab-pane :label="$t('segment')" name="first">
                  <ModelSegment :model="props.row" v-if="props.row.tabTypes === 'first'" />
                </el-tab-pane>
                <el-tab-pane :label="$t('aggregate')" name="second">
                  <ModelAggregate :model="props.row" :project-name="currentSelectedProject" v-if="props.row.tabTypes === 'second'" />
                </el-tab-pane>
                <el-tab-pane :label="$t('tableIndex')" name="third">
                  <TableIndex :modelDesc="props.row"></TableIndex>
                </el-tab-pane>
                <el-tab-pane label="JSON" name="forth">
                  <el-input
                    class="model-json"
                    :value="JSON.stringify(props.row, '', 4)"
                    type="textarea"
                    :rows="18"
                    :readonly="true">
                  </el-input>
                </el-tab-pane>
              </el-tabs>
            </div>
          </template>
        </el-table-column>
        <el-table-column
        show-overflow-tooltip
        prop="alias"
          :label="$t('kylinLang.model.modelNameGrid')">
        </el-table-column>
        <el-table-column
          prop="fact_table"
          show-overflow-tooltip
          width="210"
          :label="$t('kylinLang.common.fact')">
        </el-table-column>
        <el-table-column
          prop="favorite"
          show-overflow-tooltip
          width="180"
          :label="$t('favorite')">
        </el-table-column>
        <el-table-column
          prop="gmtTime"
          show-overflow-tooltip
          sortable="custom"
          width="210"
          :label="$t('dataLoadTime')">
        </el-table-column>
        <el-table-column
          prop="status"
          show-overflow-tooltip
          width="100"
          :label="$t('status')">
          <template slot-scope="scope">
        <el-tag size="small" :type="scope.row.status === 'OFFLINE' ? 'info' : scope.row.status === 'DESCBROKEN'? 'danger' : 'success'">{{scope.row.status}}</el-tag>
      </template>
        </el-table-column>
        <el-table-column
          prop="owner"
          show-overflow-tooltip
          width="100"
          :label="$t('kylinLang.model.ownerGrid')">
        </el-table-column>
        <el-table-column class="ksd-center"
        width="120"
        :label="$t('kylinLang.common.action')">
          <template slot-scope="scope">
            <span v-if="!(isAdmin || hasPermissionOfProject())"> N/A</span>
             <div v-show="isAdmin || hasPermissionOfProject()">
              <common-tip :content="$t('kylinLang.common.edit')" class="ksd-ml-10"><i class="el-icon-ksd-table_edit ksd-fs-16" @click="handleEditModel(scope.row.alias)"></i></common-tip>
              <common-tip :content="$t('build')" class="ksd-ml-10"><i class="el-icon-ksd-data_range ksd-fs-16" @click="setModelBuldRange(scope.row)" v-if="scope.row.management_type!=='TABLE_ORIENTED'"></i></common-tip>
              <common-tip :content="$t('kylinLang.common.moreActions')" class="ksd-ml-10" v-if="!scope.row.is_draft">
                <el-dropdown @command="(command) => {handleCommand(command, scope.row)}" :id="scope.row.name" trigger="click" >
                  <span class="el-dropdown-link" >
                      <i class="el-icon-ksd-table_others ksd-fs-16"></i>
                  </span>
                 <el-dropdown-menu slot="dropdown"  :uuid='scope.row.uuid' >
                    <el-dropdown-item command="dataCheck">{{$t('datacheck')}}</el-dropdown-item>
                    <!-- 设置partition -->
                    <el-dropdown-item command="dataLoad" v-if="scope.row.management_type!=='TABLE_ORIENTED'">{{$t('dataloading')}}</el-dropdown-item>
                    <!-- <el-dropdown-item command="favorite" disabled>{{$t('favorite')}}</el-dropdown-item> -->
                    <el-dropdown-item command="importMDX" divided disabled v-if="scope.row.status !== 'DESCBROKEN'">{{$t('importMdx')}}</el-dropdown-item>
                    <el-dropdown-item command="exportTDS" disabled v-if="scope.row.status !== 'DESCBROKEN'">{{$t('exportTds')}}</el-dropdown-item>
                    <el-dropdown-item command="exportMDX" disabled v-if="scope.row.status !== 'DESCBROKEN'">{{$t('exportMdx')}}</el-dropdown-item>
                    <el-dropdown-item command="rename" divided v-if="scope.row.status !== 'DESCBROKEN'">{{$t('rename')}}</el-dropdown-item>
                    <el-dropdown-item command="clone" v-if="scope.row.status !== 'DESCBROKEN'">{{$t('kylinLang.common.clone')}}</el-dropdown-item>
                    <el-dropdown-item command="delete">{{$t('delete')}}</el-dropdown-item>
                    <el-dropdown-item command="purge" v-if="scope.row.management_type!=='TABLE_ORIENTED'">{{$t('purge')}}</el-dropdown-item>
                    <el-dropdown-item command="offline" v-if="scope.row.status !== 'OFFLINE' && scope.row.status !== 'DESCBROKEN'">{{$t('offLine')}}</el-dropdown-item>
                    <el-dropdown-item command="online" v-if="scope.row.status !== 'ONLINE' && scope.row.status !== 'DESCBROKEN'">{{$t('onLine')}}</el-dropdown-item>
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
      <p v-if="isAdmin || hasPermissionOfProject()">{{$t('noModel')}}</p>
      <div>
      <el-button size="medium" type="primary" icon="el-icon-plus"  v-if="isAdmin || hasPermissionOfProject()" @click="showAddModelDialog">{{$t('kylinLang.common.model')}}</el-button>
       </div>
    </div>

    <el-dialog width="440px" :title="$t('kylinLang.common.notice')" :visible.sync="reachThreshold" :show-close="false">
      <div>
        {{$t('hello', {user: currentUser.username})}}<br/>
        <p style="text-indent:25px; line-height: 26px;" v-html="$t('speedTip', {queryCount: modelSpeedEvents ,modelCount: modelSpeedModelsCount})"></p>
      </div>
      <div slot="footer" class="dialog-footer">
        <el-button size="medium" @click="ignoreSpeed" :loading="btnLoadingCancel">{{$t('ignore')}}</el-button>
        <el-button size="medium" type="primary" plain @click="applySpeed" :loading="btnLoading">{{$t('apply')}}</el-button>
      </div>
    </el-dialog>
    <!-- 模型检查 -->
    <ModelCheckDataModal/>
    <!-- 模型构建 -->
    <ModelBuildModal/>
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
import $ from 'jquery'
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
import { mockSQL } from './mock'
import '../../../../util/fly.js'
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'modelsPagerRenderData',
      'briefMenuGet'
    ]),
    modelSpeedEvents () {
      return this.$store.state.model.modelSpeedEvents
    },
    reachThreshold () {
      return this.$store.state.model.reachThreshold
    },
    modelSpeedModelsCount () {
      return this.$store.state.model.modelSpeedModelsCount
    },
    currentUser () {
      return this.$store.state.user.currentUser
    }
  },
  methods: {
    ...mapActions({
      loadModels: 'LOAD_MODEL_LIST',
      delModel: 'DELETE_MODEL',
      checkModelName: 'CHECK_MODELNAME',
      applySpeedInfo: 'APPLY_SPEED_INFO',
      purgeModel: 'PURGE_MODEL',
      disableModel: 'DISABLE_MODEL',
      enableModel: 'ENABLE_MODEL',
      getSpeedInfo: 'GET_SPEED_INFO',
      ignoreSpeedInfo: 'IGNORE_SPEED_INFO',
      updataModel: 'UPDATE_MODEL'
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
    ModelPartitionModal
  },
  locales
})
export default class ModelList extends Vue {
  mockSQL = mockSQL
  checkOptions = []
  applyDialogVisible = false
  createModelVisible = false
  cloneFormVisible = false
  modelCheckModeVisible = false
  dataRangeVal = []
  btnLoading = false
  btnLoadingCancel = false
  createModelFormRule = {
    modelName: [
      {required: true, message: this.$t('inputModelName'), trigger: 'blur'},
      {validator: this.checkName, trigger: 'blur'}
    ]
  }
  createModelMeta = {
    modelName: '',
    modelDesc: ''
  }
  filterArgs = {
    pageOffset: 0,
    pageSize: 10,
    exact: false,
    model: '',
    sortBy: '',
    reverse: true
  }
  showFull = false
  activeIndex = -1
  showSearchResult = false
  searchLoading = false
  modelArray = []
  loadSpeedInfo (loadingname) {
    var loadingName = loadingname || 'btnLoading'
    this.getSpeedInfo(this.currentSelectedProject).then(() => {
      this[loadingName] = false
    }, (res) => {
      this[loadingName] = false
      handleError(res)
    })
  }
  applySpeed (event) {
    this.btnLoading = true
    this.applySpeedInfo({size: this.modelSpeedEvents, project: this.currentSelectedProject}).then(() => {
      this.flyEvent(event)
      this.loadSpeedInfo()
    }, (res) => {
      this.btnLoading = false
      handleError(res)
    })
    this.applyDialogVisible = false
  }
  // 忽略此次加速
  ignoreSpeed () {
    this.btnLoadingCancel = true
    this.ignoreSpeedInfo(this.currentSelectedProject).then(() => {
      this.btnLoadingCancel = false
      this.loadSpeedInfo('btnLoadingCancel')
    }, (res) => {
      this.btnLoadingCancel = false
      handleError(res)
    })
  }
  flyEvent (event) {
    var targetArea = $('#monitor')
    var targetDom = targetArea.find('.menu-icon')
    var offset = targetDom.offset()
    var flyer = $('<span class="fly-box"></span>')
    let leftOffset = 64
    if (this.$lang === 'en') {
      leftOffset = 74
    }
    if (this.briefMenuGet) {
      leftOffset = 20
    }
    flyer.fly({
      start: {
        left: event.pageX,
        top: event.pageY
      },
      end: {
        left: offset.left + leftOffset,
        top: offset.top,
        width: 4,
        height: 4
      },
      onEnd: function () {
        targetDom.addClass('rotateY')
        setTimeout(() => {
          targetDom.fadeTo('slow', 0.5, function () {
            targetDom.removeClass('rotateY')
            targetDom.fadeTo('fast', 1)
          })
          flyer.fadeOut(1500, () => {
            flyer.remove()
          })
        }, 3000)
      }
    })
  }
  checkName (rule, value, callback) {
    if (!NamedRegex.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  }
  setModelBuldRange (modelDesc) {
    this.callModelBuildDialog({
      modelDesc: modelDesc
    })
  }
  async handleCommand (command, modelDesc) {
    if (command === 'dataCheck') {
      this.checkModelData({
        modelDesc: modelDesc
      })
    } else if (command === 'dataLoad') {
      this.callModelPartitionDialog({
        modelDesc: modelDesc
      }).then((res) => {
        if (res.isSubmit) {
          this.handleSaveModel(modelDesc)
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
    this.updataModel(modelDesc).then(() => {
      kapMessage(this.$t('kylinLang.common.saveSuccess'))
    }, (res) => {
      handleError(res)
    })
  }
  handleModel (action, modelDesc, successTip) {
    this[action]({modelName: modelDesc.name, project: this.currentSelectedProject}).then(() => {
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
      this.modelArray.push({
        ...item,
        tabTypes: 'first'
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
  toggleShowFull (index) {
    this.showFull = true
    this.activeIndex = index
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
    this.loadModelsList()
  }
}
</script>
<style lang="less">
@import '../../../../assets/styles/variables.less';
.mode-list{
  .model-detail-tabs {
    &>.el-tabs__header{
      margin-bottom:0;
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
        color: #455A64;
        font-size: 28px;
        top: -15px;
        right: -15px;
        position: absolute;
        cursor: pointer;
        z-index: 10;
        &:hover {
          color: @base-color;
        }
      }
    }
  }
  margin-left: 20px;
  margin-right: 20px;
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
              top: 5px;
              right: 5px;
            }
          }
        }
      }
    }
  }
  .el-tabs__nav {
    margin-left: 0;
  }
  .model-json {
    margin: 20px 0;
  }
  .el-tabs__content {
    overflow: initial;
  }
}
</style>
