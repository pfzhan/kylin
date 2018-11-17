<template>
  <div class="mode-list" :class="{'full-cell': showFull}">
    <div class="ky-list-title ksd-mt-20">{{$t('kylinLang.model.modelList')}}</div>
    <div v-if="showSearchResult">
      <div  class="ksd-mb-14 ksd-fright ksd-mt-8">
        <el-input :placeholder="$t('kylinLang.common.pleaseFilterByModelName')" style="width:200px" size="medium" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'" v-model="filterArgs.model"  @input="searchModels" class="show-search-btn" >
        </el-input>
      </div>
      <el-button icon="el-icon-plus" type="primary" size="medium" plain class="ksd-mb-14 ksd-mt-8" id="addModel" v-visible="!isAutoProject && (isAdmin || hasPermissionOfProject())" @click="showAddModelDialog"><span>{{$t('kylinLang.common.model')}}</span></el-button>
      <el-table class="model_list_table"
        :data="modelArray"
        border
        tooltip-effect="dark"
        @sort-change="onSortChange"
        style="width: 100%">
        <el-table-column type="expand" min-width="30">
          <template slot-scope="props">
            <div class="cell-content" :class="{'hidden-cell': props.$index !== activeIndex}">
              <i class="el-icon-ksd-full_screen_1 ksd-fright full-model-box" v-if="!showFull" @click="toggleShowFull(props.$index)"></i>
              <i class="el-icon-ksd-collapse_1 ksd-fright full-model-box" v-else @click="showFull = false"></i>
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
        min-width="229px"
        show-overflow-tooltip
        prop="alias"
          :label="$t('kylinLang.model.modelNameGrid')">
        </el-table-column>
        <el-table-column
          prop="fact_table"
          show-overflow-tooltip
          min-width="229px"
          :label="$t('kylinLang.common.fact')">
        </el-table-column>
        <el-table-column
          prop="favorite"
          show-overflow-tooltip
          width="90px"
          :label="$t('favorite')">
        </el-table-column>
        <el-table-column
          prop="gmtTime"
          show-overflow-tooltip
          sortable="custom"
          width="150px"
          :label="$t('dataLoadTime')">
        </el-table-column>
        <el-table-column
          prop="status"
          show-overflow-tooltip
          width="94"
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
        width="98px"
        :label="$t('kylinLang.common.action')">
          <template slot-scope="scope">
            <span v-if="!(isAdmin || hasPermissionOfProject())"> N/A</span>
             <div v-show="isAdmin || hasPermissionOfProject()">
              <common-tip :content="$t('kylinLang.common.edit')"><i class="el-icon-ksd-table_edit ksd-fs-16" @click="handleEditModel(scope.row.alias)"></i></common-tip>
              <common-tip :content="$t('build')" class="ksd-ml-10"  v-if="scope.row.management_type!=='TABLE_ORIENTED'"><i class="el-icon-ksd-data_range ksd-fs-16" @click="setModelBuldRange(scope.row)"></i></common-tip>
              <common-tip :content="$t('kylinLang.common.moreActions')" class="ksd-ml-10" v-if="!scope.row.is_draft">
                <el-dropdown @command="(command) => {handleCommand(command, scope.row)}" :id="scope.row.name" trigger="click" >
                  <span class="el-dropdown-link" >
                      <i class="el-icon-ksd-table_others ksd-fs-16"></i>
                  </span>
                 <el-dropdown-menu slot="dropdown"  :uuid='scope.row.uuid' >
                    <!-- 数据检测移动至project 级别处理， -->
                    <!-- <el-dropdown-item command="dataCheck">{{$t('datacheck')}}</el-dropdown-item> -->
                    <!-- 设置partition -->
                    <el-dropdown-item command="dataLoad" :disabled="scope.row.management_type==='TABLE_ORIENTED'">{{$t('dataloading')}}</el-dropdown-item>
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
      <p v-if="!isAutoProject && (isAdmin || hasPermissionOfProject())">{{$t('noModel')}}</p>
      <div>
      <el-button size="medium" type="primary" icon="el-icon-plus"  v-if="!isAutoProject && (isAdmin || hasPermissionOfProject())" @click="showAddModelDialog">{{$t('kylinLang.common.model')}}</el-button>
       </div>
    </div>

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
      'briefMenuGet',
      'isAutoProject'
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
  createModelVisible = false
  cloneFormVisible = false
  modelCheckModeVisible = false
  dataRangeVal = []
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
