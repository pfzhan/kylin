<template>
  <div class="mode-list" :class="{'full-cell': showFull}">
    <div class="notice-box" v-if="!!modelSpeedEvents.length">
      <el-alert
      type="warning"
      :closable="false"
      show-icon>
       <span slot="title">Release 2.75 TB storage with only 3% favorite score down! <a @click="applySpeed">Apply</a></span>
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
                  <ModelAggregate :model="props.row" v-if="props.row.tabTypes === 'second'" />
                </el-tab-pane>
                <el-tab-pane label="Table Index" name="third">
                  <TableIndex></TableIndex>
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
                <el-tab-pane label="SQL" name="fifth">
                  <el-input
                    class="model-json"
                    :value="mockSQL"
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
      <!--   <el-table-column
          prop="capacity"
          show-overflow-tooltip
          width="210"
          :label="$t('capbility')">
        </el-table-column> -->
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
        <el-tag size="small" :type="scope.row.status === 'DISABLED' ? 'danger' : scope.row.status === 'DESCBROKEN'? 'info' : 'success'">{{scope.row.status}}</el-tag>
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
              <common-tip :content="$t('dataloading')" class="ksd-ml-10"><i class="el-icon-ksd-data_range ksd-fs-16" @click="dataLoad"></i></common-tip>
              <common-tip :content="$t('kylinLang.common.moreActions')" class="ksd-ml-10" v-if="!scope.row.is_draft">
                <el-dropdown @command="(command) => {handleCommand(command, scope.row)}" :id="scope.row.name" trigger="click" >
                  <span class="el-dropdown-link" >
                      <i class="el-icon-ksd-table_others ksd-fs-16"></i>
                  </span>
                 <el-dropdown-menu slot="dropdown"  :uuid='scope.row.uuid' >
                    <el-dropdown-item command="dataCheck">{{$t('datacheck')}}</el-dropdown-item>
                    <el-dropdown-item command="favorite" disabled>{{$t('favorite')}}</el-dropdown-item>
                    <el-dropdown-item command="importMDX" divided disabled>{{$t('importMdx')}}</el-dropdown-item>
                    <el-dropdown-item command="exportTDS" disabled>{{$t('exportTds')}}</el-dropdown-item>
                    <el-dropdown-item command="exportMDX" disabled>{{$t('exportMdx')}}</el-dropdown-item>
                    <el-dropdown-item command="rename" divided>{{$t('rename')}}</el-dropdown-item>
                    <el-dropdown-item command="clone" >{{$t('kylinLang.common.clone')}}</el-dropdown-item>
                    <el-dropdown-item command="delete">{{$t('delete')}}</el-dropdown-item>
                    <el-dropdown-item command="purge">{{$t('purge')}}</el-dropdown-item>
                    <el-dropdown-item command="disabled" v-if="scope.row.status === 'READY'">{{$t('disable')}}</el-dropdown-item>
                    <el-dropdown-item command="enabled" v-if="scope.row.status === 'DISABLED'" >{{$t('enable')}}</el-dropdown-item>
                    </el-dropdown-menu>
                  </el-dropdown>
                </common-tip>
              </div>
          </template>
        </el-table-column>
      </el-table>


      <kap-pager class="ksd-center ksd-mt-20 ksd-mb-20" ref="pager"  :totalSize="modelsPagerRenderData.totalSize"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>

    </div>
    <div class="ksd-null-pic-text" v-if="!showSearchResult">
      <img src="../../../../assets/img/no_model.png">
      <p v-if="isAdmin || hasPermissionOfProject()">{{$t('noModel')}}</p>
      <div>
      <el-button size="medium" type="primary" icon="el-icon-plus"  v-if="isAdmin || hasPermissionOfProject()" @click="showAddModelDialog">{{$t('kylinLang.common.model')}}</el-button>
       </div>
    </div>

  <el-dialog width="440px" :title="$t('kylinLang.common.notice')" :visible.sync="!!modelSpeedEvents.length" :show-close="false">
    <div>
      Hi Adora,<br/>
      <p style="text-indent:25px; line-height: 26px;">We have a nre proposal to reise model’s favorite score <span class="ky-highlight-text">15%</span>
up with <span class="ky-highlight-text">32.75 GB</span> storage cost. Do you wnat to apply it?</p>
    </div>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="ignoreSpeed">{{$t('ignore')}}</el-button>
      <el-button size="medium" type="primary" plain @click="applySpeed" :loading="btnLoading">{{$t('apply')}}</el-button>
    </div>
  </el-dialog>
  <!-- 模型克隆 -->
<!--   <el-dialog :title="$t('modelClone')" width="440px" :visible.sync="cloneFormVisible" :close-on-press-escape="false" :close-on-click-modal="false">
      <el-form :model="cloneModelMeta" :rules="cloneFormRule" ref="cloneForm" label-width="100px">
        <el-form-item :label="$t('modelName')" prop="newName">
          <el-input v-model="cloneModelMeta.newName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="cloneFormVisible = false" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain :loading="btnLoading" size="medium" @click="cloneModel">{{$t('kylinLang.common.clone')}}</el-button>
      </div>
  </el-dialog> -->


    <!-- 添加model -->
    <el-dialog :title="$t('kylinLang.model.addModel')" width="440px" :visible.sync="createModelVisible" @close="resetAddModelForm">
      <el-form :model="createModelMeta"  :rules="createModelFormRule" ref="addModelForm" label-width="130px" label-position="top">
        <el-form-item prop="modelName" :label="$t('kylinLang.model.modelName')">
          <span slot="label">{{$t('kylinLang.model.modelName')}}
            <common-tip :content="$t('kylinLang.model.modelNameTips')" ><i class="el-icon-question"></i></common-tip>
          </span>
          <el-input v-model="createModelMeta.modelName" auto-complete="off" size="medium"></el-input>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.model.modelDesc')" prop="modelDesc" style="margin-top: 20px;">
         <el-input
            type="textarea"
            :rows="2"
            :placeholder="$t('kylinLang.common.pleaseInput')"
            v-model="createModelMeta.modelDesc">
          </el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="createModelVisible = false" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="createModel" :loading="btnLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>


    <!-- 模型检查 -->
    <el-dialog title="Model Check" width="440px" :visible.sync="modelCheckModeVisible">
      <el-form :model="createModelMeta"  :rules="createModelFormRule" ref="addModelForm" label-width="130px" label-position="top">
        <div class="ky-list-title">数据检查项</div>
        <ul class="ksd-mtb-20">
          <li class="ksd-mb-10"><el-checkbox>模型上主外键重复</el-checkbox></li>
          <li class="ksd-mb-10"><el-checkbox>数据倾斜（偏度过高）</el-checkbox></li>
          <li class="ksd-mb-10"><el-checkbox>字段中存在空值</el-checkbox></li>
        </ul>
        <div class="ky-line"></div>
        <div class="ky-list-title ksd-mt-20">数据容忍标准</div>
        <div class="ksd-mt-20">
          有数据问题超过<el-input size="mini" style="width:70px;" class="ksd-mrl-4"></el-input>条时，采取以下方式：
        </div>
        <div class="ksd-mt-16">
          <ul>
            <li class="ksd-mb-10"><el-radio>模型上主外键重复</el-radio></li>
            <li class="ksd-mb-10"><el-radio>数据倾斜（偏度过高）</el-radio></li>
            <li><el-radio>字段中存在空值</el-radio></li>
          </ul>
        </div>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="modelCheckModeVisible = false" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="createModel" :loading="btnLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>


    <!--  数据加载 -->
    <el-dialog title="数据加载管理"  width="660px" :visible.sync="dataLoadingModeVisible">
      <div>
        <div class="ky-list-title">分区设置</div>
        <div class="ky-list-sub-title">一级分区</div>
        <el-form :inline="true"  class="demo-form-inline">
          <el-form-item label="表">
            <el-select  placeholder="请选择表">
              <el-option label="1" value="shanghai"></el-option>
              <el-option label="2" value="beijing"></el-option>
            </el-select>
          </el-form-item>
          <el-form-item label="列">
            <el-select  placeholder="请选择列">
              <el-option label="1" value="shanghai"></el-option>
              <el-option label="2" value="beijing"></el-option>
            </el-select>
          </el-form-item>
        </el-form>
        <div class="ky-list-sub-title ksd-mt-2">时间分区</div>
        <el-form :inline="true"  class="demo-form-inline">
          <el-form-item label="表">
            <el-select  placeholder="表">
              <el-option label="1" value="shanghai"></el-option>
              <el-option label="2" value="beijing"></el-option>
            </el-select>
          </el-form-item>
          <el-form-item label="列">
            <el-select  placeholder="列">
              <el-option label="1" value="1"></el-option>
              <el-option label="2" value="2"></el-option>
            </el-select>
          </el-form-item>
        </el-form>
        <div class="ky-line"></div>
        <div class="ky-list-title ksd-mt-20">数据更新</div>
        <div class="ky-list-sub-title">维度表更新</div>
        <div class="ksd-mt-14">
          <el-radio label="1">与数据源更新同步</el-radio>
          <el-radio label="2">自定义更新时间</el-radio>
        </div>
        <div class="ky-list-sub-title">事实表更新</div>
        <div class="ksd-mt-14">
          <el-radio label="1">与数据源更新同步</el-radio>
          <el-radio label="2">自定义更新时间</el-radio>
        </div>
      </div>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dataLoadingModeVisible = false" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="createModel" :loading="btnLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>
    <ModelRenameModal/>
    <ModelCloneModal/>
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
import { mockSQL } from './mock'
import './fly.js'
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'modelsPagerRenderData',
      'briefMenuGet'
    ]),
    modelSpeedEvents () {
      return this.$store.state.model.modelSpeedEvents
    }
  },
  methods: {
    ...mapActions({
      loadModels: 'LOAD_MODEL_LIST',
      cloneModel: 'CLONE_MODEL',
      delModel: 'DELETE_MODEL',
      checkModelName: 'CHECK_MODELNAME',
      applySpeedInfo: 'APPLY_SPEED_INFO',
      purgeModel: 'PURGE_MODEL',
      disableModel: 'DISABLE_MODEL',
      enableModel: 'ENABLE_MODEL',
      getSpeedInfo: 'GET_SPEED_INFO',
      ignoreSpeedInfo: 'IGNORE_SPEED_INFO'
    }),
    ...mapActions('ModelRenameModal', {
      callRenameModelDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelCloneModal', {
      callCloneModelDialog: 'CALL_MODAL'
    })
  },
  components: {
    TableIndex,
    ModelSegment,
    ModelAggregate,
    ModelRenameModal,
    ModelCloneModal
  },
  locales
})
export default class ModelList extends Vue {
  mockSQL = mockSQL
  applyDialogVisible = false
  createModelVisible = false
  cloneFormVisible = false
  modelCheckModeVisible = false
  dataLoadingModeVisible = false
  btnLoading = false
  createModelFormRule = {
    modelName: [
      {required: true, message: this.$t('inputModelName'), trigger: 'blur'},
      {validator: this.checkName, trigger: 'blur'}
    ]
  }
  cloneFormRule = {
    newName: [
      {required: true, message: this.$t('inputCloneName'), trigger: 'blur'},
      {validator: this.checkName, trigger: 'blur'}]
  }
  createModelMeta = {
    modelName: '',
    modelDesc: ''
  }
  cloneModelMeta = {
    newName: '',
    oldName: '',
    project: ''
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
  loadSpeedInfo () {
    this.getSpeedInfo(this.currentSelectedProject).then(() => {
      this.btnLoading = false
    }, (res) => {
      this.btnLoading = false
      handleError(res)
    })
  }
  applySpeed (event) {
    var eventId = []
    this.modelSpeedEvents.forEach((ev) => {
      eventId.push(ev.uuid)
    })
    this.btnLoading = true
    this.applySpeedInfo({size: this.modelSpeedEvents.length, project: this.currentSelectedProject}).then(() => {
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
    this.btnLoading = true
    this.ignoreSpeedInfo(this.currentSelectedProject).then(() => {
      this.loadSpeedInfo()
    }, (res) => {
      this.btnLoading = false
      handleError(res)
    })
  }
  flyEvent (event) {
    var targetArea = $('#monitor')
    var targetDom = targetArea.find('.menu-icon')
    var offset = targetDom.offset()
    var flyer = $('<span class="fly-box"></span>')
    let leftOffset = 60
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
  dataLoad () {
    this.dataLoadingModeVisible = true
  }
  async handleCommand (command, modelInstance) {
    if (command === 'dataCheck') {
      this.modelCheckModeVisible = true
    } else if (command === 'rename') {
      const isSubmit = await this.callRenameModelDialog(objectClone(modelInstance))
      isSubmit && this.loadModelsList()
    } else if (command === 'delete') {
      kapConfirm(this.$t('delModelTip')).then(() => {
        this.handleDrop(modelInstance)
      })
    } else if (command === 'purge') {
      kapConfirm(this.$t('pergeModelTip')).then(() => {
        this.handlePurge(modelInstance)
      })
    } else if (command === 'clone') {
      const isSubmit = await this.callCloneModelDialog(objectClone(modelInstance))
      isSubmit && this.loadModelsList()
    } else if (command === 'disabled') {
      kapConfirm(this.$t('disbaleModelTip')).then(() => {
        this.handleDisableModel(objectClone(modelInstance))
      })
    } else if (command === 'enabled') {
      kapConfirm(this.$t('enableModelTip')).then(() => {
        this.handleEnableModel(objectClone(modelInstance))
      })
    }
  }
  handleDisableModel (modelInstance) {
    let model = objectClone(modelInstance)
    model.status = 'DISABLED'
    this.disableModel({modelName: modelInstance.name, project: this.currentSelectedProject}).then(() => {
      kapMessage(this.$t('disbaleModelSuccessTip'))
      this.loadModelsList()
    }, (res) => {
      handleError(res)
    })
  }
  handleEnableModel (modelInstance) {
    this.enableModel({modelName: modelInstance.name, project: this.currentSelectedProject}).then(() => {
      kapMessage(this.$t('enabledModelSuccessTip'))
      this.loadModelsList()
    }, (res) => {
      handleError(res)
    })
  }
  handleEdit (model) {
    this.$emit('addtabs', 'model', model.name, 'modelEdit', {
      project: model.project,
      modelName: model.name,
      uuid: model.uuid,
      status: model.is_draft
    })
  }
  handleDrop (model) {
    this.delModel({modelName: model.name, project: this.currentSelectedProject}).then(() => {
      kapMessage(this.$t('deleteModelSuccessTip'))
      this.loadModelsList()
    }, (res) => {
      handleError(res)
    })
  }
  handlePurge (model) {
    this.purgeModel({project: this.currentSelectedProject, modelName: model.name}).then(() => {
      kapMessage(this.$t('purgeModelSuccessTip'))
    }, (res) => {
      handleError(res)
    })
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
  // 全凭查看模型附属信息
  toggleShowFull (index) {
    this.showFull = true
    this.activeIndex = index
  }
  handleEditModel (modelName) {
    this.$router.push({ name: 'ModelEdit', params: { modelName: modelName, action: 'edit' } })
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
    this.createModelVisible = true
  }
  createModel () {}
  resetAddModelForm () {}
  cloneModel () {}
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
@range: 2160deg;
@timer:3s;
.rotateY{
  -webkit-transform: rotate3d(0,1,0,@range);
  -moz-transform: rotate3d(0,1,0,@range);
  transform: rotate3d(0,1,0,@range);
  transition: -webkit-transform @timer ease-out;
  transition: -moz-transform @timer ease-out;
  transition: transform @timer ease-out;
}
.fly-box {
  position:absolute;
  width:14px;
  height:14px;
  border-radius: 50%;
  background-color:@error-color-1;
  z-index:99999;
}
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
