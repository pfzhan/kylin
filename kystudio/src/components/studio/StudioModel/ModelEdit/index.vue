<template>
  <div class="model-edit-outer" @drop='dropTable($event)' v-drag="{sizeChangeCb:dragBox}">
    <div class="model-edit"  @dragover='allowDrop($event)' @dragleave="dragLeave">
      <!-- table box -->
      <div class="table-box" :id="t.guid" v-event-stop v-if="modelRender && modelRender.tables" v-for="t in modelRender && modelRender.tables || []" :key="t.guid" :style="tableBoxStyle(t.drawSize)">
        <div class="table-title"  @mousedown="activeTablePanel(t)" :data-zoom="modelRender.zoom"  v-drag:change.left.top="t.drawSize" :class="{isLookup:t.kind==='LOOKUP'}">
          <el-input v-show="t.aliasIsEdit" v-focus="t.aliasIsEdit" v-event-stop v-model="t.alias" @blur="saveNewAlias(t)" @keyup.enter="saveNewAlias(t)"></el-input>
          <span v-show="!t.aliasIsEdit" @click.stop="changeTableType(t)">
            <i class="el-icon-ksd-fact_table kind" v-if="t.kind==='FACT'"></i>
            <i v-else class="el-icon-ksd-lookup_table kind"></i>
          </span>
          <common-tip class="name" v-show="!t.aliasIsEdit">
            <span slot="content">{{t.alias}} <i class="el-icon-ksd-table_edit" v-show="t.kind!=='FACT'" @click="editAlias(t)"></i></span>
            <span>{{t.alias}}</span>
          </common-tip>
          <span v-show="!t.aliasIsEdit" class="close" @click="delTable(t.guid)"><i class="el-icon-ksd-close"></i></span>
        </div>
        <div class="column-list-box" @dragover='($event) => {allowDropColumn($event, t.guid)}' v-event-stop @drop='(e) => {dropColumn(e, null, t)}' v-scroll>
          <ul >
            <li class="column-li" @drop='(e) => {dropColumn(e, col, t)}' @dragstart="(e) => {dragColumns(e, col, t)}"  draggable v-for="col in t.columns" :key="col.name">
              <span class="col-name">{{col.name|omit(14,'...')}}</span>
              <span class="col-type">{{col.datatype}}</span>
            </li>
          </ul>
        </div>
        <div class="drag-bar" v-drag:change.height="t.drawSize" :data-zoom="modelRender.zoom"><i class="el-icon-ksd-bottom_bar"></i></div>
      </div>
      <!-- table box end -->
    </div>
    <DimensionModal/>
    <TableJoinModal/>
    <AddMeasure
      :isShow="measureVisible"
      :isEditMeasure="isEditMeasure"
      :measureObj="measureObj"
      :modelInstance="modelInstance"
      v-on:closeAddMeasureDia="closeAddMeasureDia">
    </AddMeasure>
    <SingleDimensionModal/>
    <!-- datasource面板  index 3-->
    <div class="tool-icon icon-ds" :class="{active: panelAppear.datasource.display}" @click.stop="toggleMenu('datasource')"><i class="el-icon-ksd-data_source"></i></div>
      <transition name="bounceleft">
        <div class="panel-box panel-datasource" v-show="panelAppear.datasource.display" :style="panelStyle('datasource')" v-event-stop>
          <div class="panel-title"><span class="title">{{$t('kylinLang.common.dataSource')}}</span><span class="close" @click="toggleMenu('datasource')"><i class="el-icon-ksd-close"></i></span></div>
          <div v-scroll style="height:calc(100% - 50px)">
            <DataSourceBar 
              class="tree-box"
              :project-name="currentSelectedProject"
              :is-show-load-source="true"
              :is-show-settings="false"
              :is-show-action-group="false"
              :is-expand-on-click-node="false"
              :expand-node-types="['datasource', 'database']"
              :draggable-node-types="['table']"
              :searchable-node-types="['table']"
              @drag="dragTable">
            </DataSourceBar>
          </div>
        </div>
      </transition>
      <!-- datasource面板  end-->
      <div class="tool-icon icon-lock-status" :class="{'unlock-icon': autoSetting}" v-event-stop>
        <common-tip class="name" :content="$t('avoidSysChange')" placement="left" v-if="autoSetting"><i class="el-icon-ksd-lock" v-if="autoSetting"></i></common-tip>
        <common-tip class="name" :content="$t('allowSysChange')" placement="left" v-else><i class="el-icon-ksd-unlock"></i></common-tip>
      </div>
      <div class="tool-icon-group" v-event-stop>
        <div class="tool-icon" :class="{active: panelAppear.dimension.display}" @click="toggleMenu('dimension')">D</div>
        <div class="tool-icon" :class="{active: panelAppear.measure.display}" @click="toggleMenu('measure')">M</div>
        <!-- <div class="tool-icon" :class="{active: panelAppear.setting.display}" @click="toggleMenu('setting')"><i class="el-icon-ksd-setting"></i></div> -->
        <div class="tool-icon" :class="{active: panelAppear.search.display}" @click="toggleMenu('search')">
          <i class="el-icon-ksd-search"></i>
          <span class="new-icon">New</span>
        </div>
      </div>
      <div class="sub-tool-icon-group" v-event-stop>
        <div class="tool-icon" @click="reduceZoom"><i class="el-icon-ksd-shrink" ></i></div>
        <div class="tool-icon" @click="addZoom"><i class="el-icon-ksd-enlarge"></i></div>
        <!-- <div class="tool-icon" v-event-stop>{{modelRender.zoom}}0%</div> -->
        <div class="tool-icon" @click="fullScreen"><i class="el-icon-ksd-full_screen_2" v-if="!isFullScreen"></i><i class="el-icon-ksd-collapse_2" v-if="isFullScreen"></i></div>
        <div class="tool-icon" @click="autoLayout"><i class="el-icon-ksd-auto"></i></div>
      </div>
      <!-- 右侧面板组 -->
      <!-- <div class="panel-group"> -->
        <!-- dimension面板  index 0-->
        <transition name="bounceright">
          <div class="panel-box panel-dimension" @mousedown.stop="activePanel('dimension')" :style="panelStyle('dimension')" v-if="panelAppear.dimension.display">
            <div class="panel-title" @mousedown="activePanel('dimension')" v-drag:change.right.top="panelAppear.dimension">
              <span><i class="el-icon-ksd-dimension"></i></span>
              <span class="title">{{$t('kylinLang.common.dimension')}}</span>
              <span class="close" @click="toggleMenu('dimension')"><i class="el-icon-ksd-close"></i></span>
            </div>
            <div class="panel-sub-title">
              <span @click="batchSetDimension"><i class="el-icon-ksd-batch"></i></span>
              <span @click="addCCDimension"><i class="el-icon-ksd-project_add"></i></span>
              <span><i class="el-icon-ksd-table_delete" @click="deleteDimenisons"></i></span>
            </div>
            <div class="panel-main-content" v-scroll>
              <ul class="dimension-list">
                <li v-for="(d, i) in allDimension" :key="d.name">
                  <el-checkbox v-model="dimensionSelectedList" :label="i">{{d.name|omit(18,'...')}}</el-checkbox>
                  <i class="el-icon-ksd-table_edit" @click="editDimension(d, i)"></i>
                  <i class="el-icon-ksd-table_delete" @click="deleteDimenison(i)"></i>
                  <span class="li-type ky-option-sub-info">{{d.datatype}}</span>
                </li>
              </ul>
            </div>
            <div class="panel-footer" v-drag:change.height="panelAppear.dimension"><i class="el-icon-ksd-bottom_bar"></i></div>
          </div>
        </transition>
        <!-- measure面板  index 1-->
        <transition name="bounceright">
          <div class="panel-box panel-measure" @mousedown.stop="activePanel('measure')" :style="panelStyle('measure')"  v-if="panelAppear.measure.display">
            <div class="panel-title" @mousedown="activePanel('measure')" v-drag:change.right.top="panelAppear.measure">
              <span><i class="el-icon-ksd-measure"></i></span>
              <span class="title">{{$t('kylinLang.common.measure')}}</span>
              <span class="close" @click="toggleMenu('measure')"><i class="el-icon-ksd-close"></i></span>
            </div>
            <div class="panel-sub-title">
              <span><i class="el-icon-ksd-project_add" @click="addNewMeasure"></i></span>
              <span><i class="el-icon-ksd-table_delete" @click="deleteMeasures"></i></span>
            </div>
            <div class="panel-main-content" v-scroll>
              <ul class="measure-list">
                <li v-for="(m, i) in modelRender.all_measures" :key="m.name">
                <el-checkbox v-model="measureSelectedList" :label="i">{{m.name|omit(18,'...')}}</el-checkbox>
                  <i class="el-icon-ksd-table_edit" @click="editMeasure(m)" v-if="m.name !== 'COUNT_ALL'"></i>
                  <i class="el-icon-ksd-table_delete" @click="deleteMeasure(i)" v-if="m.name !== 'COUNT_ALL'"></i>
                  <span class="li-type ky-option-sub-info">{{m.return_type}}</span>
                </li>
              </ul>
            </div>
            <div class="panel-footer" v-drag:change.height="panelAppear.measure"><i class="el-icon-ksd-bottom_bar"></i></div>
          </div>
        </transition>
        <!-- setting面板  index 2-->
        <transition name="bounceright">
          <div class="panel-box panel-setting" @mousedown.stop="activePanel('setting')" :style="panelStyle('setting')" v-if="panelAppear.setting.display">
            <div class="panel-title" @mousedown="activePanel('setting')" v-drag:change.right.top="panelAppear.setting">
              <span><i class="el-icon-ksd-setting"></i></span>
              <span class="title">{{$t('modelSetting')}}</span>
              <span class="close" @click="toggleMenu('setting')"><i class="el-icon-ksd-close"></i></span>
            </div>
            <div class="panel-main-content" style="top:36px;" data-scrollbar>
              <div :class="{active:autoSetting}">
                <div><el-radio v-model="modelRender.maintain_model_type" label="MANUAL_MAINTAIN">{{$t('userMaintainedModel')}}<i class="el-icon-ksd-lock ksd-ml-4"></i></el-radio></div>
                <ul>
                  <li>{{$t('userMaintainedTip1')}}</li>
                  <li>{{$t('userMaintainedTip2')}}</li>
                  <li>{{$t('userMaintainedTip3')}}</li>
                </ul>
              </div>
              <div :class="{active:!autoSetting}">
                <div><el-radio v-model="modelRender.maintain_model_type" label="AUTO_MAINTAIN">{{$t('systemMaintainedModel')}}<i class="el-icon-ksd-unlock ksd-ml-4"></i></el-radio></div>
                <ul>
                  <li>{{$t('systemMaintainedTip1')}}</li>
                  <li>{{$t('systemMaintainedTip2')}}</li>
                  <li>{{$t('systemMaintainedTip3')}}</li>
                </ul>
              </div>
            </div>
          </div>
        </transition>
      <!-- </div> -->
      <!-- 右侧面板组end -->

    <!-- 搜索面板 -->
    <transition name="bouncecenter">
     <div class="panel-search-box panel-box"  v-event-stop :style="panelStyle('search')" v-if="panelAppear.search.display">
      <span class="close" @click="toggleMenu('search')"><i class="el-icon-ksd-close"></i></span>
       <el-input @input="searchModelEverything"  clearable class="search-input" placeholder="search table, dimension, measure, column name" v-model="modelGlobalSearch" prefix-icon="el-icon-search"></el-input>
       <transition name="bounceleft">
       <div v-scroll class="search-result-box" v-keyborad-select="{scope:'.search-content', searchKey: modelGlobalSearch}" v-show="modelGlobalSearch && showSearchResult" v-search-highlight="{scope:'.search-name', hightlight: modelGlobalSearch}">
        <div>
         <div class="search-group" v-for="(k,v) in searchResultData" :key="v">
           <ul>
             <li class="search-content" v-for="(x, i) in k" @click="(e) => {selectResult(e, x)}" :key="x.action + x.name + i"><span class="search-category">[{{$t(x.i18n)}}]</span> <span class="search-name">{{x.name}}</span><span v-html="x.extraInfo"></span></li>
           </ul>
           <div class="ky-line"></div>
         </div>
         <div v-show="Object.keys(searchResultData).length === 0" class="search-noresult">No Result!</div>
       </div>
     </div>
     </transition>
    </div>
    </transition> 
    <PartitionModal/>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters, mapMutations, mapState } from 'vuex'
import locales from './locales'
import DataSourceBar from '../../../common/DataSourceBar'
import { handleSuccess, handleError, loadingBox, kapMessage, kapConfirm } from '../../../../util/business'
import { isIE, groupData, objectClone } from '../../../../util'
import $ from 'jquery'
import DimensionModal from '../DimensionsModal/index.vue'
import AddMeasure from '../AddMeasure/index.vue'
import TableJoinModal from '../TableJoinModal/index.vue'
import SingleDimensionModal from '../SingleDimensionModal/addDimension.vue'
import PartitionModal from '../ModelList/ModelPartitionModal/index.vue'
import NModel from './model.js'
import { modelRenderConfig, modelErrorMsg } from './config'
@Component({
  props: ['extraoption'],
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'isFullScreen'
    ]),
    ...mapState('TableJoinModal', {
      tableJoinDialogShow: state => state.isShow
    }),
    ...mapState('SingleDimensionModal', {
      singleDimensionDialogShow: state => state.isShow
    }),
    ...mapState('DimensionsModal', {
      dimensionDialogShow: state => state.isShow
    })
  },
  methods: {
    ...mapActions({
      getModelByModelName: 'LOAD_MODEL_INFO',
      loadDataSourceByProject: 'LOAD_DATASOURCE',
      saveModel: 'SAVE_MODEL',
      updataModel: 'UPDATE_MODEL'
    }),
    ...mapActions('DimensionsModal', {
      showDimensionDialog: 'CALL_MODAL'
    }),
    ...mapActions('TableJoinModal', {
      showJoinDialog: 'CALL_MODAL'
    }),
    ...mapActions('SingleDimensionModal', {
      showSingleDimensionDialog: 'CALL_MODAL'
    }),
    ...mapActions('ModelPartitionModal', {
      showPartitionDialog: 'CALL_MODAL'
    }),
    ...mapMutations({
      toggleFullScreen: 'TOGGLE_SCREEN'
    })
  },
  components: {
    DataSourceBar,
    AddMeasure,
    DimensionModal,
    TableJoinModal,
    SingleDimensionModal,
    PartitionModal
  },
  locales
})
export default class ModelEdit extends Vue {
  datasource = []
  modelRender = {tables: {}}
  dimensionSelectedList = []
  measureSelectedList = []
  modelInstance = null // 模型实例对象
  currentDragTable = '' // 当前拖拽的表
  currentDragColumn = '' // 当前拖拽的列
  currentDropColumnData = {} // 当前释放到的列
  currentDragColumnData = {} // 当前拖拽列携带信息
  modelGlobalSearch = '' // model全局搜索信息
  showSearchResult = true
  modelGlobalSearchResult = []
  modelData = {}
  globalLoading = loadingBox()
  renderBox = modelRenderConfig.drawBox
  measureVisible = false
  isEditMeasure = false
  allColumns = []
  // baseIndex = modelRenderConfig.baseIndex
  autoSetting = true
  measureObj = {
    name: '',
    expression: 'SUM(column)',
    parameter_value: [{type: 'column', value: '', table_guid: null}],
    converted_columns: [],
    return_type: ''
  }
  panelAppear = modelRenderConfig.pannelsLayout()
  radio = 1
  get allDimension () {
    return this.modelRender.dimensions
  }
  query (className) {
    return $(this.$el.querySelector(className))
  }
  delTable (guid) {
    kapConfirm(this.$t('delTableTip')).then(() => {
      this.modelInstance.delTable(guid)
    })
    // this.modelInstance.delTable(guid).then(() => {
    // }, () => {
    //   kapMessage(this.$t('delTableTip'), {type: 'warning'})
    // })
  }
  editAlias (t) {
    this.$set(t, 'aliasIsEdit', true)
  }
  saveNewAlias (t) {
    this.modelInstance.setUniqueAlias(t)
    this.$set(t, 'aliasIsEdit', false)
    this.modelInstance.changeAlias()
  }
  // 切换悬浮菜单
  toggleMenu (i) {
    this.panelAppear[i].display = !this.panelAppear[i].display
    if (this.panelAppear[i].display) {
      this.activePanel(i)
    }
  }
  activePanel (i) {
    var curPanel = this.panelAppear[i]
    this.modelInstance.setIndexTop(Object.values(this.panelAppear), curPanel, '')
  }
  activeTablePanel (t) {
    this.modelInstance.setIndexTop(Object.values(this.modelRender.tables), t, 'drawSize')
  }
  closeAddMeasureDia () {
    this.measureVisible = false
  }
  changeTableType (t) {
    if (this._checkTableType(t)) {
      this.modelInstance.changeTableType(t)
    }
  }
  _checkTableType (t) {
    if (t.fact) {
      // 提示 增量构建的不能改成lookup
      return false
    }
    if (Object.keys(t.joinInfo).length && t.kind === modelRenderConfig.tableKind.lookup) {
      // 提示，主键表不能作为fact
      return false
    }
    return true
  }
  // 放大视图
  addZoom (e) {
    this.modelInstance.addZoom()
  }
  // 缩小视图
  reduceZoom (e) {
    this.modelInstance.reduceZoom()
  }
  // 全屏
  fullScreen () {
    this.toggleFullScreen(!this.isFullScreen)
  }
  // 自动布局
  autoLayout () {
    this.modelInstance.renderPosition()
  }
  initModelDesc (cb) {
    if (this.extraoption.modelName && this.extraoption.action === 'edit') {
      this.getModelByModelName({model: this.extraoption.modelName, project: this.extraoption.project}).then((response) => {
        handleSuccess(response, (data) => {
          if (data.models && data.models.length) {
            this.modelData = data.models[0]
            this.modelData.project = this.currentSelectedProject
            cb(this.modelData)
            this.globalLoading.hide()
          }
        })
      }, () => {
        this.globalLoading.hide()
      })
    } else if (this.extraoption.action === 'add') {
      this.modelData = {
        name: this.extraoption.modelName,
        description: this.extraoption.modelDesc,
        project: this.currentSelectedProject
      }
      cb(this.modelData)
      this.globalLoading.hide()
    }
  }
  batchSetDimension () {
    this.showDimensionDialog({
      modelDesc: this.modelRender
    })
  }
  addCCDimension () {
    this.allColumns = this.modelInstance.getTableColumns()
    this.showSingleDimensionDialog({
      modelInstance: this.modelInstance
    })
  }
  addNewMeasure () {
    this.measureVisible = true
    this.isEditMeasure = false
    this.measureObj = {
      name: '',
      expression: 'SUM(column)',
      parameter_value: [{type: 'column', value: '', table_guid: null}],
      converted_columns: [],
      return_type: ''
    }
  }
  saveMeasure (measureObj, ccObj, isEdit) {
    if (isEdit) {
      this.modelInstance.editMeasure(measureObj)
      this.modelInstance.addCC(ccObj)
    } else {
      this.modelInstance.addMeasure(measureObj)
      this.modelInstance.addCC(ccObj)
    }
    this.measureVisible = false
  }
  editDimension (dimension, i) {
    dimension._id = i
    this.showSingleDimensionDialog({
      dimension: objectClone(dimension),
      modelInstance: this.modelInstance
    })
  }
  deleteDimenison (i) {
    let indexInSelected = this.dimensionSelectedList.indexOf(i)
    if (indexInSelected >= 0) {
      this.dimensionSelectedList.splice(indexInSelected, 1)
    }
    this.dimensionSelectedList = this.dimensionSelectedList.map((k) => {
      if (k > i) {
        return k - 1
      }
      return k
    })
    this.modelInstance.delDimension(i)
  }
  // 批量删除
  deleteDimenisons () {
    this.dimensionSelectedList.sort((a, b) => b - a)
    this.dimensionSelectedList && this.dimensionSelectedList.forEach((i) => {
      this.deleteDimenison(i)
    })
    this.dimensionSelectedList = []
  }
  deleteMeasure (i) {
    let indexInSelected = this.measureSelectedList.indexOf(i)
    if (indexInSelected >= 0) {
      this.measureSelectedList.splice(indexInSelected, 1)
    }
    this.measureSelectedList = this.measureSelectedList.map((k) => {
      if (k > i) {
        return k - 1
      }
      return k
    })
    this.modelInstance.delMeasure(i)
  }
  deleteMeasures () {
    this.measureSelectedList.sort((a, b) => b - a)
    this.measureSelectedList && this.measureSelectedList.forEach((i) => {
      this.deleteMeasure(i)
    })
    this.measureSelectedList = []
  }
  editMeasure (m) {
    this.$nextTick(() => {
      this.measureObj = m
    })
    this.measureVisible = true
    this.isEditMeasure = true
  }
  // 拖动画布
  dragBox (x, y) {
    this.modelInstance.moveModelPosition(x, y)
  }
  // 拖动tree-table
  dragTable (node) {
    this.currentDragTable = node.database + '.' + node.label
  }
  // 拖动列
  dragColumns (event, col, table) {
    event.stopPropagation()
    this.currentDragColumn = event.srcElement ? event.srcElement : event.target
    event.dataTransfer && (event.dataTransfer.effectAllowed = 'move')
    if (!isIE()) {
      event.dataTransfer && event.dataTransfer.setData && event.dataTransfer.setData('text', this.currentDragColumn.innerHTML)
    }
    event.dataTransfer.setDragImage && event.dataTransfer.setDragImage(this.currentDragColumn, 0, 0)
    this.currentDragColumnData = {
      guid: table.guid,
      columnName: col.name,
      btype: col.btype
    }
    return true
  }
  // 释放table
  dropTable (e) {
    e.preventDefault && e.preventDefault()
    e.stopPropagation && e.stopPropagation()
    var target = e.srcElement ? e.srcElement : e.target
    if (!this.currentDragTable) {
      return
    }
    if (target.className.indexOf(modelRenderConfig.drawBox.substring(1)) >= 0) {
      this.modelInstance.addTable({
        table: this.currentDragTable,
        alias: this.currentDragTable.split('.')[1],
        drawSize: {
          left: e.offsetX,
          top: e.offsetY
        }
      })
    }
    this.currentDragTableData = {}
    this.currentDragTable = null
    this.removeDragInClass()
  }
  // 释放列
  dropColumn (event, col, table) {
    this.removeDragInClass()
    // 判断是否是自己连自己
    if (this.currentDragColumnData.guid === table.guid) {
      return
    }
    // 判断是否是把fact当主键表（连向fact）
    if (table.kind === modelRenderConfig.tableKind.fact) {
      return
    }
    // 判断连接的表是不是已经已经做了别人的主键
    var curTableLinkTable = table.getJoinInfo() && table.getJoinInfo().foreignTable || null
    if (curTableLinkTable && curTableLinkTable.guid !== this.currentDragColumnData.guid) {
      return
    }
    if (this.currentDragColumnData.guid) {
      this.currentDropColumnData = {
        guid: table.guid,
        columnName: col && col.name || ''
      }
    }
    let fTable = this.modelRender.tables[this.currentDragColumnData.guid]
    this.callJoinDialog({
      fid: this.currentDragColumnData.guid,
      pid: table.guid,
      fColumnName: fTable.alias + '.' + this.currentDragColumnData.columnName,
      tables: this.modelRender.tables
    })
  }
  callJoinDialog (data) {
    // 弹出框弹出
    this.showJoinDialog(data).then(({isSubmit, data}) => {
      // 保存的回调
      if (isSubmit) {
        this.saveLinkData(data)
      }
    })
  }
  saveLinkData (data) {
    var pGuid = data.selectP
    var fGuid = data.selectF
    var joinData = data.joinData
    var joinType = data.joinType
    var fcols = joinData.foreign_key
    var pcols = joinData.primary_key
    var fTable = this.modelInstance.tables[fGuid]
    var pTable = this.modelInstance.tables[pGuid]
    // 给table添加连接数据
    pTable.addLinkData(fTable, fcols, pcols, joinType)
    this.currentDragColumnData = {}
    this.currentDropColumnData = {}
    this.currentDragColumn = null
    // 渲染下连线
    this.modelInstance.renderLink(pGuid, fGuid)
  }
  removeDragInClass () {
    $(this.$el).removeClass('drag-in').find('.drag-in').removeClass('drag-in')
  }
  _checkTableDropOver (className) {
    return className.indexOf(modelRenderConfig.drawBox.substring(1)) >= 0
  }
  allowDrop (e, guid) {
    e.preventDefault()
    var target = e.srcElement ? e.srcElement : e.target
    if (this.currentDragTable && this._checkTableDropOver(target.className)) {
      $(this.$el).addClass('drag-in')
    }
  }
  allowDropColumn (e, guid) {
    e.preventDefault()
    var target = e.srcElement ? e.srcElement : e.target
    if (this.currentDragColumn) {
      if (this.currentDragColumnData.guid === guid) {
        return
      }
      $(target).parents('.column-list-box').addClass('drag-in')
    }
  }
  dragLeave (e) {
    e.preventDefault()
    this.removeDragInClass()
  }
  searchHandleStart = false // 标识业务弹窗是不是通过搜索弹出的
  selectResult (e, select) {
    this.searchHandleStart = true
    this.showSearchResult = false
    this.modelGlobalSearch = ''
    var moreInfo = select.more
    if (select.action === 'showtable') {
      if (select.more) {
        let nTable = select.more
        let offset = nTable.getTableInViewOffset()
        this.modelInstance.moveModelPosition(offset.x, offset.y)
      }
    }
    if (select.action === 'tableeditjoin') {
      let pguid = moreInfo.guid
      let joinInfo = moreInfo.getJoinInfo()
      let fguid = joinInfo.foreignTable.guid
      this.callJoinDialog({
        pid: pguid,
        fid: fguid,
        tables: this.modelRender.tables
      })
    }
    if (select.action === 'tableaddjoin') {
      let pguid = moreInfo.guid
      this.callJoinDialog({
        fid: '',
        pid: pguid,
        tables: this.modelRender.tables
      })
    }
    if (select.action === 'adddimension') {
      this.showSingleDimensionDialog({
        dimension: {
          column: moreInfo.full_colname
        },
        modelInstance: this.modelInstance
      })
    }
    if (select.action === 'editdimension') {
      this.showSingleDimensionDialog({
        dimension: moreInfo,
        modelInstance: this.modelInstance
      })
    }
    if (select.action === 'editjoin') {
      let pguid = moreInfo.guid
      let joinInfo = moreInfo.getJoinInfo()
      let fguid = joinInfo.foreignTable.guid
      this.callJoinDialog({
        pid: pguid,
        fid: fguid,
        tables: this.modelRender.tables
      })
    }
    if (select.action === 'addmeasure') {
      this.measureObj = {
        name: '',
        expression: 'SUM(column)',
        parameter_value: [{type: 'column', value: moreInfo.full_colname, table_guid: null}],
        converted_columns: [],
        return_type: ''
      }
      this.measureVisible = true
      this.isEditMeasure = false
    }
    if (select.action === 'editmeasure') {
      this.measureObj = moreInfo
      this.measureVisible = true
      this.isEditMeasure = true
    }
    if (select.action === 'addjoin') {
      let pguid = moreInfo.guid
      this.callJoinDialog({
        foreignTable: this.modelRender.tables[pguid],
        primaryTable: {},
        tables: this.modelRender.tables,
        ftableName: moreInfo.name
      })
    }
    this.panelAppear.search.display = false
  }
  @Watch('dimensionDialogShow')
  @Watch('singleDimensionDialogShow')
  @Watch('tableJoinDialogShow')
  @Watch('measureVisible')
  tableJoinDialogClose (val) {
    if (!val) {
      if (this.searchHandleStart) {
        this.searchHandleStart = false
        this.panelAppear.search.display = true
      }
    }
  }
  searchModelEverything (val) {
    this.modelGlobalSearchResult = this.modelInstance.search(val)
  }
  getColumnType (tableName, column) {
    var ntable = this.modelInstance.getTable('alias', tableName)
    return ntable && ntable.getColumnType(column)
  }
  @Watch('modelGlobalSearch')
  watchSearch (v) {
    this.showSearchResult = true
  }
  get panelStyle () {
    return (k) => {
      var styleObj = {'z-index': this.panelAppear[k].zIndex, width: this.panelAppear[k].width + 'px', height: this.panelAppear[k].height + 'px', right: this.panelAppear[k].right + 'px', top: this.panelAppear[k].top + 'px'}
      if (this.panelAppear[k].left) {
        styleObj.left = this.panelAppear[k].left + 'px'
      }
      if (this.panelAppear[k].right) {
        styleObj.right = this.panelAppear[k].right + 'px'
      }
      return styleObj
    }
  }
  get tableBoxStyle () {
    return (drawSize) => {
      if (drawSize) {
        return {'z-index': drawSize.zIndex, width: drawSize.width + 'px', height: drawSize.height + 'px', left: drawSize.left + 'px', top: drawSize.top + 'px'}
      }
    }
  }
  get searchResultData () {
    return groupData(this.modelGlobalSearchResult, 'kind')
  }
  mounted () {
    this.globalLoading.show()
    this.$el.onselectstart = function (e) {
      return false
    }
    this.loadDataSourceByProject({project: this.currentSelectedProject, isExt: true}).then((res) => { // 初始化project数据
      handleSuccess(res, (data) => {
        this.datasource = data
        this.initModelDesc((data) => { // 初始化模型数据
          this.modelInstance = new NModel(Object.assign(data, {
            project: this.currentSelectedProject,
            renderDom: this.renderBox
          }), this.modelRender, this)
          this.modelInstance.bindConnClickEvent((ptable, ftable) => {
            // 设置连接弹出框数据
            this.callJoinDialog({
              pid: ptable.guid,
              fid: ftable.guid,
              primaryTable: ptable,
              tables: this.modelRender.tables
            })
          })
        })
      })
    }, (err) => {
      handleError(err)
      this.globalLoading.hide()
    })
    this.$on('saveModel', () => {
      this.modelInstance.generateMetadata().then((data) => {
        if (this.modelRender.management_type !== 'TABLE_ORIENTED') {
          this.showPartitionDialog({
            modelDesc: data
          }).then((res) => {
            if (res.isSubmit) {
              this.handleSaveModel(data)
            } else {
              this.$emit('saveRequestEnd')
            }
          })
        } else {
          this.handleSaveModel(data)
        }
      }, (errMsg) => {
        kapMessage(this.$t(modelErrorMsg[errMsg]), {type: 'warning'})
        this.$emit('saveRequestEnd')
      }).catch(() => {
        this.$emit('saveRequestEnd')
      })
    })
  }
  handleSaveModel (data) {
    let action = 'saveModel'
    let para = data
    if (data.uuid) {
      action = 'updataModel'
    }
    this[action](para).then((res) => {
      handleSuccess(res, () => {
        kapMessage(this.$t('kylinLang.common.saveSuccess'))
        this.$emit('saveRequestEnd')
        setTimeout(() => {
          this.$router.replace({name: 'ModelList', params: { ignoreIntercept: true }})
        }, 1000)
      })
    }).catch((res) => {
      this.$emit('saveRequestEnd')
      handleError(res)
    })
  }
  created () {
    console.log(this.extraoption)
  }
  beforeDestroy () {
    this.toggleFullScreen(false)
  }
  destoryed () {
    $(document).unbind('selectstart')
  }
}
</script>
<style lang="less">
@import '../../../../assets/styles/variables.less';
.drag-in {
  box-shadow: inset 0 0 14px 0 @base-color;
}
.jtk-overlay {
  background-color: @tablelink-line-overlay-bg-color;
  padding: 2px;
  font: 12px sans-serif;
  z-index: 21;
  font-weight: bold;
  border: 2px solid @tablelink-line-color;
  cursor: pointer;
  min-width: 32px;
  height: 15px;
  border-radius: 10px;
  text-align: center;
  line-height: 15px;
  &.label_inner, &.label_left{
    border: 2px solid @tablelink-line-color;
    color:@text-normal-color;
    &:hover{
      color:@fff;
      background-color: #f7b72a;
      border: 2px solid #f7b72a;
    }
  }
  &:hover {
    background-color:@tablelink-line-color;
    color:#fff;
  }
}
.box-css() {
  position:relative;
  background-color:@grey-3;
}
.search-position() {
  width:783px;
  left:50%;
  margin-left:-392px;
  position:relative;
}
.model-edit-outer {
  border-top:@text-placeholder-color;
  user-select:none;
  overflow:hidden;
  .box-css();
  height: calc(~"100% - 94px");
  .panel-box{
      box-shadow: @box-shadow;
      position:relative;
      width:250px;
      .panel-title {
        background:@text-normal-color;
        height:36px;
        color:#fff;
        font-size:14px;
        line-height:36px;
        padding-left: 10px;
        .title{
          margin-left:4px;
          font-weight:@font-medium;
        }
        .close{
          float: right;
          margin-right:10px;
          font-size:14px;
          transform: scale(0.8);
        }
      }
      .panel-main-content{
        overflow:hidden;
        position:absolute;
        bottom: 16px;
        top:64px;
        right:0;
        left:0;
        ul {
          list-style: circle;
          margin-top:15px;
          margin-bottom:17px;
        }
        .dimension-list , .measure-list{
          margin-top:0;
          li {
            line-height:28px;
            height:28px;
            padding-left: 9px;
            padding-right: 8px;
            .li-type{
              position:absolute;
              right:4px;
              font-size:12px;
              color:@text-disabled-color;
            }
            i {
              display:none;
              margin-left:10px;
              float:right;
              font-size:12px;
            }
            &:hover {
              .li-type{
                display:none;
              }
              background-color:@base-color-10;
              i{
                display:inline-block;
                height:28px;
                line-height:28px;
              }
            }
          }
        }
      }
      .panel-sub-title {
        height:28px;
        background:@text-placeholder-color;
        line-height:28px;
        span{
          display: inline-block;
          margin-left: 10px;
        }
      }
      .panel-footer {
        position:absolute;
        bottom: 0;
        width:100%;
        text-align: center;
        background-color: @background-color3;
        line-height:16px;
        z-index:1;
      }
      background:#fff;
      position:absolute;
    }
    .panel-datasource {
      .tree-box {
        width:228px;
        .body{
          padding:10px;
        }
      }
      height:80%!important;
    }
    .panel-setting {
      height:316px;
      .panel-main-content{
        color:@text-disabled-color;
        .el-radio {
          color:@text-disabled-color;
        }
        overflow:hidden;
        padding: 10px;
        .active{
          color:@text-normal-color;
          .el-radio{
            color:@text-normal-color;
          }
        }
        ul {
          margin-left:40px;
          li{
            list-style:disc;
          }
        }
      }
    }
    .panel-search-box {
      width:100%!important;
      height:100%!important;
      position:fixed;
      top:112px!important;
      bottom:0!important;
      left:0!important;
      right:0!important;
      opacity: 0.93;
      z-index: 120!important;
      .close {
        position: absolute;
        right:10px;
        top:10px;
        font-size:18px;
      } 
      .search-result-box {
        max-height:calc(~"100% - 364px")!important;
        min-height:250px;
        overflow:auto;
        .search-position();
        box-shadow:@box-shadow;
        .search-noresult {
          font-size:20px;
          text-align: center;
          margin-top:100px;
          color:@text-placeholder-color;
        }
        .search-group {
          padding-top: 5px;
          padding-bottom: 5px; 
        }
        .search-content {
          &.active,&:hover{
            background-color:@base-color-10;
          }
          cursor:pointer;
          height:32px;
          line-height:32px;
          padding-left: 20px;
          .search-category {
            font-size:12px;
            color:@text-normal-color;
          }
          .search-name {
            i {
              color:@base-color;
              font-style: normal;
            }
            font-size:14px;
            color:@text-title-color;
          }
        }
      }
      .search-input {
        .search-position();
        height:72px;
        margin-top: 140px;
        .el-input__inner {
          height:72px;
          font-size:24px;
          color:@text-normal-color;
          padding-left: 50px;
        }
        .el-input__prefix {
          font-size:24px;
          margin-left: 14px;
        }
        .el-input__suffix {
          font-size:24px;
        }
      }
    }
    .tool-icon{
      position:absolute;
      width: 32px;
      height:32px;
      text-align: center;
      line-height: 32px;
      border-radius: 50%;
      cursor: pointer;
      .new-icon {
        background-color:@error-color-1;
        border-radius:2px;
        font-size:12px;
        text-align: center;
        position:absolute;
        width:30px;
        height:18px;
        line-height:18px;
        left: 16px;
        top:-6px;
        transform:scale(0.6);
      }
    }
    .tool-icon-group {
      position:absolute;
      width:32px;
      top:72px;
      right:10px;
      .tool-icon {
        box-shadow: @box-shadow;
        background:@text-normal-color;
        color:#fff;
        position:relative;
        margin-bottom: 10px;
        font-weight: bold;
        font-size: 16px;
        &.active{
          background:@base-color;
        }
      }
    }
    .sub-tool-icon-group {
      position:absolute;
      right:10px;
      top:258px;
      width:32px;
      .tool-icon{
        position:relative;
        height:30px;
        line-height:30px;
        i {
          color:@text-normal-color;
          font-size:18px;
          &:hover{
            color:@base-color;
          }
        }
      }
    }
    .icon-ds {
      top:10px;
      left:10px;
      background:@text-normal-color;
      color:#fff;
      box-shadow: @box-shadow;
      &.active{
        background:@base-color;
      }
      &:hover{
        background:@base-color;
      }
    }
    .icon-lock-status {
      top:10px;
      right:10px;
      i {
         display: block;
         line-height: 32px;
      }
      border:solid 1px @text-normal-color;
      &:hover{
        color: #fff;
        background-color:@normal-color-1;
        border:solid 1px @normal-color-1;
      }
    }
    .unlock-icon {
      &:hover{
        color: #fff;
        background-color:@base-color;
        border:solid 1px @base-color;
      }
    }
  }
  .model-edit{
    height: 100%;
    .box-css();
    .table-box {
      background-color:#fff;
      position:absolute;
      box-shadow:@box-shadow;
      overflow-x: hidden;
      .table-title {
        .close {  
          float:right;
          border-radius:50%;
          font-size:14px;
          width:20px;
          height:20px;
          line-height:20px;
          text-align: center;
          margin-top: 4px;
          margin-right: 3px;
          &:hover {
            background-color:@text-title-color;
            i {
              color:@base-color;
            }
          }
          i {
            color:@grey-3;
            transform:scale(0.8);
            margin: auto;
          }
        }
        .name {
          text-overflow: ellipsis;
          overflow: hidden;
          width:calc(~"100% - 50px");
        }
        span {
          width:24px;
          height:24px;
          float:left;
        }
        .kind:hover {
          background-color:@base-color;
          color:@grey-3;
        }
        &.isLookup {
          .close {
            &:hover{
              background-color:@grey-3;
            }
          }
          background-color: @text-secondary-color;
          color:@text-title-color;
        }
        height:28px;
        background-color: @text-normal-color;
        color:#fff;
        line-height:28px;
        i {
          color:@base-color;
          margin: auto 6px 8px;
        }
      }
      .drag-bar {
        position:absolute;
        bottom: 0;
        width:100%;
        text-align: center;
        background-color: @background-color3;
        line-height:16px;
      }
      .column-list-box {
        overflow:auto;
        position:absolute;
        top:28px;
        bottom:16px;
        right:0;
        left:0;
        overflow-x:hidden;
        ul {
          li {
            &:hover{
              background-color:@base-color-10;
            }
            padding-left:10px;
            cursor:move;
            border-bottom:solid 1px @table-stripe-color;
            height:26px;
            line-height:26px;
            font-size:12px;
            .col-type {
              float:right;
              color:@text-disabled-color;
              font-size:12px;
              margin-right: 10px;
            }
          }
        }
      }
    }
  }

</style>
