<template>
  <div class="model-edit-outer" @drop='dropTable($event)' v-drag="{sizeChangeCb:dragBox}">

    <div class="model-edit"  @dragover='allowDrop($event)' @dragleave="dragLeave">
      <!-- table box -->
      <div class="table-box" :id="t.guid" v-event-stop v-if="modelRender && modelRender.tables" v-for="t in modelRender && modelRender.tables || []" :style="tableBoxStyle(t.drawSize)">
        <div class="table-title"  @mousedown="activeTablePanel(t)"   v-drag:change.left.top="t.drawSize" :class="{isLookup:t.kind==='LOOKUP'}">
          <el-input v-show="t.aliasIsEdit" v-focus="t.aliasIsEdit" v-model="t.alias" @blur=" t.aliasIsEdit = false" v-event-stop></el-input>
          <span v-show="!t.aliasIsEdit">
            <i class="el-icon-ksd-fact_table kind" v-if="t.kind==='FACT'" v-event-stop></i>
            <i v-else class="el-icon-ksd-lookup_table kind" v-event-stop></i>
          </span>
          <common-tip class="name" v-show="!t.aliasIsEdit">
            <span slot="content">{{t.alias}} <i class="el-icon-ksd-table_edit" @click="editAlias(t)"></i></span>
            <span>{{t.alias}}</span>
          </common-tip>
          <span v-show="!t.aliasIsEdit" class="close" @click="delTable(t.guid)"><i class="el-icon-ksd-close"></i></span>
        </div>
        <div class="column-list-box" @dragover='($event) => {allowDropColumn($event, t.guid)}' v-event-stop @drop='(e) => {dropColumn(e, null, t)}' v-scroll>
          <ul >
            <li class="column-li" @drop='(e) => {dropColumn(e, col, t)}' @dragstart="(e) => {dragColumns(e, col, t)}"  draggable v-for="col in t.columns">
              <span class="col-name">{{col.name|omit(14,'...')}}</span>
              <span class="col-type">{{col.datatype}}</span>
            </li>
          </ul>
        </div>
        <div class="drag-bar" v-drag:change.height="t.drawSize"><i class="el-icon-ksd-bottom_bar"></i></div>
      </div>
      <!-- table box end -->
    </div>
    <DimensionModal/>
    <TableJoinModal/>
    <AddMeasure :isShow="measureVisible" v-on:closeAddMeasureDia="closeAddMeasureDia"></AddMeasure>
    <SingleDimensionModal/>
    <!-- datasource面板  index 3-->
    <div class="tool-icon icon-ds" :class="{active: panelAppear.datasource.display}" @click="toggleMenu('datasource')" v-event-stop><i class="el-icon-ksd-data_source"></i></div>
      <transition name="bounceleft">
        <div class="panel-box panel-datasource" v-show="panelAppear.datasource.display" :style="panelStyle('datasource')" v-event-stop>
          <div class="panel-title"><span>Data Source</span><span class="close" @click="toggleMenu('datasource')"><i class="el-icon-ksd-close"></i></span></div>
          <DataSourceBar
            class="tree-box"
            :is-show-load-source="false"
            :is-show-settings="false"
            :is-show-action-group="false"
            :is-expand-on-click-node="false"
            :expand-node-types="['datasouce', 'database']"
            :draggable-node-types="['table']"
            @drag="dragTable"
            :datasource="datasource">
          </DataSourceBar>
        </div>
      </transition>
      <!-- datasource面板  end-->
      <div class="tool-icon icon-lock-status" v-event-stop><i class="el-icon-ksd-lock"></i></div>
      <div class="tool-icon-group" v-event-stop>
        <div class="tool-icon" :class="{active: panelAppear.dimension.display}" @click="toggleMenu('dimension')">D</div>
        <div class="tool-icon" :class="{active: panelAppear.measure.display}" @click="toggleMenu('measure')">M</div>
        <div class="tool-icon" :class="{active: panelAppear.setting.display}" @click="toggleMenu('setting')"><i class="el-icon-ksd-setting"></i></div>
        <div class="tool-icon" :class="{active: panelAppear.search.display}" @click="toggleMenu('search')">
          <i class="el-icon-ksd-search"></i>
          <span class="new-icon">New</span>
        </div>
      </div>
      <div class="sub-tool-icon-group">
        <div class="tool-icon" @click="reduceZoom" v-event-stop><i class="el-icon-ksd-shrink" ></i></div>
        <div class="tool-icon" @click="addZoom" v-event-stop><i class="el-icon-ksd-enlarge"></i></div>
        <!-- <div class="tool-icon" v-event-stop>{{modelRender.zoom}}0%</div> -->
        <div class="tool-icon" @click="fullScreen" v-event-stop><i class="el-icon-ksd-full_screen" v-if="!isFullScreen"></i><i class="el-icon-ksd-collapse" v-if="isFullScreen"></i></div>
        <div class="tool-icon" @click="autoLayout" v-event-stop><i class="el-icon-ksd-auto"></i></div>
      </div>
      <!-- 右侧面板组 -->
      <!-- <div class="panel-group"> -->
        <!-- dimension面板  index 0-->
        <transition name="bounceright">
          <div class="panel-box panel-dimension" @mousedown="activePanel('dimension')" v-event-stop :style="panelStyle('dimension')" v-if="panelAppear.dimension.display">
            <div class="panel-title" @mousedown="activePanel('dimension')" v-drag:change.right.top="panelAppear.dimension">
              <span><i class="el-icon-ksd-dimansion"></i></span>
              <span class="title">Dimension</span>
              <span class="close" @click="toggleMenu('dimension')"><i class="el-icon-ksd-close"></i></span>
            </div>
            <div class="panel-sub-title">
              <span @click="batchDimension"><i class="el-icon-ksd-add"></i></span>
              <span @click="addDimension"><i class="el-icon-ksd-computed"></i></span>
              <span><i class="el-icon-ksd-table_delete"></i></span>
            </div>
            <div class="panel-main-content" v-scroll>
              <ul class="dimension-list">
                <template v-for="d in modelRender.dimensions">
                <li v-for="c in d.columns">{{c|omit(18,'...')}}<i class="el-icon-ksd-table_edit" @click="editDimension"></i><i class="el-icon-ksd-table_delete"></i><span>{{getColumnType(d.table, c)}}</span></li>
                </template>
              </ul>
            </div>
            <div class="panel-footer" v-drag:change.height="panelAppear.dimension"><i class="el-icon-ksd-bottom_bar"></i></div>
          </div>
        </transition>
        <!-- measure面板  index 1-->
        <transition name="bounceright">
          <div class="panel-box panel-measure" @mousedown="activePanel('measure')" v-event-stop :style="panelStyle('measure')"  v-if="panelAppear.measure.display">
            <div class="panel-title" @mousedown="activePanel('measure')" v-drag:change.right.top="panelAppear.measure">
              <span><i class="el-icon-ksd-measure"></i></span>
              <span class="title">Measure</span>
              <span class="close" @click="toggleMenu('measure')"><i class="el-icon-ksd-close"></i></span>
            </div>
            <div class="panel-sub-title">
              <span><i class="el-icon-ksd-add" @click="measureVisible = true"></i></span>
              <span @click="addDimension"><i class="el-icon-ksd-computed"></i></span>
              <span><i class="el-icon-ksd-table_delete"></i></span>
            </div>
            <div class="panel-main-content" v-scroll>
              <ul class="measure-list">
                <li v-for="m in modelRender.all_measures">{{m.name|omit(18,'...')}}</li>
              </ul>
            </div>
            <div class="panel-footer" v-drag:change.height="panelAppear.measure"><i class="el-icon-ksd-bottom_bar"></i></div>
          </div>
        </transition>
        <!-- setting面板  index 2-->
        <transition name="bounceright">
          <div class="panel-box panel-setting" v-event-stop @mousedown="activePanel('setting')" :style="panelStyle('setting')" v-if="panelAppear.setting.display">
            <div class="panel-title" @mousedown="activePanel('setting')" v-drag:change.right.top="panelAppear.setting">
              <span><i class="el-icon-ksd-setting"></i></span>
              <span class="title">Model Setting</span>
              <span class="close" @click="toggleMenu('setting')"><i class="el-icon-ksd-close"></i></span>
            </div>
            <div class="panel-main-content" style="top:36px;" data-scrollbar>
              <div :class="{active:autoSetting}">
                <div><el-radio v-model="autoSetting" :label="true">Auto-Model <i class="el-icon-ksd-lock"></i></el-radio></div>
                <ul>
                  <li>可以自动修改模型定义，比如Joint-tree,Dimension,Measure</li>
                  <li>可以添加和删除cuboid</li>
                  <li>可以自动删除模型</li>
                </ul>
              </div>
              <div :class="{active:!autoSetting}">
                <div><el-radio v-model="autoSetting" :label="false">Auto-Model <i class="el-icon-ksd-lock"></i></el-radio></div>
                <ul>
                  <li>无法自动修改模型定义，比如Joint-tree,Dimension,Measure</li>
                  <li>可以添加和删除cuboid</li>
                  <li>无法自动删除模型</li>
                </ul>
              </div>
            </div>
          </div>
        </transition>
      <!-- </div> -->
      <!-- 右侧面板组end -->

    <!-- 搜索面板 -->
    <transition name="bouncecenter">
     <div class="panel-search-box panel-box"  v-event-stop :style="panelStyle('search')" v-show="panelAppear.search.display">
      <span class="close" @click="toggleMenu('search')"><i class="el-icon-ksd-close"></i></span>
       <el-input @input="searchModelEverything"  clearable class="search-input" placeholder="search table, dimension, measure, column name" v-model="modelGlobalSearch" prefix-icon="el-icon-search"></el-input>
       <transition name="bounceleft">
       <div v-scroll class="search-result-box" v-keyborad-select="{scope:'.search-content', searchKey: modelGlobalSearch}" v-show="modelGlobalSearch && showSearchResult" v-search-highlight="{scope:'.search-name', hightlight: modelGlobalSearch}">
        <div>
         <div class="search-group" v-for="(k,v) in searchResultData" :key="v">
           <ul>
             <li class="search-content" v-for="x in k" @click="(e) => {selectResult(e, x)}" :key="x.action+x.name"><span class="search-category">[{{x.action}}]</span> <span class="search-name">{{x.name}}</span></li>
           </ul>
           <div class="ky-line"></div>
         </div>
         <div v-show="Object.keys(searchResultData).length === 0" class="search-noresult">No Result!</div>
       </div>
     </div>
     </transition>
    </div>
    </transition> 
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters, mapMutations } from 'vuex'
import locales from './locales'
import DataSourceBar from '../../../common/DataSourceBar'
import { handleSuccess, loadingBox } from '../../../../util/business'
import { isIE, groupData } from '../../../../util'
import Scrollbar from 'smooth-scrollbar'
import $ from 'jquery'
import DimensionModal from '../DimensionsModal/index.vue'
import AddMeasure from '../AddMeasure/index.vue'
import TableJoinModal from '../TableJoinModal/index.vue'
import SingleDimensionModal from '../SingleDimensionModal/addDimension.vue'
import NModel from './model.js'
import { modelRenderConfig } from './config'
@Component({
  props: ['extraoption'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      getModelByModelName: 'LOAD_MODEL_INFO',
      loadDataSourceByProject: 'LOAD_DATASOURCE'
    }),
    ...mapActions('DimensionsModal', {
      showDimensionDialog: 'CALL_MODAL'
    }),
    ...mapActions('TableJoinModal', {
      showJoinDialog: 'CALL_MODAL'
    }),
    ...mapMutations('TableJoinModal', {
      setLinkDialogData: 'SET_MODAL'
    }),
    ...mapActions('SingleDimensionModal', {
      showSingleDimensionDialog: 'CALL_MODAL'
    })
  },
  components: {
    DataSourceBar,
    AddMeasure,
    DimensionModal,
    TableJoinModal,
    SingleDimensionModal
  },
  locales
})
export default class ModelEdit extends Vue {
  datasource = []
  modelRender = {}
  modelInstance = null // 模型实例对象
  currentDragTable = '' // 当前拖拽的表
  currentDragColumn = '' // 当前拖拽的列
  currentDragColumnData = {} // 当前拖拽列携带信息
  modelGlobalSearch = '' // model全局搜索信息
  showSearchResult = true
  modelGlobalSearchResult = [{x: 1}]
  modelData = {}
  globalLoading = loadingBox()
  renderBox = modelRenderConfig.drawBox
  measureVisible = false
  baseIndex = 100
  autoSetting = true
  isFullScreen = false
  // 0 dimension 1 measure 2 setting 3 datasource 4 searchbox
  panelAppear = {
    dimension: {
      top: 72,
      right: 60,
      width: 250,
      height: 316,
      zIndex: this.baseIndex - 2,
      display: false,
      minheight: 80,
      // limit: {height: [80, 200], top: [0, 100], right: [0, 122]},
      box: modelRenderConfig.rootBox
    },
    measure: {
      top: 115,
      right: 60,
      width: 250,
      height: 316,
      minheight: 80,
      zIndex: this.baseIndex - 1,
      display: false,
      // limit: {height: [80, 200], top: [0, 100], right: [0, 122]},
      box: modelRenderConfig.rootBox
    },
    setting: {
      top: 158,
      right: 60,
      width: 250,
      height: 316,
      minheight: 80,
      zIndex: this.baseIndex,
      display: false,
      // limit: {height: [80, 200], top: [0, 100], right: [0, 122]},
      box: modelRenderConfig.rootBox
    },
    datasource: {
      top: 52,
      left: 10,
      width: 250,
      height: 316,
      minheight: 80,
      zIndex: this.baseIndex,
      display: true,
      // limit: {height: [80, 200], top: [0, 100], right: [0, 122]},
      box: modelRenderConfig.rootBox
    },
    search: {
      top: 52,
      left: 10,
      width: 250,
      height: 316,
      minheight: 80,
      zIndex: this.baseIndex,
      display: false,
      // limit: {height: [80, 200], top: [0, 100], right: [0, 122]},
      box: modelRenderConfig.rootBox
    }
  }
  radio = 1
  query (className) {
    return $(this.$el.querySelector(className))
  }
  delTable (guid) {
    this.modelInstance.delTable(guid)
  }
  editAlias (t) {
    this.$set(t, 'aliasIsEdit', true)
  }
  // 切换悬浮菜单
  toggleMenu (i) {
    this.panelAppear[i].display = !this.panelAppear[i].display
    if (this.panelAppear[i].display) {
      this.panelAppear[i].zindex = ++this.baseIndex
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
    this.isFullScreen = !this.isFullScreen
    $('#fullBox').toggleClass('fullLayoutForModelEdit')
  }
  // 自动布局
  autoLayout () {
    this.modelInstance.renderPosition()
  }
  // 老数据和新数据的转换  临时函数
  tempTransFormData (data) {
    var newtenData = {}
    newtenData.fact_table = data.fact_table
    newtenData.name = data.name
    newtenData.filter_condition = data.filter_condition
    newtenData.uuid = data.uuid
    newtenData.computed_columns = data.computed_columns
    newtenData.join_tables = data.lookups
    newtenData.partition_desc = data.partition_desc
    newtenData.partition_desc = data.last_modified
    newtenData.dimensions = data.dimensions
    newtenData.all_measures = data.metrics.map((x) => {
      return {name: x}
    })
    return newtenData
  }
  initModelDesc (cb) {
    if (this.extraoption.modelName && this.extraoption.action === 'edit') {
      this.getModelByModelName({modelName: this.extraoption.modelName, project: this.extraoption.project}).then((response) => {
        handleSuccess(response, (data) => {
          var transData = this.tempTransFormData(data.model)
          this.modelData = transData
          transData.action = this.action
          cb(transData)
          this.globalLoading.hide()
        })
      }, () => {
        this.globalLoading.hide()
      })
    } else if (this.action === 'new') {
      this.modelData = {
        modelName: this.extraoption.modelName,
        name: this.extraoption.action
      }
      cb(this.modelData)
      this.globalLoading.hide()
    }
  }
  initScroll () {
    this.$nextTick(() => {
      this.$el.querySelectorAll('.panel-main-content').forEach((el) => {
        Scrollbar.init(el)
      })
      this.$el.querySelectorAll('.column-list-box').forEach((el) => {
        Scrollbar.init(el)
      })
    })
  }
  batchDimension () {
    this.showDimensionDialog([], [])
  }
  addDimension () {
  }
  editDimension () {
    this.showSingleDimensionDialog()
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
        alias: this.currentDragTable,
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
    let dropColumnData = {}
    // 判断是否是自己连自己
    if (this.currentDragColumnData.guid === table.guid) {
      return
    }
    // 判断是否是把fact当主键盘（连向fact）
    if (table.kind === modelRenderConfig.tableKind.fact) {
      return
    }
    // 判断连接的表是不是已经已经做了别人的主键
    var curTableLinkTable = table.getJoinInfo() && table.getJoinInfo().foreignTable || null
    if (curTableLinkTable && curTableLinkTable.guid !== this.currentDragColumnData.guid) {
      return
    }
    if (this.currentDragColumnData.guid) {
      dropColumnData = {
        guid: table.guid,
        columnName: col && col.name || ''
      }
    }
    // 设置连接弹出框数据
    this.setLinkDialogData({
      foreignTable: this.modelRender.tables[this.currentDragColumnData.guid],
      primaryTable: table,
      tables: this.modelRender.tables
    })
    // 弹出框弹出
    this.showJoinDialog()
    this.modelInstance.renderLink(dropColumnData.guid, this.currentDragColumnData.guid)
    this.currentDragColumnData = {}
    this.currentDragColumn = null
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
  selectResult (e, select) {
    this.showSearchResult = false
    this.modelGlobalSearch = ''
    if (select.action === 'adddimension') {
      this.showDimensionDialog([], [])
    }
    if (select.action === 'editdimension') {
      this.showDimensionDialog([], [])
    }
    if (select.action === 'editmeasure') {
      this.measureVisible = true
    }
    if (select.action === 'editjoin') {
      this.showJoinDialog()
    }
    this.panelAppear.search.display = false
  }
  searchModelEverything (val) {
    this.modelGlobalSearchResult = []
    Array.prototype.push.apply(this.modelGlobalSearchResult, this.modelInstance.search(val))
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
    this.loadDataSourceByProject({project: this.currentSelectedProject, isExt: true}).then((res) => {
      handleSuccess(res, (data) => {
        this.datasource = data
        this.initModelDesc((data) => {
          this.modelInstance = new NModel(Object.assign(data, {
            project: this.currentSelectedProject,
            renderDom: this.renderBox
          }), this.modelRender, this)
          this.modelInstance.bindConnClickEvent((ptable, ftable) => {
            // 设置连接弹出框数据
            this.setLinkDialogData({
              foreignTable: ftable,
              primaryTable: ptable,
              tables: this.modelRender.tables
            })
            this.showJoinDialog()
          })
          this.initScroll()
        })
      })
    })
  }
  beforeCreate () {

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
          font-size:12px;
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
            span{
              float:right;
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
              span{
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
        max-height:calc(~"100% - 300px")!important;
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
      border:solid 1px @text-normal-color;
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
          font-size:12px;
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
          display:inline-block;
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
