<template>
  <div class="model-edit-outer" @drop='dropTable($event)' @dragover='allowDrop($event)' v-drag="{sizeChangeCb:dragBox}" @dragleave="dragLeave">
    <div class="model-edit">
      <!-- table box -->
      <div class="table-box" @click="activeTablePanel(t)" v-visible="!currentEditTable || currentEditTable.guid !== t.guid" :id="t.guid" v-event-stop v-if="modelRender && modelRender.tables" :class="{isLookup:t.kind==='LOOKUP'}" v-for="t in modelRender && modelRender.tables || []" :key="t.guid" :style="tableBoxStyle(t.drawSize)">
        <div class="table-title" :data-zoom="modelRender.zoom"  v-drag:change.left.top="t.drawSize">
          <!-- <el-input v-show="t.aliasIsEdit" v-focus="t.aliasIsEdit" v-event-stop v-model="t.alias" @blur="saveNewAlias(t)" @keyup.enter="saveNewAlias(t)"></el-input> -->
          <span>
            <i class="el-icon-ksd-fact_table kind" v-if="t.kind==='FACT'"></i>
            <i v-else class="el-icon-ksd-lookup_table kind"></i>
          </span>
          <common-tip class="name" v-show="!t.aliasIsEdit">
            <span slot="content">{{t.alias}}</span>
            <span class="alias-span">{{t.alias}}</span>
          </common-tip>
          <span class="close" @click="editTable(t.guid)"><i class="el-icon-ksd-table_setting"></i></span>
        </div>
        <div class="column-list-box" @dragover='($event) => {allowDropColumn($event, t.guid)}' @drop='(e) => {dropColumn(e, null, t)}' v-scroll>
          <ul >
            <li v-on:dragover="(e) => {dragColumnEnter(e, t)}" v-on:dragleave="dragColumnLeave" class="column-li" :class="{'column-li-cc': col.is_computed_column}" @drop.stop='(e) => {dropColumn(e, col, t)}' @dragstart="(e) => {dragColumns(e, col, t)}"  draggable v-for="col in t.columns" :key="col.name">
              <span class="col-type-icon"><i :class="columnTypeIconMap(col.datatype)"></i></span>
              <span class="col-name">{{col.name|omit(14,'...')}}</span>
              <!-- <span class="li-type ky-option-sub-info">{{col.datatype}}</span> -->
            </li>
            <template v-if="t.kind=== 'FACT'">
              <li class="column-li column-li-cc" @drop='(e) => {dropColumn(e, {name: col.columnName }, t)}' @dragstart="(e) => {dragColumns(e, {name: col.columnName}, t)}"  draggable v-for="col in modelRender.computed_columns" :key="col.name">
                <span class="col-type-icon"><i :class="columnTypeIconMap(col.datatype)"></i></span>
                <span class="col-name">{{col.columnName|omit(14,'...')}}</span>
                <!-- <span class="li-type ky-option-sub-info">{{col.datatype}}</span> -->
              </li>
            </template>
          </ul>
        </div>
        <!-- 拖动操纵 -->
        <DragBar :dragData="t.drawSize" :dragZoom="modelRender.zoom"/>
        <!-- 拖动操纵 -->
      </div>
      <!-- table box end -->
    </div>
    <!-- datasource面板  index 3-->
    <div class="tool-icon icon-ds" :class="{active: panelAppear.datasource.display}" v-event-stop @click="toggleMenu('datasource')"><i class="el-icon-ksd-data_source"></i></div>
      <transition name="bounceleft">
        <div class="panel-box panel-datasource"  v-show="panelAppear.datasource.display" :style="panelStyle('datasource')" v-event-stop>
          <div class="panel-title" v-drag:change.left.top="panelAppear.datasource"><span class="title">{{$t('kylinLang.common.dataSource')}}</span><span class="close" @click="toggleMenu('datasource')"><i class="el-icon-ksd-close"></i></span></div>
          <div v-scroll style="height:calc(100% - 40px)" class="ksd-right-4">
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
          <!-- 拖动操纵 -->
          <DragBar :dragData="panelAppear.datasource"/>
          <!-- 拖动操纵 -->
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
        <div class="tool-icon" :class="{active: panelAppear.cc.display}" @click="toggleMenu('cc')"><i class="el-icon-ksd-computed_column"></i></div>
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
              <span class="title">{{$t('kylinLang.common.dimension')}} ({{allDimension.length}})</span>
              <span class="close" @click="toggleMenu('dimension')"><i class="el-icon-ksd-close"></i></span>
            </div>
            <div class="panel-sub-title">
              <div class="action_group" :class="{'is_active': !isShowCheckbox}"
              :style="{
                msTransform: `translateX(${ translate }px)`,
                webkitTransform: `translateX(${ translate }px)`,
                transform: `translateX(${ translate }px)`,
                width: panelAppear.dimension.width-2+'px'
              }">
                <span class="action_btn" @click="addCCDimension">
                  <i class="el-icon-ksd-project_add"></i>
                  <span>{{$t('add')}}</span>
                </span>
                <span class="action_btn" @click="batchSetDimension">
                  <i class="el-icon-ksd-backup"></i>
                  <span>{{$t('batchAdd')}}</span>
                </span>
                <span class="action_btn" :class="{'disabled': allDimension.length==0}" @click="toggleCheckbox">
                  <i class="el-icon-ksd-batch_delete"></i>
                  <span>{{$t('batchDel')}}</span>
                </span>
              </div>
              <div
              class="batch_group"
              :class="{'is_active': isShowCheckbox}"
              :style="{
                msTransform: `translateX(${ translate+parseInt(panelAppear.dimension.width) }px)`,
                webkitTransform: `translateX(${ translate+parseInt(panelAppear.dimension.width) }px)`,
                transform: `translateX(${ translate+parseInt(panelAppear.dimension.width) }px)`,
                width: panelAppear.dimension.width-2+'px'
              }">
                <span class="action_btn" @click="toggleCheckAllDimension">
                  <i class="el-icon-ksd-batch_uncheck" v-if="dimensionSelectedList.length==allDimension.length"></i>
                  <i class="el-icon-ksd-batch" v-else></i>
                  <span v-if="dimensionSelectedList.length==allDimension.length">{{$t('unCheckAll')}}</span>
                  <span v-else>{{$t('checkAll')}}</span>
                </span>
                <span class="action_btn" :class="{'disabled': dimensionSelectedList.length==0}" @click="deleteDimenisons">
                  <i class="el-icon-ksd-table_delete"></i>
                  <span>{{$t('delete')}}</span>
                </span>
                <span class="action_btn" @click="toggleCheckbox">
                  <i class="el-icon-ksd-back"></i>
                  <span>{{$t('back')}}</span>
                </span>
              </div>
            </div>
            <div class="panel-main-content" @dragover='($event) => {allowDropColumnToPanle($event)}' v-event-stop @drop='(e) => {dropColumnToPanel(e, "dimension")}' v-scroll>
              <ul class="dimension-list">
                <li v-for="(d, i) in allDimension" :key="d.name" :class="{'is-checked':dimensionSelectedList.indexOf(d.name)>-1}">
                  <el-checkbox v-model="dimensionSelectedList" v-if="isShowCheckbox" :label="d.name">{{d.name|omit(18,'...')}}</el-checkbox>
                  <span v-else>{{d.name|omit(18,'...')}}</span>
                  <span class="icon-group">
                    <span class="icon-span"><i class="el-icon-ksd-table_delete" @click="deleteDimenison(d.name)"></i></span>
                    <span class="icon-span"><i class="el-icon-ksd-table_edit" @click="editDimension(d, i)"></i></span>
                    <span class="li-type ky-option-sub-info">{{d.datatype}}</span>
                  </span>
                </li>
              </ul>
            </div>
            <!-- 拖动操纵 -->
            <DragBar :dragData="panelAppear.dimension"/>
            <!-- 拖动操纵 -->
          </div>
        </transition>
        <!-- measure面板  index 1-->
        <transition name="bounceright">
          <div class="panel-box panel-measure" @mousedown.stop="activePanel('measure')" :style="panelStyle('measure')"  v-if="panelAppear.measure.display">
            <div class="panel-title" @mousedown="activePanel('measure')" v-drag:change.right.top="panelAppear.measure">
              <span><i class="el-icon-ksd-measure"></i></span>
              <span class="title">{{$t('kylinLang.common.measure')}} ({{modelRender.all_measures.length}})</span>
              <span class="close" @click="toggleMenu('measure')"><i class="el-icon-ksd-close"></i></span>
            </div>
            <div class="panel-sub-title">
              <div class="action_group" :class="{'is_active': !isShowMeaCheckbox}"
              :style="{
                msTransform: `translateX(${ translateMea }px)`,
                webkitTransform: `translateX(${ translateMea }px)`,
                transform: `translateX(${ translateMea }px)`,
                width: panelAppear.measure.width-2+'px'
              }">
                <span class="action_btn" @click="addNewMeasure">
                  <i class="el-icon-ksd-project_add"></i>
                  <span>{{$t('add')}}</span>
                </span>
                <span class="action_btn" @click="toggleMeaCheckbox" :class="{'disabled': modelRender.all_measures.length==1}">
                  <i class="el-icon-ksd-batch_delete"></i>
                  <span>{{$t('batchDel')}}</span>
                </span>
              </div>
              <div
                class="batch_group"
                :class="{'is_active': isShowMeaCheckbox}"
                :style="{
                  msTransform: `translateX(${ translateMea+parseInt(panelAppear.measure.width) }px)`,
                  webkitTransform: `translateX(${ translateMea+parseInt(panelAppear.measure.width) }px)`,
                  transform: `translateX(${ translateMea+parseInt(panelAppear.measure.width) }px)`,
                  width: panelAppear.measure.width-2+'px'
                }">
                <span class="action_btn" @click="toggleCheckAllMeasure">
                  <i class="el-icon-ksd-batch_uncheck" v-if="measureSelectedList.length==modelRender.all_measures.length-1"></i>
                  <i class="el-icon-ksd-batch" v-else></i>
                  <span v-if="measureSelectedList.length==modelRender.all_measures.length-1">{{$t('unCheckAll')}}</span>
                  <span v-else>{{$t('checkAll')}}</span>
                </span>
                <span class="action_btn" :class="{'disabled': measureSelectedList.length==0}" @click="deleteMeasures">
                  <i class="el-icon-ksd-table_delete"></i>
                  <span>{{$t('delete')}}</span>
                </span>
                <span class="action_btn" @click="toggleMeaCheckbox">
                  <i class="el-icon-ksd-back"></i>
                  <span>{{$t('back')}}</span>
                </span>
              </div>
            </div>
            <div class="panel-main-content"  @dragover='($event) => {allowDropColumnToPanle($event)}' v-event-stop @drop='(e) => {dropColumnToPanel(e, "measure")}' v-scroll>
              <ul class="measure-list">
                <li v-for="m in modelRender.all_measures" :key="m.name" :class="{'is-checked':measureSelectedList.indexOf(m.name)>-1}">
                  <el-checkbox v-model="measureSelectedList" v-if="isShowMeaCheckbox" :disabled="m.name=='COUNT_ALL'" :label="m.name">{{m.name|omit(18,'...')}}</el-checkbox>
                  <span v-else>{{m.name|omit(18,'...')}}</span>
                  <span class="icon-group">
                    <span class="icon-span" v-if="m.name !== 'COUNT_ALL'"><i class="el-icon-ksd-table_delete" @click="deleteMeasure(m.name)"></i></span>
                    <span class="icon-span" v-if="m.name !== 'COUNT_ALL'"><i class="el-icon-ksd-table_edit" @click="editMeasure(m)"></i></span>
                    <span class="li-type ky-option-sub-info">{{m.return_type}}</span>
                  </span>
                </li>
              </ul>
            </div>
            <!-- 拖动操纵 -->
            <DragBar :dragData="panelAppear.measure"/>
            <!-- 拖动操纵 -->
          </div>
        </transition>
        <!-- 可计算列 -->
        <transition name="bounceright">
          <div class="panel-box panel-cc" @mousedown.stop="activePanel('cc')" :style="panelStyle('cc')"  v-if="panelAppear.cc.display">
            <div class="panel-title" @mousedown="activePanel('cc')" v-drag:change.right.top="panelAppear.cc">
              <span><i class="el-icon-ksd-auto_computed_column"></i></span>
              <span class="title">{{$t('kylinLang.model.computedColumn')}} ({{modelRender.computed_columns.length}})</span>
              <span class="close" @click="toggleMenu('cc')"><i class="el-icon-ksd-close"></i></span>
            </div>
            <div class="panel-sub-title">
              <div class="action_group" :class="{'is_active': !isShowCCCheckbox}"
              :style="{
                msTransform: `translateX(${ translateCC }px)`,
                webkitTransform: `translateX(${ translateCC }px)`,
                transform: `translateX(${ translateCC }px)`,
                width: panelAppear.cc.width-2+'px'
              }">
                <span class="action_btn" @click="addCC">
                  <i class="el-icon-ksd-project_add"></i>
                  <span>{{$t('add')}}</span>
                </span>
                <span class="action_btn" @click="toggleCCCheckbox" :class="{'active': isShowCCCheckbox}">
                  <i class="el-icon-ksd-batch_delete"></i>
                  <span>{{$t('batchDel')}}</span>
                </span>
              </div>
              <div
                class="batch_group"
                :class="{'is_active': isShowCCCheckbox}"
                :style="{
                  msTransform: `translateX(${ translateCC+parseInt(panelAppear.cc.width) }px)`,
                  webkitTransform: `translateX(${ translateCC+parseInt(panelAppear.cc.width) }px)`,
                  transform: `translateX(${ translateCC+parseInt(panelAppear.cc.width) }px)`,
                  width: panelAppear.cc.width-2+'px'
                }">
                <span class="action_btn" @click="toggleCheckAllCC">
                  <i class="el-icon-ksd-batch_uncheck" v-if="ccSelectedList.length==modelRender.computed_columns.length"></i>
                  <i class="el-icon-ksd-batch" v-else></i>
                  <span v-if="ccSelectedList.length==modelRender.computed_columns.length">{{$t('unCheckAll')}}</span>
                  <span v-else>{{$t('checkAll')}}</span>
                </span>
                <span class="action_btn" :class="{'disabled': ccSelectedList.length==0}" @click="delCCs">
                  <i class="el-icon-ksd-table_delete"></i>
                  <span>{{$t('delete')}}</span>
                </span>
                <span class="action_btn" @click="toggleCCCheckbox">
                  <i class="el-icon-ksd-back"></i>
                  <span>{{$t('back')}}</span>
                </span>
              </div>
            </div>
            <div class="panel-main-content" v-scroll>
              <ul class="cc-list">
                <li v-for="m in modelRender.computed_columns" :key="m.name" :class="{'is-checked':ccSelectedList.indexOf(m.columnName)>-1}">
                  <el-checkbox v-model="ccSelectedList" v-if="isShowCCCheckbox" :label="m.columnName">{{m.columnName|omit(18,'...')}}</el-checkbox>
                  <span v-else>{{m.columnName|omit(18,'...')}}</span>
                  <span class="icon-group">
                    <span class="icon-span"><i class="el-icon-ksd-table_delete" @click="delCC(m.columnName)"></i></span>
                    <span class="icon-span"><i class="el-icon-ksd-details" @click="showCCDetail(m)"></i></span>
                    <span class="li-type ky-option-sub-info">{{m.datatype}}</span>
                  </span>
                </li>
              </ul>
            </div>
            <!-- 拖动操纵 -->
            <DragBar :dragData="panelAppear.cc"/>
            <!-- 拖动操纵 -->
          </div>
        </transition>

    <!-- 搜索面板 -->
    <transition name="bouncecenter">
      <div class="panel-search-box panel-box" :class="{'full-screen': isFullScreen}"  v-event-stop :style="panelStyle('search')" v-if="panelAppear.search.display">
        <span class="close" @click="toggleMenu('search')"><i class="el-icon-ksd-close"></i></span>
         <el-alert class="search-action-result" v-if="modelSearchActionSuccessTip" v-timer-hide:2
          :title="modelSearchActionSuccessTip"
          type="success"
          :closable="false"
          show-icon>
        </el-alert>
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
            <div v-show="Object.keys(searchResultData).length === 0" class="search-noresult">{{$t('kylinLang.common.noData')}}</div>
          </div>
        </div>
        </transition>
      </div>
    </transition> 
    <PartitionModal/>
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
    <AddCC/>
    <ShowCC/>

    <!-- 编辑模型table遮罩 -->
    <div class="full-screen-cover" v-event-stop @click="cancelTableEdit" v-if="showTableCoverDiv"></div>
    <transition name="slide-fade">
      <!-- 编辑table 快捷按钮 -->
      <div class="fast-action-box" v-event-stop @click="cancelTableEdit" :class="{'edge-right': currentEditTable.drawSize.isInRightEdge}" :style="tableBoxToolStyleNoZoom(currentEditTable.drawSize)" v-if="currentEditTable && showTableCoverDiv">
        <div>
          <div class="action switch" v-if="currentEditTable.kind === 'FACT'" @click.stop="changeTableType(currentEditTable)"><i class="el-icon-ksd-switch"></i>
            <span >{{$t('switchLookup')}}</span>
          </div>
          <div class="action switch" v-if="modelInstance.checkTableCanSwitchFact(currentEditTable.guid)" @click.stop="changeTableType(currentEditTable)"><i class="el-icon-ksd-switch"></i>
            <span >{{$t('switchFact')}}</span>
          </div>
        </div>
        <div v-show="showEditAliasForm">
          <div class="alias-form" v-event-stop:click>
              <el-input v-model="currentEditAlias" size="mini" @click.stop @keyup.enter.native="saveEditTableAlias"></el-input>
              <el-button type="primary" size="mini" icon="el-icon-check" @click.stop="saveEditTableAlias"></el-button><el-button size="mini" @click.stop="cancelEditAlias" icon="el-icon-close" plain></el-button>
          </div>
        </div>
        <div v-show="!showEditAliasForm && currentEditTable.kind!=='FACT'">
          <div class="action">
            <div @click.stop="openEditAliasForm"><i class="el-icon-ksd-table_edit"></i> {{$t('editTableAlias')}}</div>
          </div>
        </div>
        <el-popover
          popper-class="fast-action-popper"
          style="z-index:100001"
          ref="popover5"
          placement="top"
          width="160"
          v-model="delTipVisible">
          <p>{{$t('delTableTip')}}</p>
          <div style="text-align: right; margin: 0">
            <el-button size="mini" type="info" text @click="delTipVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
            <el-button type="primary" size="mini" @click.enter="delTable">{{$t('kylinLang.common.ok')}}</el-button>
          </div>
        </el-popover>
        <div class="action del" v-if="!modelInstance.checkTableCanDel(currentEditTable.guid)" @click.stop="showDelTableTip"  v-popover:popover5><i class="el-icon-ksd-table_delete"></i> {{$t('deleteTable')}}</div>
        <div class="action del" v-else @click.stop="delTable"><i class="el-icon-ksd-table_delete"></i> {{$t('deleteTable')}}</div>
      </div>
    </transition>
    <!-- 被编辑table clone dom -->
    <div class="table-box fast-action-temp-table" :id="currentEditTable.guid + 'temp'" v-event-stop v-if="showTableCoverDiv" :class="{isLookup:currentEditTable.kind==='LOOKUP'}" :style="tableBoxStyleNoZoom(currentEditTable.drawSize)">
      <div class="table-title" :data-zoom="modelRender.zoom"  v-drag:change.left.top="currentEditTable.drawSize">
        <span @click.stop="changeTableType(currentEditTable)">
          <i class="el-icon-ksd-fact_table kind" v-if="currentEditTable.kind==='FACT'"></i>
          <i v-else class="el-icon-ksd-lookup_table kind"></i>
        </span>
        <span class="alias-span name">{{currentEditTable.alias}}</span>
        <span class="close" @click="cancelTableEdit"><i class="el-icon-ksd-table_setting"></i></span>
      </div>
      <div class="column-list-box"  v-scroll>
        <ul >
          <li class="column-li" :class="{'column-li-cc': col.is_computed_column}"  v-for="col in currentEditTable.columns" :key="col.name">
            <span class="col-type-icon"><i :class="columnTypeIconMap(col.datatype)"></i></span>
            <span class="col-name">{{col.name|omit(14,'...')}}</span>
            <!-- <span class="li-type ky-option-sub-info">{{col.datatype}}</span> -->
          </li>
          <template v-if="currentEditTable.kind=== 'FACT'">
            <li class="column-li column-li-cc"  v-for="col in modelRender.computed_columns" :key="col.name">
              <span class="col-type-icon"><i :class="columnTypeIconMap(col.datatype)"></i></span>
              <span class="col-name">{{col.columnName|omit(14,'...')}}</span>
              <!-- <span class="li-type ky-option-sub-info">{{col.datatype}}</span> -->
            </li>
          </template>
        </ul>
      </div>
      <!-- 拖动操纵 -->
      <DragBar :dragData="currentEditTable.drawSize"/>
      <!-- 拖动操纵 -->
    </div>
    
  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters, mapMutations, mapState } from 'vuex'
import locales from './locales'
import DataSourceBar from '../../../common/DataSourceBar'
import { handleSuccess, handleError, loadingBox, kapMessage } from '../../../../util/business'
import { isIE, groupData, objectClone } from '../../../../util'
import $ from 'jquery'
import DimensionModal from '../DimensionsModal/index.vue'
import AddMeasure from '../AddMeasure/index.vue'
import TableJoinModal from '../TableJoinModal/index.vue'
import SingleDimensionModal from '../SingleDimensionModal/addDimension.vue'
import PartitionModal from '../ModelList/ModelPartitionModal/index.vue'
import DragBar from './dragbar.vue'
import AddCC from '../AddCCModal/addcc.vue'
import ShowCC from '../ShowCC/showcc.vue'
import NModel from './model.js'
import { modelRenderConfig, modelErrorMsg, columnTypeIcon } from './config'
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
    }),
    ...mapActions('CCAddModal', {
      showAddCCDialog: 'CALL_MODAL'
    }),
    ...mapActions('ShowCCDialogModal', {
      showCCDetailDialog: 'CALL_MODAL'
    })
  },
  components: {
    DataSourceBar,
    DragBar,
    AddMeasure,
    DimensionModal,
    TableJoinModal,
    SingleDimensionModal,
    PartitionModal,
    AddCC,
    ShowCC
  },
  locales
})
export default class ModelEdit extends Vue {
  datasource = []
  modelRender = {tables: {}}
  dimensionSelectedList = []
  measureSelectedList = []
  ccSelectedList = []
  modelInstance = null // 模型实例对象
  currentDragTable = '' // 当前拖拽的表
  currentDragColumn = '' // 当前拖拽的列
  currentDropColumnData = {} // 当前释放到的列
  currentDragColumnData = {} // 当前拖拽列携带信息
  modelGlobalSearch = '' // model全局搜索信息
  showSearchResult = true
  modelGlobalSearchResult = []
  modelData = {}
  columnTypeIconMap = columnTypeIcon
  modelSearchActionSuccessTip = ''
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
    parameterValue: {type: 'column', value: '', table_guid: null},
    convertedColumns: [],
    return_type: ''
  }
  panelAppear = modelRenderConfig.pannelsLayout()
  radio = 1
  isShowCheckbox = false
  isShowMeaCheckbox = false
  isShowCCCheckbox = false
  get allDimension () {
    return this.modelRender.dimensions
  }
  query (className) {
    return $(this.$el.querySelector(className))
  }
  // 快捷编辑table操作 start
  showTableCoverDiv = false
  currentEditTable = null
  showEditAliasForm = false
  currentEditAlias = ''
  delTipVisible = false
  // 取消table编辑
  cancelTableEdit () {
    this.showTableCoverDiv = false
    this.currentEditTable = null
    this.showEditAliasForm = false
    this.currentEditAlias = ''
    this.delTipVisible = false
  }
  showDelTableTip () {
    this.delTipVisible = true
  }
  toggleCheckbox () {
    if (this.allDimension.length === 0 && !this.isShowCheckbox) {
      return
    } else if (this.isShowCheckbox) {
      this.dimensionSelectedList = []
    }
    this.isShowCheckbox = !this.isShowCheckbox
  }
  get translate () {
    if (this.isShowCheckbox) {
      return 0 - this.panelAppear.dimension.width
    } else {
      return 0
    }
  }
  toggleMeaCheckbox () {
    if (this.modelRender.all_measures.length === 1 && !this.isShowMeaCheckbox) {
      return
    } else if (this.isShowMeaCheckbox) {
      this.measureSelectedList = []
    }
    this.isShowMeaCheckbox = !this.isShowMeaCheckbox
  }
  get translateMea () {
    if (this.isShowMeaCheckbox) {
      return 0 - this.panelAppear.measure.width
    } else {
      return 0
    }
  }
  toggleCCCheckbox () {
    if (this.modelRender.computed_columns.length === 0 && !this.isShowCCCheckbox) {
      return
    } else if (this.isShowCCCheckbox) {
      this.ccSelectedList = []
    }
    this.isShowCCCheckbox = !this.isShowCCCheckbox
  }
  get translateCC () {
    if (this.isShowCCCheckbox) {
      return 0 - this.panelAppear.cc.width
    } else {
      return 0
    }
  }
  delTable () {
    this.modelInstance.delTable(this.currentEditTable.guid).then(() => {
      this.cancelTableEdit()
    }, () => {
      this.delTipVisible = false
      kapMessage(this.$t('delTableTip'), {type: 'warning'})
    })
  }
  // 编辑table
  editTable (guid) {
    this._hisZoom = this.modelRender.zoom
    this.currentEditTable = this.modelInstance.getTableByGuid(guid)
    this.currentEditAlias = this.currentEditTable.alias
    this.showTableCoverDiv = true
    this.showEditAliasForm = false
    this.delTipVisible = false
  }
  // 保存table的别名
  saveEditTableAlias () {
    this.currentEditTable.alias = this.currentEditAlias
    this.saveNewAlias(this.currentEditTable)
    this.showEditAliasForm = false
  }
  saveNewAlias (t) {
    this.modelInstance.setUniqueAlias(t)
    this.modelInstance.changeAlias()
  }
  // 快捷编辑table操作 end
  // 切换悬浮菜单
  toggleMenu (i) {
    this.panelAppear[i].display = !this.panelAppear[i].display
    if (this.panelAppear[i].display) {
      this.activePanel(i)
    }
    this.modelSearchActionSuccessTip = ''
  }
  activePanel (i) {
    var curPanel = this.panelAppear[i]
    this.modelInstance.setIndexTop(Object.values(this.panelAppear), curPanel, '')
  }
  activeTablePanel (t) {
    this.modelInstance.setIndexTop(Object.values(this.modelRender.tables), t, 'drawSize')
  }
  closeAddMeasureDia (isSubmit) {
    if (isSubmit) {
      this.modelSearchActionSuccessTip = 'measure 保存成功'
    }
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
      parameterValue: {type: 'column', value: '', table_guid: null},
      convertedColumns: [],
      return_type: ''
    }
  }
  editDimension (dimension, i) {
    dimension._id = i
    this.showSingleDimensionDialog({
      dimension: objectClone(dimension),
      modelInstance: this.modelInstance
    })
  }
  deleteDimenison (name) {
    this.modelInstance.delDimension(name)
  }
  toggleCheckAllDimension () {
    if (this.dimensionSelectedList.length === this.allDimension.length) {
      this.dimensionSelectedList = []
    } else {
      this.dimensionSelectedList = this.allDimension.map((item, i) => {
        return item.name
      })
    }
  }
  // 批量删除
  deleteDimenisons () {
    this.dimensionSelectedList && this.dimensionSelectedList.forEach((name) => {
      this.modelInstance.delDimension(name)
    })
    this.dimensionSelectedList = []
    if (this.allDimension.length === 0) {
      this.toggleCheckbox()
    }
  }
  deleteMeasure (name) {
    this.modelInstance.delMeasure(name)
  }
  toggleCheckAllMeasure () {
    if (this.measureSelectedList.length === this.modelRender.all_measures.length - 1) {
      this.measureSelectedList = []
    } else {
      this.measureSelectedList = this.modelRender.all_measures.map((item, i) => {
        return item.name
      })
      this.measureSelectedList.shift()
    }
  }
  deleteMeasures () {
    this.measureSelectedList && this.measureSelectedList.forEach((name) => {
      this.modelInstance.delMeasure(name)
    })
    this.measureSelectedList = []
    if (this.modelRender.all_measures.length === 1) {
      this.toggleMeaCheckbox()
    }
  }
  addCC () {
    this.showAddCCDialog({
      modelInstance: this.modelInstance
    })
  }
  // 单个删除CC
  delCC (name) {
    this.modelInstance.delCC(name)
  }
  showCCDetail (cc) {
    this.showCCDetailDialog({
      ccDetail: cc
    })
  }
  toggleCheckAllCC () {
    if (this.ccSelectedList.length === this.modelRender.computed_columns.length) {
      this.ccSelectedList = []
    } else {
      this.ccSelectedList = this.modelRender.computed_columns.map((item) => {
        return item.columnName
      })
    }
  }
  // 批量删除CC
  delCCs () {
    this.ccSelectedList && this.ccSelectedList.forEach((i) => {
      this.delCC(i)
    })
    this.ccSelectedList = []
    if (this.modelRender.computed_columns.length === 0) {
      this.toggleCCCheckbox()
    }
  }
  editMeasure (m) {
    this.$nextTick(() => {
      this.measureObj = m
    })
    this.measureVisible = true
    this.isEditMeasure = true
  }
  cancelEditAlias () {
    this.showEditAliasForm = false
  }
  openEditAliasForm () {
    this.showEditAliasForm = true
    this.currentEditAlias = this.currentEditTable.alias
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
  dragColumnEnter (event, t) {
    if (t.guid === this.currentDragColumnData.guid) {
      return
    }
    var target = event.currentTarget
    $(target).addClass('drag-column-in')
  }
  dragColumnLeave (event) {
    var target = event.currentTarget
    $(target).removeClass('drag-column-in')
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
          left: e.offsetX - this.modelRender.zoomXSpace,
          top: e.offsetY - this.modelRender.zoomYSpace
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
    let joinDialogOption = {
      fid: this.currentDragColumnData.guid,
      pid: table.guid,
      fColumnName: fTable.alias + '.' + this.currentDragColumnData.columnName,
      // pColumnName: table.alias + '.' + col.name,
      tables: this.modelRender.tables
    }
    if (col) {
      joinDialogOption.pColumnName = table.alias + '.' + col.name
    }
    this.callJoinDialog(joinDialogOption)
  }
  // 释放列
  dropColumnToPanel (event, type) {
    this.removeDragInClass()
    let guid = this.currentDragColumnData.guid
    let table = this.modelInstance.getTableByGuid(guid)
    if (!table) {
      return
    }
    let alias = table.alias
    let fullName = alias + '.' + this.currentDragColumnData.columnName
    if (type === 'dimension') {
      this.showSingleDimensionDialog({
        dimension: {
          column: fullName
        },
        modelInstance: this.modelInstance
      })
    } else if (type === 'measure') {
      this.measureObj = {
        name: '',
        expression: 'SUM(column)',
        parameterValue: {type: 'column', value: fullName, table_guid: null},
        convertedColumns: [],
        return_type: ''
      }
      this.measureVisible = true
      this.isEditMeasure = false
    }
  }
  callJoinDialog (data) {
    return new Promise((resolve, reject) => {
      // 弹出框弹出
      this.showJoinDialog(data).then(({isSubmit, data}) => {
        // 保存的回调
        if (isSubmit) {
          resolve(data)
          this.saveLinkData(data)
        }
      })
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
    $(this.$el).find('.drag-column-in').removeClass('drag-column-in')
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
  allowDropColumnToPanle (e) {
    e.preventDefault()
    var target = e.srcElement ? e.srcElement : e.target
    if (this.currentDragColumn) {
      $(target).parents('.panel-box').find('.panel-main-content').addClass('drag-in')
    }
  }
  dragLeave (e) {
    e.preventDefault()
    this.removeDragInClass()
  }
  searchHandleStart = false // 标识业务弹窗是不是通过搜索弹出的
  selectResult (e, select) {
    this.modelSearchActionSuccessTip = ''
    this.searchHandleStart = true
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
      }).then(() => {
        this.modelSearchActionSuccessTip = 'table join 条件编辑成功'
      })
    }
    if (select.action === 'tableaddjoin') {
      let pguid = moreInfo.guid
      this.callJoinDialog({
        fid: '',
        pid: pguid,
        tables: this.modelRender.tables
      }).then(() => {
        this.modelSearchActionSuccessTip = 'table join 条件添加成功'
      })
    }
    if (select.action === 'adddimension') {
      this.showSingleDimensionDialog({
        dimension: {
          column: moreInfo.full_colname
        },
        modelInstance: this.modelInstance
      }).then((res) => {
        if (res && res.isSubmit) {
          this.modelSearchActionSuccessTip = 'dimension添加成功'
        }
      })
    }
    if (select.action === 'editdimension') {
      this.showSingleDimensionDialog({
        dimension: moreInfo,
        modelInstance: this.modelInstance
      }).then((res) => {
        if (res && res.isSubmit) {
          this.modelSearchActionSuccessTip = 'dimension修改成功'
        }
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
      }).then((res) => {
        this.modelSearchActionSuccessTip = '连接条件编辑成功'
      })
    }
    if (select.action === 'addmeasure') {
      this.measureObj = {
        name: '',
        expression: 'SUM(column)',
        parameterValue: {type: 'column', value: moreInfo.full_colname, table_guid: null},
        convertedColumns: [],
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
      }).then((res) => {
        this.modelSearchActionSuccessTip = '连接条件添加成功'
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
  get tableBoxStyleNoZoom () {
    return (drawSize) => {
      if (drawSize) {
        let zoom = this.modelRender.zoom / 10
        return {'z-index': drawSize.zIndex, width: drawSize.width + 'px', height: drawSize.height + 'px', left: drawSize.left * zoom + this.modelRender.zoomXSpace + 'px', top: drawSize.top * zoom + this.modelRender.zoomYSpace + 'px'}
      }
    }
  }
  get tableBoxToolStyleNoZoom () {
    return (drawSize) => {
      if (drawSize) {
        let zoom = this.modelRender.zoom / 10
        if (drawSize.isInRightEdge) {
          return {left: drawSize.left * zoom + this.modelRender.zoomXSpace - 280 + 'px', top: drawSize.top * zoom + this.modelRender.zoomYSpace + 'px'}
        }
        return {left: this.currentEditTable.drawSize.width + drawSize.left * zoom + this.modelRender.zoomXSpace + 'px', top: drawSize.top * zoom + this.modelRender.zoomYSpace + 'px'}
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
    // console.log(this.extraoption)
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
@fact-title-color: @base-color;
@lookup-title-color: @base-color-12;
@fact-shadow: 0 0 4px 0 @base-color;
@fact-hover-shadow: 0 0 8px 0 @base-color;
@lookup-shadow:0 0 4px 0 @base-color-12;
@lookup-hover-shadow: 0 0 8px 0 @base-color-12;
@--index-normal: 1;
.drag-in {
  box-shadow: inset 0 0 14px 0 @base-color;
}
.fast-action-popper {
  z-index:100001!important;
}
.jtk-overlay {
  background-color: @base-color;
  padding: 2px;
  font-size: 12px;
  z-index: 21;
  font-weight: @font-medium;
  border: 2px solid @base-color;
  cursor: pointer;
  min-width: 32px;
  height: 15px;
  border-radius: 10px;
  text-align: center;
  line-height: 15px;
  color:@fff;

  &:hover {
    height: 17px;
    line-height: 17px;
    font-size:13px;
    background-color:@grey-4;
    color:@fff;
    background-color: @base-color-11;
    border: 2px solid @base-color-11;
  }
}
.drag-column-in {
 background-color: @base-color-10;
 box-shadow: 2px 2px 4px 0 @text-secondary-color;
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
  .slide-fade-enter-active {
    transition: all .3s;
  }
  .slide-fade-leave-active {
    transition: all 0s cubic-bezier(1.0, 0.5, 0.8, 1.0);
  }
  .slide-fade-enter, .slide-fade-leave-to{
    transform: translateX(-10px);
    opacity: 0;
  }
  .full-screen-cover {
    position: fixed;
    top:0;
    left:0;
    right:0;
    bottom:0;
    // background-color: #000;
    z-index: 99999;
    background-color: rgba(24, 32, 36, 0.7);
  }
  .fast-action-box {
    width:260px;
    &.edge-right {
      text-align: right;
    }
    color:@fff;
    position: absolute;
    z-index: 100001;
    margin-left:10px;
    div {
      margin-bottom:5px;
    }
    div.alias-form{
      .el-input {
        width:140px;
      }
      .el-button+.el-button {
        margin-left:5px;
      }
    }
    div.action {
      display: inline-block;
      border-radius: 2px;
      background:black;
      color:@fff;
      height:24px;
      padding-left:5px;
      padding-right:6px;
      font-size:12px;
      line-height:25px;
      cursor:pointer;
      margin-left:0;
      transform: margin-left ease;
      &:hover {
        margin-left: 4px;
      }
    }
  }
  .fast-action-temp-table {
    z-index:100000!important;
  }
  border-top:@text-placeholder-color;
  user-select:none;
  overflow:hidden;
  .box-css();
  height: calc(~"100% - 34px");
  .panel-box{
      box-shadow: @box-shadow;
      position:relative;
      width:250px;
      .panel-title {
        background:@text-normal-color;
        height:28px;
        color:#fff;
        font-size:14px;
        line-height:28px;
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
        top:62px;
        right:0;
        left:0;
        ul {
          list-style: circle;
          margin-top:15px;
          margin-bottom:17px;
        }
        .dimension-list , .measure-list, .cc-list{
          cursor:default;
          margin-top:0;
          li {
            line-height:28px;
            height:28px;
            padding: 0 7px 0px 10px;
            border-bottom: 1px solid @text-placeholder-color;
            box-sizing: border-box;
            .li-type{
              position:absolute;
              right:4px;
              font-size:12px;
              color:@text-disabled-color;
            }
            .col-name {
              color:@text-title-color;
            }
            .icon-group {
              position: absolute;
              right: 7px;
            }
            .icon-span {
              display:none;
              margin-left:7px;
              float:right;
              font-size:14px;
            }
            &.is-checked {
              background-color:@base-color-10;
            }
            &:hover {
              .li-type{
                display:none;
              }
              background-color:@base-color-10;
              .icon-span{
                .ky-square-box(22px,22px);
                display:inline-block;
                margin-top: 3px;
                border-radius: 2px;
                &:hover {
                  background-color: @text-placeholder-color;
                  color: @base-color;
                }
              }
            }
          }
        }
        .cc-list {
          li {
            background-color:@warning-color-2;
            &:hover,
            &.is-checked {
              background-color:@warning-color-3;
            }
          }
        }
      }
      .panel-sub-title {
        height:35px;
        background:@fff;
        line-height:34px;
        border-bottom: 1px solid @text-placeholder-color;
        padding: 1px;
        box-sizing: border-box;
        position: relative;
        overflow-x: hidden;
        .action_group,
        .batch_group {
          font-size: 0;
          display: flex;
          position: absolute;
          width: 248px;
          top: 1px;
          z-index: @--index-normal - 1;
          transition: transform .4s ease-in-out;
          &.is_active {
            transition: transform .4s ease-in-out;
            z-index: @--index-normal + 1;
          }
          .action_btn {
            height: 32px;
            flex: 1 0 33.1%;
            font-size: 14px;
            display: inline-block;
            border-right: 1px solid @fff;
            background-color: @grey-3;
            color:  @base-color;
            text-align: center;
            cursor: pointer;
            i {
              display: inline;
            }
            span {
              display: none;
              font-size: 12px;
            }
            &:last-child {
              border-right: 0;
            }
            &:hover {
              background-color: @base-color;
              color: @fff;
              i {
                display: none;
              }
              span {
                display: inline;
              }
            }
            &.disabled,
            &.disabled:hover {
              background-color: @line-border-color;
              color: @text-disabled-color;
              cursor: not-allowed;
              i {
                cursor: not-allowed;
              }
            }
          }
        }
        .batch_group .action_btn {
          background-color: @base-color;
          color: @fff;
          &:hover {
            background-color: @base-color-11;
          }

        }
      }
      background:#fff;
      position:absolute;
    }
    .panel-datasource {
      .tree-box {
        width:100%;
        .body{
          width:100%;
          padding:10px;
        }
      }
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
      &.full-screen {
        top:88px!important;
      }
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
      .search-action-result {
        width:783px;
        // margin: 0 auto;
        top: 90px;
        position: absolute;
        left: 50%;
        margin-left: -392px;
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
  .model-edit-outer{
    .model-edit {
      height: 100%;
      position:relative;
    }
    .box-css();
    .table-box {
      &.isLookup {
        box-shadow:@lookup-shadow;
        &:hover {
          box-shadow:@lookup-hover-shadow;
        }
        .table-title {
          background-color: @lookup-title-color;
          color:@fff;
          .close {
            &:hover{
              background-color:@base-color-14;
            }
          }
        }
      }
      background-color:#fff;
      position:absolute;
      box-shadow:@fact-shadow;
      &:hover {
        box-shadow:@fact-hover-shadow;
      }
      // overflow: hidden;
      .table-title {
        .close {  
          float:right;
          font-size:14px;
          width:20px;
          height:20px;
          line-height:20px;
          text-align: center;
          margin-top: 6px;
          margin-right: 3px;
          &:hover {
            background-color:@base-color-11;
          }
          i {
            margin: auto;
            color:@fff;
          }
        }
        .name {
          &.tip_box {
            .alias-span {
              font-size: 14px;
              font-weight: @font-medium;
            }
          }
          text-overflow: ellipsis;
          overflow: hidden;
          width:calc(~"100% - 50px");
        }
        span {
          width:24px;
          height:24px;
          float:left;
        }
        .kind {
          cursor:move;
        }
        .kind:hover {
          // background-color:@base-color;
          color:@grey-3;
        }
        height:32px;
        background-color: @fact-title-color;
        color:#fff;
        line-height:32px;
        i {
          color:@fff;
          margin: auto 6px 8px;
        }
      }
      .column-list-box {
        overflow:auto;
        position:absolute;
        top:32px;
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
            border-bottom:solid 1px @text-placeholder-color;
            height:28px;
            line-height:28px;
            font-size:14px;
            .col-type-icon {
              color:@text-disabled-color;
              font-size:12px;
            }
            &.column-li-cc {
              background-color:@warning-color-2;
              &:hover {
                background-color:@warning-color-3;
              }
            }
          }
        }
      }
    }
  }

</style>
