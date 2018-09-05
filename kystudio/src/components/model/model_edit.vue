<template>
<div class="model_edit_box"  @drop='drop($event)' @dragover='allowDrop($event)' :style="{width:dockerScreen.w+'px', height:dockerScreen.h+'px'}">
    <!-- 左侧树 -->
    <div class="tree_list"  v-show="showTree">
      <div class='treeBtnRight tree-menu-toggle' @click='showTree=false'>
        <i class='el-icon-d-arrow-left' aria-hidden='true'></i>
      </div>
      <model-assets  v-on:drag="drag" :project="extraoption.project" @okFunc="serverDataToDragData" ></model-assets>
    </div>
    <div class='treeBtnLeft tree-menu-toggle' v-show="!showTree" @click='showTree=true'>
      <i class='el-icon-d-arrow-right' aria-hidden='true'></i>
    </div>
    <!-- 图例 -->
    <kap-model-edit-illustrate></kap-model-edit-illustrate>
    <!-- 辅助操作工具栏 -->
    <kap-model-edit-assist-tool :currentZoom="currentZoom" @addZoom="addZoom" @subZoom="subZoom" @autoLayerPosition="autoLayerPosition" @toggleFullScreen="toggleFullScreen"></kap-model-edit-assist-tool>
    <!-- 空画布 -->
    <div class="notable_tip" v-show="!(tableList&&tableList.length)">
      <img src="../../assets/img/dragtable.png">
      <div style="font-size:14px;color:#4f5473" class="ksd-mt-20">{{$t('kylinLang.model.dragTip')}}</div>
    </div>
    <!-- 保存按钮 -->
    <div class="btn_group"  v-if="actionMode!=='view'">
      <el-button type="primary" @click="saveCurrentModel" :loading="saveBtnLoading">{{$t('kylinLang.common.save')}}</el-button>
    </div>


    <div class="model_edit" :style="{left:docker.x +'px'  ,top:docker.y + 'px'}">
      <div class="table_box" v-if="table&&table.kind" @drop='dropColumn' @dragover='allowDrop($event)'  v-for="table in tableList" :key="table.guid" :id="table.guid" v-bind:class="table.kind.toLowerCase()" v-bind:style="{ left: table.pos.x + 'px', top: table.pos.y + 'px' }" >
        <kap-model-table-assist-tool :table="table" :mode="actionMode" @toggleMutilSelect="toggleMutilSelect" @openModelSubMenu="openModelSubMenu" @addComputedColumn="addComputedColumn" @sortColumns="sortColumns" @inputSql="inputSql" @selectTableKind="selectTableKind"></kap-model-table-assist-tool>
        <i class="el-icon-close close_table" v-on:click="removeTable(table.guid)" v-if="actionMode!=='view'"></i>
        <p class="table_name  drag_bar" v-on:dblclick="editAlias(table.guid)" v-visible="aliasEditTableId!=table.guid">
        <common-tip :tips="table.database+'.'+table.name" class="drag_bar">{{(table.alias)|omit(16,'...')}}</common-tip></p>
        <el-input v-model="table.alias" v-on:blur="cancelEditAlias(table.guid)" class="alias_input"  size="small" :placeholder="$t('kylinLang.common.enterAlias')" v-visible="aliasEdit&&aliasEditTableId==table.guid"></el-input>
        <p class="filter_box"><el-input v-model="table.filterName" v-on:input="filterColumnByInput1(table.filterName,table.guid)"  size="small" :placeholder="$t('kylinLang.common.pleaseFilter')"></el-input></p>
        <kap-model-table-batch-tool :table="table" @checkAllColumns="checkAllColumns" @changeSelectedColumnKind="changeSelectedColumnKind" @autoSuggestDM="autoSuggestDM"></kap-model-table-batch-tool>
        <section data-scrollbar  class="columns_box">
          <ul class="columns-body">
            <li draggable v-on:click="selectFilterColumn(table.guid,column.name,column.datatype)"  @dragstart="dragColumns" @dragend="dragColumnsEnd"  v-for="column in table.columns" :key="column.guid"  class="column_li" v-show="column.isShow!==false"  v-bind:class="{'active_filter':column.isActive && !table.openMutilSelected, 'is_computed': column.isComputed, 'selected': column.isSelected, 'pointer': table.openMutilSelected}" :data-guid="table.guid" :data-btype="column.btype" :data-isComputed="column.isComputed" :data-column="column.name" @dragenter="dragColumnsEnter" @dragleave="dragColumnsLeave" >
              <span class="kind" :class="{dimension:column.btype=='D',measure:column.btype=='M'}" v-on:click.stop="changeColumnBType(table.guid,column.name,column.btype, column.isComputed)">{{column.btype}}</span>
              <span class="column"  >
                <common-tip trigger="click" :tips="column.name" placement="right-start" style="font-size:10px;">{{column.name|omit(14,'...')}}</common-tip>
              </span>
              <span class="column_type">{{column.datatype}}</span>
            </li>
          </ul>
        </section>
        <div class="more_tool"></div>
      </div>
    </div>

    <model-tool @changeColumnType="changeColumnBType" v-if="modelDataLoadEnd" :modelInfo="modelInfo" :actionMode="actionMode" :editLock="editLock" :compeleteModelId="modelData&&modelData.uuid||null" :columnsForTime="timeColumns" :columnsForDate="dateColumns"  :activeName="submenuInfo.menu1" :activeNameSub="submenuInfo.menu2" :tableList="tableList" :partitionSelect="partitionSelect"  :selectTable="currentSelectTable" ref="modelsubmenu" :sqlString="sqlString"
    :checkModel="checkModel" :hasStreamingTable="hasStreamingTable" @changeIsSnapshot="changeSnapshotStatus">
    </model-tool>


     <el-dialog :title="$t('addJoinCondition')" width="660px" :visible.sync="dialogVisible" class="links_dialog" @close="saveLinks(currentLinkData.source.guid,currentLinkData.target.guid, true)" :close-on-press-escape="false" :close-on-click-modal="false">
        <span>
            <br/>
             <el-row :gutter="10" class="ksd-mb10" >
             <el-col :span="9" style="text-align:center">
              <div class="grid-content bg-purple">
              <el-input v-model="currentLinkData.source.alias" :disabled="true"></el-input>
                <!-- <a style="color:#56c0fc">{{currentLinkData.source.alias}}</a> -->
              </div>
            </el-col>
            <el-col :span="6" style="text-align:center">
              <div class="grid-content bg-purple">
                  <el-select v-model="currentLinkData.joinType"  :disabled = "actionMode ==='view'"  @change="modelParserTool.editJoinType(currentLinkData.source.guid,currentLinkData.target.guid, currentLinkData.joinType)" :placeholder="$t('kylinLang.common.pleaseSelect')">
                    <el-option
                      v-for="item in joinTypes"
                      :key="item.label"
                      :label="item.label"
                      :value="item.value">
                    </el-option>
                  </el-select>
              </div>
            </el-col>
            <el-col :span="9" style="text-align:center">
              <div class="grid-content bg-purple">
              <el-input v-model="currentLinkData.target.alias" :disabled="true"></el-input>
              </div>
            </el-col>
            </el-row>
          </el-row>

          <el-button icon="el-icon-plus" type="primary" size="small" class="ksd-mt-20" v-if="actionMode!=='view'" @click="addJoinCondition(currentLinkData.source.guid,currentLinkData.target.guid, '', '', currentLinkData.joinType,true)">{{$t('addJoinConditionBtn')}}</el-button>
          <el-row :gutter="10" v-for="link in currentTableLinks" :key="link.join(',')" class="ksd-mt-20">
            <el-col :span="10">
               <el-select v-model="link[2]" style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled = "actionMode ==='view'">
                  <el-option
                    v-for="item in currentLinkData.source.columns"
                    :key="item.name"
                    :label="item.name"
                    :value="item.name">
                  </el-option>
                </el-select>
            </el-col>
            <el-col :span="1" class="ksd-center" style="font-size:20px;">
               =
            </el-col>
            <el-col :span="10">
              <el-select v-model="link[3]"  style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelect')"  :disabled = "actionMode ==='view'">
                  <el-option
                    v-for="item in currentLinkData.target.columns"
                    :key="item.name"
                    :label="item.name"
                    :value="item.name">
                  </el-option>
                </el-select>
            </el-col>
            <el-col :span="3" class="ksd-center">
              <!--   <el-button size="small" type="danger" @click="delConnect(link)">
                    {{$t('kylinLang.common.drop')}}
                </el-button> -->
                <el-button @click="delConnect(link)" icon="el-icon-delete" :disabled = "actionMode ==='view'"></el-button>

            </el-col>
          </el-row>
            <br/>

        </span>
         <span slot="footer" class="dialog-footer">
            <el-button type="primary" plain @click="saveLinks(currentLinkData.source.guid,currentLinkData.target.guid)" size="medium">{{$t('kylinLang.common.ok')}}</el-button>
          </span>
      </el-dialog>


       <el-dialog :title="$t('kylinLang.common.computedColumn')" :visible.sync="computedColumnFormVisible" :close-on-press-escape="false" :close-on-click-modal="false" class="computedColumn">
          <div>
            <p style="font-size:12px">{{$t('kylinLang.model.ccRules')}}</p>
            <p style="font-size:12px">{{$t('kylinLang.model.ccRules1')}}</p>
            <p style="font-size:12px">{{$t('kylinLang.model.ccRules2')}}</p>
            <el-button type="primary" class="ksd-mb-10 ksd-mt-20" icon="el-icon-plus" v-show="!openAddComputedColumnForm&&actionMode!=='view'" @click="addComputedForm">{{$t('kylinLang.common.computedColumn')}}</el-button>
            <el-form  :model="computedColumn"  label-width="120px" ref="computedColumnForm"  class="cc-addedit-form" :rules="computedRules" v-show="openAddComputedColumnForm" label-position="top">
              <el-form-item :label="$t('kylinLang.model.columnName')">
                <el-row :gutter="10">
                <el-col :span="12">
                  <el-form-item  prop="name">
                  <kap-filter-select
                    :disabled="isEditComputedColumn"
                    class="inline-input"
                    v-model="computedColumn.name"
                    :asyn="false"
                    @change="cleanMsg"
                    @select="handleSelect"
                    :list="renderCCList"
                    placeholder="kylinLang.common.pleaseInput" :size="100"
                    style="width: 100%;"
                   >
                    <template slot="select" slot-scope="props">
                      <span style="float: left">{{ props.item.value }}</span>
                      <span style="float: right; color: #8492a6; font-size: 13px">{{ props.item.model }}</span>
                    </template>
                  </kap-filter-select>
                </el-form-item>
                </el-col>
                <el-col :span="12">
                   <el-form-item prop="returnType">
                    <el-select v-model="computedColumn.returnType" :placeholder="$t('pleaseSelectDataType')" style="width: 100%;" size="medium">
                      <el-option
                        v-for="(item, index) in computedDataTypeSelects"
                        :key="item"
                        :label="item"
                        :value="item">
                      </el-option>
                    </el-select>
                  </el-form-item>
                </el-col>
              </el-row>
              </el-form-item>
              <el-form-item class="cc-expression" :label="$t('kylinLang.dataSource.expression')" prop="expression">
                <span slot="label">{{$t('kylinLang.dataSource.expression')}} <common-tip :content="$t('conditionExpress')" ><i class="el-icon-question"></i></common-tip></span>
                <kap_editor ref="expressionBox" height="100" width="100%" lang="sql" theme="chrome" v-model="computedColumn.expression" dragbar="#393e53">
                </kap_editor>
                <p :class="{isvalid:checkExpressResult.isValid}" v-if="checkExpressResult.msg" class="checkresult">
                 {{checkExpressResult.msg}}
                 <span @click="reuseClick" v-if="showReuseCC" style="cursor: pointer;color: #218fea;text-decoration: underline;">{{$t('kylinLang.model.reuseClick')}}</span>
                </p>
              </el-form-item>
              <el-form-item>
               <!--  <el-select class="ksd-fleft" v-model="computedColumn.returnType" :placeholder="$t('kylinLang.common.pleaseSelect')">
                  <el-option
                    v-for="(item, index) in computedDataTypeSelects"
                    :key="item"
                    :label="item"
                    :value="item">
                  </el-option>
                </el-select> -->
                 <div v-show="computedColumn.expression">
                  <el-button size="small" type="primary" @click="checkExpression" :loading="checkExpressionBtnLoad">
                    {{$t('expresscheck')}}
                    <common-tip :content="$t('longTimeTip')">
                      <i class="el-icon-question"></i>
                    </common-tip>
                  </el-button>
                  <el-button type="info" text size="mini" v-show="checkExpressionBtnLoad" @click="cancelCheckExpression" >{{$t('kylinLang.common.cancel')}}</el-button>
                </div>
              </el-form-item>
              <el-form-item>
                <div class="ksd-right">
                  <el-button  plain @click="cancelComputedEditForm" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
                  <el-button type="primary" plain @click="saveComputedColumn" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
                </div>
              </el-form-item>
                <!-- <div class="line-primary" style="margin: 0px -20px;box-shadow: 0 -1px 0 0 #424860;"></div> -->
            </el-form>
            <!-- <kap-nodata v-if="!(currentTableComputedColumns && currentTableComputedColumns.length)"></kap-nodata> -->
            <el-table border
              :data="currentTableComputedColumns"
              style="width: 100%" class="ksd-mt-20">
              <el-table-column
                show-overflow-tooltip
                prop="columnName"
                :label="$t('kylinLang.dataSource.columns')"
                width="180">
              </el-table-column>
              <el-table-column
                prop="expression"
                show-overflow-tooltip
                :label="$t('kylinLang.dataSource.expression')"
                >
              </el-table-column>
              <el-table-column
              width="100"
                prop="datatype"
              show-overflow-tooltip
                :label="$t('kylinLang.dataSource.returnType')">
              </el-table-column>
              <el-table-column
                 width="140"
                :label="$t('kylinLang.common.action')" v-if="actionMode!=='view'">
                <template slot-scope="scope">
                  <el-button  icon="el-icon-edit" style="margin-right: 5px;" size="small" v-on:click='editComputedColumn(scope.row)'></el-button>
                   <confirm-btn  v-on:okFunc='delComputedColumn(scope.row)' :tips="$t('kylinLang.common.confirmDel')">
                   <el-button size="small" icon="el-icon-delete" ></el-button></confirm-btn>
                </template>
              </el-table-column>
            </el-table>
          </div>
            <span slot="footer" class="dialog-footer">
            <el-button @click="computedColumnFormVisible = false" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
            <el-button type="primary" plain @click="computedColumnFormVisible = false" size="medium">{{$t('kylinLang.common.ok')}}</el-button>
          </span>
        </el-dialog>
    <el-dialog :title="$t('kylinLang.common.save')"  width="660px" :visible.sync="addModelDialogDisable" :close-on-press-escape="false" :close-on-click-modal="false" class="saveModel">
      <partition-column :comHeight="450" :modelInfo="modelInfo" :actionMode="actionMode" :columnsForTime="timeColumns" :columnsForDate="dateColumns" :tableList="tableList" :partitionSelect="partitionSelect" :editLock="editLock" :checkModel="checkModel" :hasStreamingTable="hasStreamingTable" :showModelCheck="true" labelPos="top"  leftBoxWidth="100%" rightBoxWidth="85%"></partition-column>
      <div slot="footer" class="dialog-footer">
        <el-button @click="addModelDialogDisable = false" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="saveAndCheckModel" :loading="saveBtnLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog :title="$t('sqlPatterns')" :visible.sync="addSQLFormVisible" :before-close="sqlClose" :close-on-press-escape="false" :close-on-click-modal="false">
    <p style="font-size:12px">{{$t('autoModelTip1')}}</p>
    <p style="font-size:12px">{{$t('autoModelTip2')}}</p>
    <p style="font-size:12px">{{$t('autoModelTip3')}}</p>
    <div :class="{hasCheck: hasCheck}">
      <kap_editor ref="sqlbox" class="ksd-mt-20" height="200" width="100%" lang="sql" theme="chrome" v-model="sqlString" dragbar="#393e53">
      </kap_editor>
    </div>
    <div class="ksd-mt-4">
      <el-button :loading="checkSqlLoadBtn" type="primary" size="medium" @click="validateSql" :disabled="actionMode==='view' || sqlString === ''" >{{$t('kylinLang.common.check')}}</el-button>
      <el-button type="info" text v-show="checkSqlLoadBtn" size="medium" @click="cancelCheckSql">{{$t('kylinLang.common.cancel')}}</el-button>
    </div>
    <transition name="fade">
      <div v-if="errorMsg">
        <el-alert class="ksd-mt-10"
        show-icon
        :closable="false"
        type="error">
        <p v-html="errorMsg.replace(/\n/g,'<br/>')"></p>
       </el-alert>
      </div>
    </transition>
    <transition name="fade">
      <div v-if="successMsg">
       <el-alert class="ksd-mt-10"
          :title="successMsg"
          show-icon
          :closable="false"
          type="success">
        </el-alert>
      </div>
    </transition>

    <span slot="footer" class="dialog-footer">
      <el-checkbox class="ksd-fleft" v-model="ignoreErrorSql" v-show="hasValidFailSql && hasValidSuccessSql">{{$t('ignoreErrorSqls')}}</el-checkbox>
      <common-tip :content="$t('ignoreTip')" class="ksd-fleft ksd-ml-4" v-show="hasValidFailSql && hasValidSuccessSql">
        <i class="el-icon-question"></i>
      </common-tip>
      <el-button @click="sqlClose()" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" plain :loading="sqlBtnLoading" size="medium" @click="submitSqlPatterns" :disabled="actionMode==='view' || !hasValidSuccessSql || !hasCheck" >{{$t('kylinLang.common.submit')}}</el-button>
    </span>
  </el-dialog>
</div>
</template>
<script>
// import { jsPlumb } from 'jsplumb'
import { indexOfObjWithSomeKey, objectArraySort, getNextValInArray, isIE } from 'util/index'
import { NamedRegex, DatePartitionRule, permissions, TimePartitionRule, IntegerType, computedDataType } from 'config/index'
import { modelParser } from 'util/metadata/model_parser.js'
import { modelRenderConfig } from 'util/metadata/model_render_config'
import { mapActions, mapGetters } from 'vuex'
import $ from 'jquery'
import Scrollbar from 'smooth-scrollbar'
import modelassets from 'components/model/model_assets'
// import Draggable from 'draggable'
import modelEditTool from 'components/model/model_edit_panel'
import partitionColumn from 'components/model/model_partition.vue'
import modelEditIllustrate from 'components/model/model_edit_spare/illustrate.vue'
import modelEditAssistTool from 'components/model/model_edit_spare/assist_tool.vue'
import modelTableAssistTool from 'components/model/model_edit_spare/table_tool.vue'
import modelTableBatchTool from 'components/model/model_edit_spare/table_batch_tool.vue'
import { handleSuccess, handleError, hasRole, filterNullValInObj, kapConfirm, hasPermission, filterMutileSqlsToOneLine, jsPlumbTool } from 'util/business'
export default {
  name: 'modeledit',
  components: {
    'model-assets': modelassets,
    'model-tool': modelEditTool,
    'partition-column': partitionColumn,
    'kap-model-edit-illustrate': modelEditIllustrate,
    'kap-model-edit-assist-tool': modelEditAssistTool,
    'kap-model-table-assist-tool': modelTableAssistTool,
    'kap-model-table-batch-tool': modelTableBatchTool
  },
  props: ['extraoption'],
  data () {
    return {
      plumbTool: jsPlumbTool(),
      modelParserTool: modelParser(),
      showUnSelectTip: false,
      computedDataTypeSelects: computedDataType,
      showTree: true,
      firstLoad: false,
      ignoreErrorSql: false,
      sqlBtnLoading: false,
      addSQLFormVisible: false,
      checkSqlLoadBtn: false,
      hasValidFailSql: false,
      hasValidSuccessSql: false,
      errorMsg: '',
      successMsg: '',
      hasCheck: false,
      sqlString: '',
      oldSqlString: '',
      result: [],
      checkExpressionBtnLoad: false,
      checkExpressResult: {},
      currentTableComputedColumns: [],
      isEditComputedColumn: false,
      modelDataLoadEnd: false,
      openAddComputedColumnForm: false,
      // createCubeVisible: false,
      modelStaticsRange: 100,
      addModelDialogDisable: false,
      isFullScreen: false,
      hasStreamingTable: false,
      actionMode: 'edit',
      firstRenderServerData: false,
      // table 添加 修改类型  修改别名
      saveBtnLoading: false,
      draftBtnLoading: false,
      modelData: null,
      draftData: null,
      modelInfo: {
        uuid: null,
        last_modified: '',
        filterStr: '',
        owner: '',
        modelName: '',
        modelDiscribe: '',
        computed_columns: [],
        partition_desc: {
          partition_date_column: '',
          partition_time_column: null,
          partition_date_start: 0,
          partition_date_format: '',
          partition_time_format: ''
        },
        smart_model: false,
        smart_model_sqls: []
      },
      // encodings: loadBaseEncodings(this.$store.state.datasource),
      partitionSelect: {
        'date_table': '',
        'date_column': '',
        'time_table': '',
        'time_column': '',
        'mutilLevel_table': '',
        'mutilLevel_column': '',
        'partition_date_column': '',
        'partition_time_column': '',
        'partition_date_start': 0,
        'partition_date_format': '',
        'partition_time_format': '',
        'partition_type': 'APPEND'
      },
      checkModel: {
        openModelCheck: this.selectedProjectDatasource === '0',
        modelStatus: this.selectedProjectDatasource === '0',
        factTable: this.selectedProjectDatasource === '0',
        lookupTable: this.selectedProjectDatasource === '0',
        checkList: 0
      },
      submenuInfo: {
        menu1: 'first',
        menu2: 'first'
      },
      currentSelectTable: {
        database: '',
        tablename: '',
        columnname: ''
      },
      columnSort: ['order', 'reversed', 'restore'],
      columnBType: modelRenderConfig.columnType,
      joinTypes: modelRenderConfig.joinKindSelectData,
      timerST: null,
      hisModelJsonStr: '',
      modelAssets: [],
      currentZoom: 0.8,
      currentDragDom: null,
      dateColumns: {},
      timeColumns: {},
      currentDragData: {
        table: '',
        columnName: ''
      },
      currentDropDom: null,
      currentDropData: {
        table: '',
        columnName: ''
      },
      currentLinkData: {
        source: {
          guid: '',
          database: '',
          name: '',
          alias: '',
          columns: []
        },
        target: {
          guid: '',
          database: '',
          name: '',
          alias: '',
          columns: []
        },
        joinType: 'inner'
      },
      joinType: false,
      aliasEdit: false,
      aliasEditTableId: '',
      switchWidth: 100,
      dialogVisible: false,
      computedColumnFormVisible: false,
      selectColumn: {},
      plumbInstance: null,
      plumbInstanceForShowLink: null,
      links: [],
      project: '',
      cubesList: [],
      showLinkCons: {},
      currentTableLinks: [],
      pointType: 'source',
      columnBussiTypeMap: {},
      tableList: [],
      dragType: '',
      docker: {
        x: -20000,
        y: -20000
      },
      dockerScreen: {
        w: 1024,
        h: 1000
      },
      computedColumn: {
        guid: '',
        name: '',
        expression: '',
        returnType: ''
      },
      reuseCC: {
        name: '',
        expression: '',
        isreused: false,
        msg: ''
      },
      ccArray: {},
      showReuseCC: false,
      currentMenuStatus: 'hide',
      firstLoadCC: true,
      columnUsedInfo: [],
      noMoreAlert: false
    }
  },
  beforeDestroy () {
    this.modelParserTool.deleteAllEndPoints()
  },
  methods: {
    ...mapActions({
      suggestDM: 'SUGGEST_DIMENSION_MEASURE',
      getModelByModelName: 'LOAD_MODEL_INFO',
      saveModel: 'SAVE_MODEL',
      saveModelDraft: 'SAVE_MODEL_DRAFT',
      updateModel: 'CACHE_UPDATE_MODEL_EDIT',
      diagnose: 'DIAGNOSE',
      cacheModelEdit: 'CACHE_MODEL_EDIT',
      getUsedCols: 'GET_USED_COLS',
      getCubesList: 'GET_CUBES_LIST',
      checkCubeName: 'CHECK_CUBE_NAME_AVAILABILITY',
      statsModel: 'COLLECT_MODEL_STATS',
      checkComputedExpression: 'CHECK_COMPUTED_EXPRESSION',
      getComputedColumns: 'GET_COMPUTED_COLUMNS',
      autoModelApi: 'AUTO_MODEL',
      checkSql: 'VALID_AUTOMODEL_SQL',
      getAutoModelSql: 'GET_AUTOMODEL_SQL'
    }),
    getDimensions: function (ignoreComputed) {
      var resultArr = []
      for (var i = 0; i < this.tableList.length; i++) {
        // 排除未和任何表进行关联的loolup表
        if (!this.modelParserTool.getConnectsByTableId(this.tableList[i].guid) && this.tableList[i].kind !== 'ROOTFACT' && this.tableList[i].kind !== 'FACT') {
          continue
        }
        var obj = {
          table: this.tableList[i].alias,
          columns: []
        }
        var columns = this.tableList[i].columns
        var len = columns && columns.length || 0
        for (var j = 0; j < len; j++) {
          if (columns[j].btype === 'D') {
            if (ignoreComputed && columns[j].isComputed) {
              continue
            }
            obj.columns.push(columns[j].name)
          }
        }
        if (obj.columns.length) {
          resultArr.push(obj)
        }
      }
      return resultArr
    },
    getMeasures: function (ignoreComputed) {
      var resultArr = []
      for (var i = 0; i < this.tableList.length; i++) {
        if (!this.modelParserTool.getConnectsByTableId(this.tableList[i].guid) && this.tableList[i].kind !== 'ROOTFACT' && this.tableList[i].kind !== 'FACT') {
          continue
        }
        var columns = this.tableList[i].columns
        var len = columns && columns.length || 0
        for (var j = 0; j < len; j++) {
          if (columns[j].btype === 'M') {
            if (ignoreComputed && columns[j].isComputed) {
              continue
            }
            resultArr.push(this.tableList[i].alias + '.' + columns[j].name)
          }
        }
      }
      return resultArr
    },
    // 切换多列操作模式
    toggleMutilSelect (tableInfo) {
      this.$set(tableInfo, 'openMutilSelected', !tableInfo.openMutilSelected)
      tableInfo.allChecked = false
      this.checkAllColumns(tableInfo)
    },
    checkAllColumns (tableInfo) {
      if (tableInfo) {
        this.$set(tableInfo, 'mutilSelectedList', [])
        tableInfo.columns.forEach((co) => {
          if (tableInfo.allChecked) {
            tableInfo.mutilSelectedList.push(co.name)
          }
          this.modelParserTool.editTableColumnInfo(tableInfo.guid, 'name', co.name, 'isSelected', tableInfo.allChecked)
        })
      }
    },
    // 修改列类型 D、M、-
    changeSelectedColumnKind (tableInfo, bType) {
      if (tableInfo && tableInfo.mutilSelectedList && tableInfo.mutilSelectedList.length) {
        tableInfo.mutilSelectedList.forEach((name) => {
          if (!this.modelParserTool.isColumnUsedInConnection(tableInfo.guid, name)) {
            this.modelParserTool.editTableColumnInfo(tableInfo.guid, 'name', name, 'btype', bType)
          }
        })
      } else {
        this.$set(tableInfo, 'showUnSelectTip', true)
      }
    },
    autoSuggestDM (tableInfo) {
      if (!tableInfo.mutilSelectedList || tableInfo.mutilSelectedList && tableInfo.mutilSelectedList.length === 0) {
        return
      }
      kapConfirm(this.$t('autoSuggestConfirm')).then(() => {
        if (tableInfo) {
          var needAutoSuggestList = tableInfo.mutilSelectedList.filter((name) => {
            if (this.modelParserTool.isColumnUsedInConnection(tableInfo.guid, name)) {
              return false
            }
            return true
          })
          this.suggestColumnDtype(tableInfo, needAutoSuggestList)
        }
      })
    },
    resetModelEditArea () {
      this.currentTableComputedColumns = []
      this.modelInfo = {
        uuid: null,
        last_modified: '',
        filterStr: '',
        owner: '',
        modelName: '',
        modelDiscribe: '',
        computed_columns: [],
        partition_desc: {
          partition_date_column: '',
          partition_time_column: null,
          partition_date_start: 0,
          partition_date_format: '',
          partition_time_format: ''
        },
        smart_model: false,
        smart_model_sqls: []
      }
      this.currentSelectTable = {
        database: '',
        tablename: '',
        columnname: ''
      }
      this.tableList = []
    },
    sqlClose () {
      if (this.actionMode === 'view') {
        this.addSQLFormVisible = false
        return
      }
      kapConfirm(this.$t('kylinLang.common.willClose'), {
        confirmButtonText: this.$t('kylinLang.common.continue'),
        cancelButtonText: this.$t('kylinLang.common.cancel')
      }).then(() => {
        this.sqlString = this.oldSqlString
        this.addSQLFormVisible = false
      })
    },
    sortModelData: function (modelDesc) {
      let modelData = {
        'dimensions': modelDesc.dimensions.sort(),
        'fact_table': modelDesc.fact_table,
        'lookups': modelDesc.lookups.sort(),
        'metrics': modelDesc.metrics.sort(),
        'computed_columns': modelDesc.computed_columns.sort()
      }
      modelData.dimensions.forEach((dimension) => {
        if (dimension.columns.length > 0) {
          dimension.columns.sort()
        }
      })
      return modelData
    },
    submitSqlPatterns () {
      if (this.ignoreErrorSql === false && this.hasValidSuccessSql && this.hasValidFailSql) {
        this.$message({
          message: this.$t('checkIgnore'),
          type: 'warning',
          showClose: true,
          customClass: 'alertSQL',
          duration: 0
        })
        return
      }
      var sqls = filterMutileSqlsToOneLine(this.sqlString)
      if (sqls.length === 0) {
        return
      }
      this.sqlBtnLoading = true
      var rootFact = this.modelParserTool.getRootFact()
      if (rootFact.length) {
        var rootFactName = rootFact[0].database + '.' + rootFact[0].name
        this.autoModelApi({ modelName: this.modelInfo.modelName, project: this.project, sqls: sqls, factTable: rootFactName }).then((res) => {
          handleSuccess(res, (data) => {
            this.sqlBtnLoading = false
            let sqlModel = this.sortModelData(data)
            let jsonData = this.DragDataToServerData(true)
            let nowModel = this.sortModelData(jsonData)
            if (JSON.stringify(sqlModel) !== JSON.stringify(nowModel)) {
              kapConfirm(this.$t('submitSqlTip'), {
                confirmButtonText: this.$t('kylinLang.common.continue')
              }).then(() => {
                this.renderAutoModel(data)
              })
            } else {
              this.addSQLFormVisible = false
            }
          })
        }, (res) => {
          this.sqlBtnLoading = false
          handleError(res)
        })
      }
    },
    renderAutoModel (data) {
      data.last_modified = this.modelInfo.last_modified
      data.uuid = this.modelInfo.uuid
      this.$set(data, 'smart_model', this.modelInfo.smart_model)
      this.$set(data, 'smart_model_sqls', this.modelInfo.smart_model_sqls)
      this.modelParserTool.removeAllConnections()
      // 清空画布数据
      this.resetModelEditArea()
      this.$nextTick(() => {
        // 重新绘制
        this.loadEditData(data)
        this.addSQLFormVisible = false
      })
    },
    autoModel () {
      var sqls = filterMutileSqlsToOneLine(this.sqlString)
      if (sqls.length === 0) {
        return
      }
      this.sqlBtnLoading = true
      var rootFact = this.modelParserTool.getRootFact()
      if (rootFact.length) {
        var rootFactName = rootFact[0].database + '.' + rootFact[0].name
        this.autoModelApi({ modelName: this.modelInfo.modelName, project: this.project, sqls: sqls, factTable: rootFactName }).then((res) => {
          handleSuccess(res, (data) => {
            this.sqlBtnLoading = false
            this.addSQLFormVisible = false
            this.modelParserTool.removeAllConnections()
            delete data.uuid
            delete data.last_modified
            data.last_modified = this.modelInfo.last_modified
            data.uuid = this.modelInfo.uuid
            // 清空画布数据
            this.resetModelEditArea()
            this.$nextTick(() => {
              // 重新绘制
              this.loadEditData(data)
            })
          })
        }, (res) => {
          this.sqlBtnLoading = false
          handleError(res)
        })
      }
    },
    validateSql () {
      var sqls = filterMutileSqlsToOneLine(this.sqlString)
      if (sqls.length === 0) {
        return
      }
      var editor = this.$refs.sqlbox && this.$refs.sqlbox.$refs.kapEditor.editor || ''
      this.renderEditerRender(editor)
      editor && editor.removeListener('change', this.editerChangeHandle)
      this.errorMsg = false
      this.checkSqlLoadBtn = true
      editor.setOption('wrap', 'free')
      this.sqlString = sqls.length > 0 ? sqls.join(';\r\n') + ';' : ''
      var rootFact = this.modelParserTool.getRootFact()
      if (rootFact.length) {
        var rootFactName = rootFact[0].database + '.' + rootFact[0].name
        this.checkSql({ modelName: this.modelInfo.modelName, project: this.project, sqls: sqls, factTable: rootFactName }).then((res) => {
          handleSuccess(res, (data, code, msg) => {
            this.hasCheck = true
            this.checkSqlLoadBtn = false
            this.result = data
            this.$nextTick(() => {
              this.addBreakPoint(data, editor, msg)
              editor && editor.on('change', this.editerChangeHandle)
            })
          })
        }, (res) => {
          this.checkSqlLoadBtn = false
          handleError(res)
        })
      }
    },
    editerChangeHandle () {
      if (!this.firstLoad) {
        this.hasCheck = false
      }
      this.firstLoad = false
    },
    inputSql () {
      this.errorMsg = ''
      this.successMsg = ''
      this.ignoreErrorSql = false
      this.addSQLFormVisible = true
      this.hasCheck = false
      this.$nextTick(() => {
        var editor = this.$refs.sqlbox && this.$refs.sqlbox.$refs.kapEditor.editor
        if (editor) {
          editor && editor.removeListener('change', this.editerChangeHandle)
          if (this.actionMode === 'view') {
            editor.setReadOnly(true)
          } else {
            editor.setReadOnly(false)
          }
          editor.setOption('wrap', 'free')
          this.getAutoModelSql({
            modelName: this.modelInfo.modelName
          }).then((res) => {
            handleSuccess(res, (data) => {
              var result = data
              var sqls = result.sqls
              var errorInfo = result.results
              if (sqls.length) {
                this.hasCheck = true
              }
              let smartSql = this.modelInfo.smart_model_sqls.length > 0 ? this.modelInfo.smart_model_sqls.join(';\r\n') + ';' : ''
              let verifySql = sqls.length > 0 ? sqls.join(';\r\n') + ';' : ''
              this.sqlString = sqls.length > 0 ? verifySql : smartSql
              this.oldSqlString = sqls.length > 0 ? verifySql : smartSql
              this.firstLoad = true
              this.addBreakPoint(errorInfo, editor)
              editor && editor.on('change', this.editerChangeHandle)
              this.result = errorInfo
            })
          })
        }
      })
    },
    cancelCheckSql () {
      this.checkSqlLoadBtn = false
    },
    addBreakPoint (data, editor, msg) {
      this.errorMsg = ''
      this.successMsg = ''
      if (!editor) {
        return
      }
      this.bindBreakClickEvent(editor)
      if (data && data.length) {
        this.hasValidFailSql = false
        this.hasValidSuccessSql = false
        data.forEach((r, index) => {
          if (r.capable === false) {
            this.hasValidFailSql = true
            editor.session.setBreakpoint(index)
          } else {
            this.hasValidSuccessSql = true
            editor.session.clearBreakpoint(index)
          }
        })
        if (this.hasValidFailSql) {
          this.errorMsg = this.$t('validFail')
        } else {
          this.successMsg = this.$t('validSuccess')
        }
      } else {
        this.errorMsg = msg
      }
    },
    bindBreakClickEvent (editor) {
      if (!editor) {
        return
      }
      editor.on('guttermousedown', (e) => {
        var row = +e.domEvent.target.innerHTML
        // var row = e.getDocumentPosition().row
        var pointDoms = this.$el.querySelectorAll('.ace_gutter-cell')
        pointDoms.forEach((dom) => {
          if (+dom.innerHTML === row) {
            dom.className += ' active'
          } else {
            dom.className = dom.className.replace(/\s+active/g, '')
          }
        })
        var data = this.result
        row = row - 1
        if (data && data.length) {
          if (data[row].capable === false) {
            if (data[row].sqladvices) {
              this.errorMsg = ''
              data[row].sqladvices.forEach((err) => {
                this.errorMsg += err.incapableReason + ' '
              })
              this.currentSqlErrorMsg = data[row].sqladvices
            }
          } else {
            this.errorMsg = ''
            this.successMsg = ''
          }
        }
        e.stop()
      })
    },
    renderEditerRender (editor) {
      // var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor || ''
      if (!(editor && editor.session)) {
        return
      }
      editor.session.gutterRenderer = {
        getWidth: (session, lastLineNumber, config) => {
          return lastLineNumber.toString().length * 12
        },
        getText: (session, row) => {
          return row + 1
        }
      }
    },
    handleSelect (item) {
      this.reuseCC.isreused = true
      setTimeout(() => {
        this.$refs['computedColumnForm'].validateField('name')
      }, 0)
    },
    cleanMsg () {
      this.$refs['computedColumnForm'].validateField('name')
      this.checkExpressResult.msg = ''
      this.checkExpressResult.isValid = false
    },
    checkExpression () {
      if (!this.computedColumn.returnType) {
        this.$message(this.$t('plsCheckReturnType'))
        return
      }
      var guid = this.computedColumn.guid
      var databaseInfo = this.modelParserTool.getTableInfoByGuid(guid)
      this.$message(this.$t('longTimeTip'))
      var editor = this.$refs.expressionBox.$refs.kapEditor.editor
      editor.setReadOnly(true)
      this.checkExpressionBtnLoad = true
      var checkData = JSON.parse(this.DragDataToServerData(false, true))
      var needCheckComputedObj = {}
      if (indexOfObjWithSomeKey(checkData.computed_columns, 'columnName', this.computedColumn.name) === -1) {
        needCheckComputedObj = {
          tableIdentity: databaseInfo.database + '.' + databaseInfo.name,
          tableAlias: databaseInfo.alias,
          columnName: this.computedColumn.name,
          expression: this.computedColumn.expression,
          comment: this.computedColumn.comment,
          datatype: this.computedColumn.returnType,
          disabled: true
        }
        checkData.computed_columns.push(needCheckComputedObj)
      }
      this.checkComputedExpression({
        modelDescData: JSON.stringify(checkData),
        project: this.project,
        ccInCheck: `${databaseInfo.alias}.${this.computedColumn.name}`
      }).then((res) => {
        this.checkExpressionBtnLoad = false
        editor.setReadOnly(false)
        this.checkExpressResult.isValid = true
        handleSuccess(res, (data, code, msg) => {
          if (code === '001') {
            switch (data.causeType) {
              case 'WRONG_POSITION_DUE_TO_EXPR':
                if (data.advise) {
                  this.checkExpressResult.msg = this.$t('kylinLang.model.ccWrongPosition') + data.advise + '".'
                } else {
                  this.checkExpressResult.msg = msg
                }
                break
              case 'WRONG_POSITION_DUE_TO_NAME':
                if (data.advise) {
                  this.checkExpressResult.msg = this.$t('kylinLang.model.ccWrongPosition') + data.advise + '".'
                } else {
                  this.checkExpressResult.msg = msg
                }
                break
              case 'SAME_NAME_DIFF_EXPR':
                if (data.advise) {
                  this.reuseCC.name = ''
                  this.reuseCC.expression = data.advise
                  this.checkExpressResult.msg = this.$t('kylinLang.model.reuseCC1') + data.conflictingModel + this.$t('kylinLang.model.reuseCC2')
                  this.showReuseCC = true
                } else {
                  this.checkExpressResult.msg = msg
                }
                break
              case 'SAME_EXPR_DIFF_NAME':
                if (data.advise) {
                  this.reuseCC.name = data.advise
                  this.reuseCC.expression = ''
                  this.checkExpressResult.msg = this.$t('kylinLang.model.reuseCC1') + data.conflictingModel + this.$t('kylinLang.model.reuseCC2')
                  this.showReuseCC = true
                } else {
                  this.checkExpressResult.msg = msg
                }
                break
              case 'SELF_CONFLICT':
                this.checkExpressResult.msg = msg
                break
              case 'LOOKUP_CC_NOT_REFERENCING_ITSELF':
                this.checkExpressResult.msg = msg
                break
              default:
                this.checkExpressResult.msg = msg
            }
            this.checkExpressResult.isValid = false
          } else {
            this.checkExpressResult.msg = this.$t('checkSuccess')
          }
        })
      }, (res) => {
        this.checkExpressionBtnLoad = false
        editor.setReadOnly(false)
        this.checkExpressResult.isValid = false
        handleError(res, (a, b, c, msg) => {
          this.checkExpressResult.msg = msg
        })
      })
    },
    cancelCheckExpression () {
      var editor = this.$refs.expressionBox.$refs.kapEditor.editor
      editor.setReadOnly(false)
      this.checkExpressionBtnLoad = false
    },
    toggleFullScreen () {
      this.isFullScreen = !this.isFullScreen
      $('#fullBox').toggleClass('fullLayoutForModelEdit')
      this.resizeWindow(this.briefMenu, this.isFullScreen)
    },
    addComputedForm () {
      this.$refs['computedColumnForm'].resetFields()
      this.openAddComputedColumnForm = true
      this.showReuseCC = false
      this.computedColumn.name = ''
      this.computedColumn.expression = ''
      this.computedColumn.returnType = ''
      this.checkExpressResult = {isValid: '', msg: ''}
      this.$nextTick(() => {
        var editor = this.$refs.expressionBox.$refs.kapEditor.editor
        editor.setValue('')
      })
    },
    reuseClick () {
      if (this.reuseCC.name !== '') {
        this.computedColumn.name = this.reuseCC.name
      }
      if (this.reuseCC.expression !== '') {
        this.computedColumn.expression = this.reuseCC.expression
      }
      this.showReuseCC = false
      this.checkExpressResult.msg = ''
    },
    // 列排序
    sortColumns: function (tableInfo) {
      var key = 'name'
      var squence = false
      var sortType = getNextValInArray(this.columnSort, tableInfo.sort)
      tableInfo.sort = sortType
      if (sortType === 'order') {
        key = 'name'
        squence = true
      } else if (sortType === 'reversed') {
        key = 'name'
        squence = false
      } else {
        key = 'id'
        squence = true
      }
      tableInfo.columns = objectArraySort(tableInfo.columns, squence, key)
    },
    // 保存正式
    saveCurrentModel () {
      var rootFact = this.modelParserTool.getRootFact()
      if (!rootFact.length) {
        this.warnAlert(this.$t('hasNoFact'))
        return
      }
      var duplicateLinkTables = this.modelParserTool.getDuplicateColumnConnection()
      if (duplicateLinkTables) {
        var tableInfo = this.modelParserTool.getTableInfoByGuid(duplicateLinkTables[0])
        var tableName = tableInfo.database + '.' + tableInfo.name
        kapConfirm(this.$t('duplicateLinkTip', {fktable: tableName, pktable: duplicateLinkTables[1]}), {
          showCancelButton: false,
          type: 'warning'
        })
        return
      }
      var dimension = this.getDimensions()
      if (dimension.length === 0) {
        this.$message({
          duration: 0,  // 不自动关掉提示
          showClose: true,    // 给提示框增加一个关闭按钮
          type: 'warning',
          message: this.$t('needOneDimension')
        })
        return
      }
      this.hasStreamingTable = this.modelParserTool.getStreamingTable().length > 0
      this.addModelDialogDisable = true
    },
    saveAndCheckModel () {
      this.$message.closeAll()
      let modelDescJSON = this.DragDataToServerData(true)
      if (!modelDescJSON.partition_desc.partition_date_column && modelDescJSON.multilevel_partition_cols.length > 0) {
        this.$message({
          duration: 0,  // 不自动关掉提示
          showClose: true,    // 给提示框增加一个关闭按钮
          type: 'warning',
          message: this.$t('kylinLang.model.secondaryPartitionWarring')
        })
        return
      }
      if (this.hasStreamingTable && (!modelDescJSON.partition_desc.partition_date_column || !modelDescJSON.partition_desc.partition_date_format)) {
        this.$message({
          type: 'warning',
          message: this.$t('kylinLang.model.partitionDateTipNeedTip')
        })
        return
      }
      if (this.draftBtnLoading) {
        this.$message({
          duration: 0,  // 不自动关掉提示
          showClose: true,    // 给提示框增加一个关闭按钮
          type: 'warning',
          message: this.$t('kylinLang.common.saveDraft')
        })
        return
      }
      this.saveBtnLoading = true
      this.saveModel({
        project: this.project,
        modelDescData: filterNullValInObj(this.DragDataToServerData())
      }).then(() => {
        this.saveBtnLoading = false
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.saveSuccess')
        })
        if (this.checkModel.openModelCheck && !this.hasStreamingTable && (this.checkModel.factTable || this.checkModel.lookupTable || this.checkModel.modelStatus)) {
          if (this.checkModel.factTable) {
            this.checkModel.checkList = this.checkModel.checkList + 1
          }
          if (this.checkModel.lookupTable) {
            this.checkModel.checkList = this.checkModel.checkList + 2
          }
          if (this.checkModel.modelStatus) {
            this.checkModel.checkList = this.checkModel.checkList + 4
          }
          this.statsModel({
            project: this.project,
            modelname: this.modelInfo.modelName,
            data: {
              ratio: (this.modelStaticsRange / 100).toFixed(2),
              checkList: this.checkModel.checkList,
              forceUpdate: true
            }
          })
        }
        this.$emit('removetabs', 'model' + this.extraoption.modelName, 'Overview')
        this.$emit('reload', 'modelList')
      }, (res) => {
        this.saveBtnLoading = false
        handleError(res)
      })
    },
    hasSomePermissionOfProject () {
      return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
    },
    openModelSubMenu: function (currentMeunStatus, database, tablename) {
      // this.$refs['modelsubmenu'].menuStatus = this.$refs['modelsubmenu'].menuStatus === 'hide' ? 'show' : 'hide'
      this.$refs['modelsubmenu'].$emit('menu-toggle', 'hide')
      this.submenuInfo.menu1 = 'second'
      this.$set(this.currentSelectTable, 'database', database || '')
      this.$set(this.currentSelectTable, 'tablename', tablename || '')
    },
    addComputedColumn: function (guid, tableKind) {
      var connectCount = this.modelParserTool.getConnectsByTableId(guid)
      if (tableKind !== 'ROOTFACT' && connectCount === 0) {
        this.$message({message: this.$t('addConnectTip')})
        return
      }
      this.isEditComputedColumn = false
      this.computedColumn = {
        guid: '',
        name: '',
        expression: '',
        returnType: ''
      }
      this.computedColumn.guid = guid
      this.currentTableComputedColumns = []
      this.computedColumnFormVisible = true
      this.openAddComputedColumnForm = false
      this.refreshComputed()
      this.getCCList()
      this.$nextTick(() => {
        var editor = this.$refs.expressionBox.$refs.kapEditor.editor
        editor && editor.on('change', this.ccEditerChange)
        editor && editor.on('blur', this.ccEditerBlur)
        editor.setValue('')
        var autoCompeleteData = []
        var setCompleteData = function (data, tableName) {
          editor.completers.splice(0, editor.completers.length - 3)
          editor.completers.unshift({
            uuid: tableName,
            identifierRegexps: [/[.a-zA-Z_0-9]/],
            getCompletions: function (editor, session, pos, prefix, callback) {
              if (prefix.length === 0) {
                return callback(null, data)
              } else {
                return callback(null, data)
              }
            }
          })
        }
        editor.setOptions({
          wrap: 'free',
          enableBasicAutocompletion: true,
          enableSnippets: true,
          enableLiveAutocompletion: true
        })
        editor.commands.on('afterExec', function (e, t) {
          if (e.command.name === 'insertstring' && e.args === ' ') {
            var all = e.editor.completers
            // e.editor.completers = completers;
            e.editor.execCommand('startAutocomplete')
            e.editor.completers = all
          }
        })
        var tableList = this.tableList
        tableList.forEach((tableInfo) => {
          autoCompeleteData.push({meta: 'table', caption: tableInfo.name, value: tableInfo.name, scope: 1})
          if (tableInfo && tableInfo.columns) {
            tableInfo.columns.forEach((co) => {
              autoCompeleteData.push({meta: co.datatype, caption: tableInfo.alias + '.' + co.name, value: tableInfo.alias + '.' + co.name, scope: 2})
              // autoCompeleteData.push({meta: co.datatype, caption: co.name, value: co.name, scope: 2})
            })
          }
        })
        setCompleteData(autoCompeleteData)
      })
    },
    checkCCName (rule, value, callback) {
      var guid = this.computedColumn.guid
      if (!NamedRegex.test(value.toUpperCase())) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else if (!this.isEditComputedColumn && (this.checkSameColumnName(guid, value.toUpperCase()) || this.checkAllComputedName(value.toUpperCase()))) {
        callback(new Error(this.$t('kylinLang.model.sameCCName1') + value.toUpperCase() + this.$t('kylinLang.model.sameCCName2')))
      } else if (this.reuseCC.isreused) {
        this.reuseCC.isreused = false
        var databaseInfo = this.modelParserTool.getTableInfoByGuid(guid)
        var checkData = JSON.parse(this.DragDataToServerData(false, true))
        var needCheckComputedObj = {}
        needCheckComputedObj = {
          tableIdentity: databaseInfo.database + '.' + databaseInfo.name,
          tableAlias: databaseInfo.alias,
          columnName: value.toUpperCase(),
          expression: null,
          comment: '',
          datatype: '',
          disabled: true
        }
        checkData.computed_columns.push(needCheckComputedObj)
        this.checkComputedExpression({
          modelDescData: JSON.stringify(checkData),
          project: this.project,
          isSeekingExprAdvice: true,
          ccInCheck: `${databaseInfo.alias}.${value.toUpperCase()}`
        }).then((res) => {
          handleSuccess(res, (data, code, msg) => {
            if (code === '001') {
              switch (data.causeType) {
                case 'WRONG_POSITION_DUE_TO_NAME':
                  if (data.advise) {
                    callback(new Error(this.$t('kylinLang.model.ccWrongPosition') + data.advise + '".'))
                  } else {
                    callback(new Error(msg))
                  }
                  break
                case 'SAME_NAME_DIFF_EXPR':
                  if (data.advise) {
                    this.computedColumn.expression = data.advise
                    callback()
                  } else {
                    callback(new Error(msg))
                  }
                  break
                default:
                  callback(new Error(msg))
                  break
              }
            } else {
              callback()
            }
          })
        }, (res) => {
          handleError(res, (a, b, c, msg) => {
            callback(new Error(msg))
          })
        })
      } else {
        callback()
      }
    },
    checkCCExpression (rule, value, callback) {
      if (!this.isEditComputedColumn) {
        for (var i = 0; i < this.modelInfo.computed_columns.length; i++) {
          if (this.modelInfo.computed_columns[i].expression === value) {
            callback(new Error(this.$t('kylinLang.model.sameCCExpression1') + this.modelInfo.computed_columns[i].columnName + this.$t('kylinLang.model.sameCCExpression2')))
            return
          }
        }
        callback()
      } else {
        callback()
      }
    },
    ccEditerChange () {
      this.cleanMsg()
    },
    ccEditerBlur (val) {
      this.$refs['computedColumnForm'].validateField('expression')
    },
    editComputedColumn (row) {
      this.checkExpressResult = {
        isvalid: '',
        msg: ''
      }
      this.isEditComputedColumn = true
      this.openAddComputedColumnForm = true
      var tableInfo = this.modelParserTool.getTableInfoByAlias(row.tableAlias)
      if (tableInfo) {
        this.computedColumn = {
          guid: tableInfo.guid,
          name: row.columnName,
          expression: row.expression,
          returnType: row.datatype
        }
      }
    },
    delComputedColumn (column, ignoreLinkChcek) {
      var tableInfo = this.modelParserTool.getTableInfoByAlias(column.tableAlias)
      var hasUsedInconn = this.modelParserTool.isColumnUsedInConnection(tableInfo.guid, column.columnName)
      var _action = () => {
        this.modelParserTool.delComputedColumn(column.tableAlias, column.columnName)
        this.refreshComputed()
        this.getPartitionDateColumns()
      }
      if (hasUsedInconn && !ignoreLinkChcek) {
        kapConfirm(this.$t('delCCTips')).then(() => {
          _action()
          this.delConnectByTableColumn(tableInfo.guid, column.columnName) // 删除对应的连接
          this.refreshConnectCountText(hasUsedInconn[0], hasUsedInconn[1])
        })
      } else {
        _action()
      }
    },
    pushComputedColumnToMeta () {
      this.addComputedColumnToDatabase((columnName) => {
        this.$message({message: this.$t('addComputedColumnSuccess'), type: 'success'})
        // this.isEditComputedColumn = false
        this.cancelComputedEditForm()
        this.refreshComputed()
      })
    },
    saveComputedColumn: function (guid) {
      this.$refs.computedColumnForm.validate((valid) => {
        if (valid) {
          if (this.checkExpressionBtnLoad) {
            kapConfirm(this.$t('checkingTip'), {
              confirmButtonText: this.$t('continueSave'),
              cancelButtonText: this.$t('continueCheck')
            }).then(() => {
              this.isEditComputedColumn = false
              this.pushComputedColumnToMeta()
            })
          } else {
            this.isEditComputedColumn = false
            this.pushComputedColumnToMeta()
          }
        }
      })
    },
    cancelComputedEditForm: function (argument) {
      var editor = this.$refs.expressionBox.$refs.kapEditor.editor
      editor.setValue('')
      this.isEditComputedColumn = false
      setTimeout(() => {
        this.openAddComputedColumnForm = false
        this.$refs.computedColumnForm.resetFields()
      }, 0)
    },
    getPartitionDateColumns: function () {
      var dateColumns = {}
      var timeColumns = {}
      var tableList = this.modelParserTool.getRootFact().concat(this.modelParserTool.getFactTables())
      var tableListLen = tableList.length
      for (var i = 0; i < tableListLen; i++) {
        if (tableList[i].kind.indexOf('FACT') === -1) {
          continue
        }
        for (var k = 0; k < tableList[i].columns.length; k++) {
          var datatype = tableList[i].columns[k].datatype.replace(/\(.*?\)/, '')
          if (DatePartitionRule.indexOf(datatype) >= 0) {
            var needFormat = true
            if (IntegerType.indexOf(datatype) >= 0) {
              needFormat = false
            }
            var tabeFullName = tableList[i].alias
            dateColumns[tabeFullName] = dateColumns[tabeFullName] || []
            dateColumns[tabeFullName].push({name: tableList[i].columns[k].name, isFormat: needFormat, database: tableList[i].database, table: tableList[i].name, column: tableList[i].columns[k]})
          }
          if (TimePartitionRule.indexOf(datatype) >= 0 && tableList[i].columns[k].btype === 'D') {
            timeColumns[tabeFullName] = timeColumns[tabeFullName] || []
            timeColumns[tabeFullName].push({name: tableList[i].columns[k].name, isFormat: true, database: tableList[i].database, table: tableList[i].name, column: tableList[i].columns[k]})
          }
        }
      }
      this.dateColumns = {}
      this.timeColumns = {}
      this.dateColumns = Object.assign({}, this.dateColumns, dateColumns)
      this.timeColumns = Object.assign({}, this.timeColumns, timeColumns)
    },
    addComputedColumnToDatabase: function (callback, isInit) {
      this.computedColumn.name = this.computedColumn.name.toUpperCase()
      var guid = this.computedColumn.guid
      var databaseInfo = this.modelParserTool.getTableInfoByGuid(guid)
      // var sameTables = this.getSameOriginTables(databaseInfo.database, databaseInfo.name)
      if (!this.checkSameColumnName(guid, this.computedColumn.name)) {
        var columnObj = {
          name: this.computedColumn.name,
          datatype: this.computedColumn.returnType,
          btype: this.computedColumn.columnType || 'D',
          expression: this.computedColumn.expression,
          isComputed: true
        }
        var computedObj = {
          tableIdentity: databaseInfo.database + '.' + databaseInfo.name,
          tableAlias: databaseInfo.alias,
          columnName: this.computedColumn.name,
          expression: this.computedColumn.expression,
          comment: this.computedColumn.comment,
          datatype: this.computedColumn.returnType,
          disabled: true
        }
        databaseInfo.columns.unshift(columnObj)
        if (!isInit) {
          this.modelInfo.computed_columns.push(computedObj)
        }
        if (typeof callback === 'function') {
          callback(computedObj.columnName)
        }
      } else {
        if (this.checkSameComputedName(guid, this.computedColumn.name)) {
          this.modelInfo.computed_columns.forEach((co) => {
            if (co.tableAlias === databaseInfo.alias && co.columnName === this.computedColumn.name) {
              co.expression = this.computedColumn.expression
              co.datatype = this.computedColumn.returnType
            }
          })
          this.editTableComputedColumnInfo(guid, this.computedColumn.name, 'expression', this.computedColumn.expression)
          this.editTableComputedColumnInfo(guid, this.computedColumn.name, 'datatype', this.computedColumn.returnType)
          this.openAddComputedColumnForm = false
        } else {
          if (!isInit) {
            this.warnAlert(this.$t('sameNameComputedColumn'))
          }
        }
      }
      this.getPartitionDateColumns()
    },
    checkSameColumnName: function (guid, column) {
      var columns = this.modelParserTool.getTableInfoByGuid(guid).columns
      return indexOfObjWithSomeKey(columns, 'name', column) !== -1
    },
    checkSameComputedName (guid, column) {
      var columns = this.modelParserTool.getTableInfoByGuid(guid).columns
      for (var s = 0; s < columns.length; s++) {
        if (columns[s].isComputed && columns[s].name === column) {
          return true
        }
      }
    },
    checkSameNormalName (guid, column) {
      var columns = this.modelParserTool.getTableInfoByGuid(guid).columns
      for (var s = 0; s < columns.length; s++) {
        if (!columns[s].isComputed && columns[s].name === column) {
          return true
        }
      }
    },
    checkAllComputedName (column) {
      for (var i = 0; i < this.modelInfo.computed_columns.length; i++) {
        if (this.modelInfo.computed_columns[i].columnName === column) {
          return true
        }
      }
      return false
    },
    // dimension and measure and disable
    changeColumnBType: function (id, columnName, columnBType, isComputed) {
      if (this.actionMode === 'view') {
        return
      }
      if (columnBType === 'D' && this.modelParserTool.isColumnUsedInConnection(id, columnName)) {
        this.warnAlert(this.$t('changeUsedForConnectColumnTypeWarn'))
        return
      }
      var willSetType = getNextValInArray(this.columnBType, columnBType)
      if (willSetType === '－' && isComputed) {
        willSetType = this.columnBType[0]
      }
      this.modelParserTool.editTableColumnInfo(id, 'name', columnName, 'btype', willSetType)
      // var fullName = tableInfo.database + '.' + tableInfo.name
      this.getPartitionDateColumns()
    },
    getDisableComputedColumn () {
      var result = []
      var len = this.modelInfo.computed_columns && this.modelInfo.computed_columns.length || 0
      for (var i = 0; i < len; i++) {
        var calcColumn = this.modelInfo.computed_columns[i]
        var obj = {}
        if (calcColumn.disabled !== false) {
          obj = Object.assign({}, calcColumn)
          result.push(obj)
        }
      }
      return result
    },
    /*
    *  Filter Func
    *  ==========================================================================
    */
    filterColumnByInput: function (filter, id) {
      var instance = Scrollbar.get($('#' + id).find('.columns_box')[0])
      var suggestObj = this.getColumnDataByLikeFilter(id, filter)
      if (suggestObj.length) {
        for (var k = 0; k < suggestObj.length; k++) {
          for (var j = k + 1; j < suggestObj.length; j++) {
            if (suggestObj[k].startIndex > suggestObj[j].startIndex) {
              var temp = suggestObj[j]
              suggestObj[j] = suggestObj[k]
              suggestObj[k] = temp
            }
          }
        }
        this.autoScroll(instance, suggestObj[0].index * 30, suggestObj[0].className, id)
        this.selectFilterColumn(id, suggestObj[0].className, suggestObj[0].columnType)
      } else {
        this.selectFilterColumn(id, '', '')
      }
    },
    filterColumnByInput1: function (filter, id) {
      filter = filter.toUpperCase()
      this.editTableColumnsUniqueInfo(id, 'name', filter, 'isShow', true, false, true)
    },
    cancelFilterColumn: function (id) {
      this.$set(this.selectColumn, id, '')
    },
    selectFilterColumn: function (id, columnName, columnType, pointType) {
      this.editTableColumnsUniqueInfo(id, 'name', columnName, 'isActive', true, false)
      if (columnName) {
        this.$set(this.selectColumn, id, {columnName: columnName, columnType: columnType})
      } else {
        this.cancelFilterColumn(id)
      }
      var tableInfo = this.modelParserTool.getTableInfoByGuid(id)
      this.$set(this.currentSelectTable, 'database', tableInfo.database || '')
      this.$set(this.currentSelectTable, 'tablename', tableInfo.name || '')
      this.$set(this.currentSelectTable, 'columnname', columnName)

      // this.editTableColumnsUniqueInfo(id, 'name', columnName, 'isSelected', true, false)
      if (tableInfo.openMutilSelected) {
        this.modelParserTool.editTableColumnInfo(id, 'name', columnName, 'isSelected')
        if (!tableInfo.mutilSelectedList) {
          this.$set(tableInfo, 'mutilSelectedList', [])
        }
        var columnIndex = tableInfo.mutilSelectedList.indexOf(columnName)
        if (columnIndex >= 0) {
          tableInfo.mutilSelectedList.splice(columnIndex, 1)
        } else {
          tableInfo.mutilSelectedList.push(columnName)
        }
        if (tableInfo.mutilSelectedList.length === tableInfo.columns.length) {
          tableInfo.allChecked = true
        } else {
          tableInfo.allChecked = false
        }
      }
    },
    getColumnDataByLikeFilter: function (guid, filter) {
      var suggest = []
      if (filter === '') {
        return suggest
      }
      this.tableList.forEach(function (table) {
        if (table.guid === guid) {
          for (let i = 0; i < table.columns.length; i++) {
            var col = table.columns[i]
            if (col.name.toUpperCase().indexOf(filter.toUpperCase()) >= 0) {
              suggest.push({
                className: col.name,
                index: i,
                columnType: col.datatype,
                startIndex: col.name.toUpperCase().indexOf(filter.toUpperCase())
              })
            }
          }
        }
      })
      return suggest
    },
    autoScroll (instance, topSize, aim, id) {
      instance.scrollTo(100, topSize, 300, function (scrollbar) {})
    },
    linkFilterColumnAnimate (sourceId, targetId, callback) {
      callback()
    },
    removeTable: function (guid) {
      var count = this.modelParserTool.getConnectsByTableId(guid)
      if (count > 0) {
        this.warnAlert(this.$t('delTableTip'))
        return
      }
      this.modelParserTool.delTable(guid)
    },
    draggleTable: function (idList) {
      var _this = this
      this.plumbInstance.draggable(idList, {
        handle: '.drag_bar,.drag_bar span',
        drop: function (e) {
        },
        stop: function (e) {
          _this.modelParserTool.editTableInfoByGuid(e.el.id, 'pos', {x: e.pos[0], y: e.pos[1]})
        }
      })
    },
    drag: function (target, dragName) {
      if (this.actionMode === 'view') {
        this.$message(this.$t('kylinLang.model.viewModeLimit'))
        return
      }
      if (target) {
        this.dragType = 'createTables'
        this.currentDragDom = target
        this.currentDragData = {
          database: dragName.split('$')[0],
          table: dragName.split('$')[1]
        }
      }
    },
    dragColumns (event) {
      if (this.actionMode === 'view') {
        this.$message(this.$t('kylinLang.model.viewModeLimit'))
        return
      }
      // event.dataTransfer && event.dataTransfer.setData('Text', '')
      this.currentDragDom = event.srcElement ? event.srcElement : event.target
      event.dataTransfer.effectAllowed = 'move'
      if (!isIE()) {
        event.dataTransfer && event.dataTransfer.setData && event.dataTransfer.setData('text', this.currentDragDom.innerHTML)
      }
      event.dataTransfer.setDragImage && event.dataTransfer.setDragImage(this.currentDragDom, 0, 0)
      this.dragType = 'createLinks'
      // var dt = event.originalEvent.dataTransfer
      // dt.effectAllowed = 'copyMove'
      var btype = $(this.currentDragDom).attr('data-btype')
      // var isComputed = $(this.currentDragDom).attr('data-isComputed')
      if (btype !== 'D') {
        this.$message(this.$t('kylinLang.model.dimensionLinkLimit'))
        return false
      }
      // if (isComputed) {
      //   this.$message(this.$t('kylinLang.model.computedLinkLimit'))
      //   return false
      // }
      this.currentDragData = {
        table: $(this.currentDragDom).attr('data-guid'),
        columnName: $(this.currentDragDom).attr('data-column'),
        btype: btype
      }
      return true
    },
    dragColumnsEnd (event) {
      event.dataTransfer.clearData('text')
      // eleDrag = null;/
      return false
    },
    drop: function (event) {
      event.preventDefault && event.preventDefault()
      event.stopPropagation && event.stopPropagation()
      if (this.actionMode === 'view') {
        return
      }
      if (this.dragType !== 'createTables') {
        return
      }
      var _this = this
      var target = event.srcElement ? event.srcElement : event.target
      if (target.className.indexOf('model_edit') >= 0) {
        var pos = {
          x: Math.abs(_this.docker.x) + event.pageX - 215,
          y: Math.abs(_this.docker.y) + event.pageY - 170
        }
        this.modelParserTool.addTableToRenderList(this.currentDragData.database, this.currentDragData.table, {pos: pos, kind: 'LOOKUP'})
        // this.suggestColumnDtype(newTableData)
      }
      return false
    },
    suggestColumnDtype (newTableData, needSuggestColumns) {
      if (!newTableData) {
        return
      }
      this.suggestDM({'table': newTableData.database + '.' + newTableData.name, project: this.project}).then((response) => {
        handleSuccess(response, (data) => {
          for (var i in data) {
            var setColumnBType
            if (!needSuggestColumns || needSuggestColumns && needSuggestColumns.indexOf(i) >= 0) {
              if (data[i].indexOf('METRIC') === 0) {
                setColumnBType = 'M'
              } else if (data[i].indexOf('DIMENSION') === 0) {
                setColumnBType = 'D'
              }
              this.modelParserTool.editTableColumnInfo(newTableData.guid, 'name', i, 'btype', setColumnBType)
            }
          }
        })
      })
    },
    dragColumnsEnter (event) {
      var target = event.srcElement ? event.srcElement : event.target
      $(target).addClass('column_in')
    },
    dragColumnsLeave (event) {
      var target = event.srcElement ? event.srcElement : event.target
      $(target).removeClass('column_in')
    },
    dropColumn: function (event, col, table) {
      event.preventDefault && event.preventDefault()
      event.stopPropagation && event.stopPropagation()
      var target = event.srcElement ? event.srcElement : event.target
      $(target).removeClass('column_in')
      if (this.dragType !== 'createLinks') {
        return
      }
      var dataBox = $(target).parents('.table_box')
      var columnBox = $(target).hasClass('column_li') ? $(target) : $(target).parents('.column_li')
      var targetId = dataBox.attr('id')
      var columnName = columnBox.attr('data-column')
      var btype = columnBox.attr('data-btype')
      // var isComputed = columnBox.attr('data-isComputed')
      // var isComputedStart = $(this.currentDragDom).attr('data-isComputed')
      var btypeStart = $(this.currentDragDom).attr('data-btype')
      if (btype !== 'D' || btypeStart !== 'D') {
        this.$message(this.$t('kylinLang.model.dimensionLinkLimit'))
        return
      }
      // if (isComputed || isComputedStart) {
      //   this.$message(this.$t('kylinLang.model.computedLinkLimit'))
      //   return
      // }
      if (targetId) {
        this.currentDropData = {
          table: targetId,
          columnName: columnName
        }
        if (this.currentDragData.table === this.currentDropData.table) {
          return
        }
        if (this.checkIncorrectLink(this.currentDragData.table, this.currentDropData.table)) {
          return
        }
        this.prepareLinkData(this.currentDragData.table, this.currentDropData.table)
      }
      return false
    },
    prepareLinkData (p1, p2, justShow) {
      this.dialogVisible = true
      var sourceTableInfo = this.modelParserTool.getTableInfoByGuid(p1)
      var targetTableInfo = this.modelParserTool.getTableInfoByGuid(p2)
      this.currentLinkData.source.guid = sourceTableInfo.guid
      this.currentLinkData.source.database = sourceTableInfo.database
      this.currentLinkData.source.name = sourceTableInfo.name
      this.currentLinkData.source.alias = sourceTableInfo.alias
      this.currentLinkData.source.columns = sourceTableInfo.columns.filter((co) => {
        return co.btype === 'D'
      })
      this.currentLinkData.target.guid = targetTableInfo.guid
      this.currentLinkData.target.database = targetTableInfo.database
      this.currentLinkData.target.name = targetTableInfo.name
      this.currentLinkData.target.alias = targetTableInfo.alias
      this.currentLinkData.target.columns = targetTableInfo.columns.filter((co) => {
        return co.btype === 'D'
      })
      this.currentLinkData.joinType = this.modelParserTool.getConnectJoinType(p1, p2) || modelRenderConfig.joinKind.inner
      this.currentTableLinks = this.modelParserTool.getConnectsByTableIds(p1, p2)
      if (!justShow) {
        this.addJoinCondition(sourceTableInfo.guid, targetTableInfo.guid, this.currentDragData.columnName, this.currentDropData.columnName, this.currentLinkData.joinType, true)
      }
    },
    allowDrop: function (event) {
      event.preventDefault()
    },
    /*
    *  Connect Func
    *  ==========================================================================
    */
    delConnect: function (connect) {
      this.modelParserTool.delConnCondition(connect)
      this.refreshConnectCountText(connect[0], connect[1])
      var linkCount = this.modelParserTool.getConnectsByTableId(connect[1])
      if (linkCount === 0) {
        this.dialogVisible = false
      }
    },
    delConnectByTableColumn: function (guid, columnName) {
      this.modelParserTool.delConnConditionByTableColumn(guid, columnName, (delCondition) => {
        this.refreshConnectCountText(delCondition[0], delCondition[1])
      })
    },
    delBrokenConnect (p1, p2) {
      this.modelParserTool.delBrokenConnCondition(p1, p2)
      this.refreshConnectCountText(p1, p2)
    },
    refreshConnectCountText (p1, p2) {
      this.currentTableLinks = this.modelParserTool.getConnectsByTableIds(p1, p2)
      var showLinkCon = this.showLinkCons[p1 + '$' + p2]
      if (showLinkCon) {
        if (this.currentTableLinks.length) {
          this.modelParserTool.setOverLayLabel(p1, p2)
        } else {
          this.modelParserTool.delConnection(p1, p2)
        }
      } else {
        this.modelParserTool.renderLink(p1, p2)
      }
    },
    addJoinCondition: function (p1, p2, col1, col2, joinType, newCondition) {
      this.modelParserTool.addLinkToRenderList(p1, p2, col1, col2, joinType, newCondition)
    },
    saveLinks (p1, p2, delError) {
      var isBroken = this.modelParserTool.isHasBrokenConnCondition(p1, p2)
      if (isBroken) {
        this.warnAlert(this.$t('checkCompleteLink'))
        if (delError) {
          this.delBrokenConnect(p1, p2)
        }
        return
      }
      this.dialogVisible = false
      this.delBrokenConnect(p1, p2)
      this.currentTableLinks = this.modelParserTool.getConnectsByTableIds(p1, p2)
      this.modelParserTool.setOverLayLabel(p1, p2, this.currentLinkData.joinType)
    },
    // 检查不合理的链接
    checkIncorrectLink: function (p1, p2) {
      // 检查是否是rootfact指向fact和lookup
      var resultTag = false
      if (this.modelParserTool.isRootFact(p2)) {
        this.warnAlert(this.$t('cannotSetFTableToFKTable'))
        resultTag = true
      }
      // 检查是否有反向链接
      for (var i = 0; i < this.links.length; i++) {
        if (this.links[i][0] === p2 && this.links[i][1] === p1) {
          this.warnAlert(this.$t('tableHasOppositeLinks'))
          resultTag = true
          break
        }
      }
      if (this.modelParserTool.getFKConnectsCountByGuid(p1, p2) >= 1) {
        this.warnAlert(this.$t('tableHasOtherFKTable'))
        resultTag = true
      }
      return resultTag
    },
    // connection end
    editAlias: function (guid) {
      if (this.actionMode === 'view') {
        return
      }
      var rootFact = this.modelParserTool.getRootFact()
      if (rootFact.length && rootFact[0].guid === guid) {
        return
      }
      this.aliasEdit = true
      this.aliasEditTableId = guid
    },
    checkSameAlias: function (guid, newAlias) {
      var hasAlias = 0
      this.tableList.forEach(function (table) {
        if (table.guid !== guid) {
          if (table.alias.toUpperCase() === newAlias.toUpperCase()) {
            hasAlias++
          }
        }
      })
      return hasAlias
    },
    cancelEditAlias: function (guid) {
      this.aliasEdit = false
      this.aliasEditTableId = ''
      var tableInfo = this.modelParserTool.getTableInfoByGuid(guid)
      var uniqueName = this.createUniqueName(guid, tableInfo.alias)
      this.$set(tableInfo, 'alias', uniqueName)
    },
    createUniqueName (guid, alias) {
      if (alias && guid) {
        var sameCount = this.checkSameAlias(guid, alias)
        var finalAlias = alias.toUpperCase().replace(/[^a-zA-Z_0-9]/g, '')
        if (sameCount === 0) {
          return finalAlias
        } else {
          while (this.checkSameAlias(guid, finalAlias + '_' + sameCount)) {
            sameCount++
          }
          return finalAlias + '_' + sameCount
        }
      }
    },
    filterSameAliasTb: function (guid, alias) {
      let sameAliasTbArr = this.tableList.filter(function (table) {
        return (table.guid !== guid && table.alias.toUpperCase() === alias.toUpperCase())
      })
      if (sameAliasTbArr.length > 0) {
        return sameAliasTbArr[0].guid
      }
      return null
    },
    selectTableKind: function (command, licompon) {
      var kind = command
      var guid = licompon.$el.getAttribute('data')
      var tableInfo = this.modelParserTool.getTableInfoByGuid(guid)
      var rootFact = this.modelParserTool.getRootFact()
      if (kind === 'ROOTFACT') {
        if (rootFact.length && rootFact[0].guid === guid) {
          this.warnAlert(this.$t('hasRootFact'))
          return
        }
        for (var i in this.links) {
          if (this.links[i][1] === guid) {
            this.warnAlert(this.$t('cannotSetFact'))
            return
          }
        }
        var tableInfoOldAlias = tableInfo.alias
        this.modelParserTool.editTableInfoByGuid(guid, 'alias', tableInfo.name)
        if (tableInfoOldAlias.toUpperCase() !== tableInfo.name.toUpperCase()) {
          var otherHasSameNameTableGuid = this.filterSameAliasTb(guid, tableInfo.name)
          if (otherHasSameNameTableGuid) {
            var otherHasSameNameTableInfo = this.modelParserTool.getTableInfoByGuid(otherHasSameNameTableGuid)
            this.$set(otherHasSameNameTableInfo, 'alias', tableInfoOldAlias)
          }
        }
      }
      this.modelParserTool.editTableInfoByGuid(guid, 'kind', kind)
      this.currentSelectTable = {
        database: tableInfo.database,
        tablename: tableInfo.tableName,
        columnname: ''
      }
      this.getPartitionDateColumns()
    },
    editTableComputedColumnInfo: function (guid, column, key, value) {
      var tableInfo = this.modelParserTool.getTableInfoByGuid(guid)
      var tableList = this.modelParserTool.getSameOriginTables(tableInfo.database, tableInfo.name)
      tableList.forEach((table) => {
        var curGuid = table.guid
        this.modelParserTool.editTableColumnInfo(curGuid, 'name', column, key, value)
      })
    },
    // 设置所有列中只有一个列有一种属性，其他都为另一种属性
    editTableColumnsUniqueInfo: function (guid, filterColumnKey, filterColumnVal, columnKey, value, oppositeValue, fuzzy) {
      this.tableList.forEach((table) => {
        if (table.guid === guid) {
          for (let i = 0; i < table.columns.length; i++) {
            var col = table.columns[i]
            if (!filterColumnVal) {
              this.$set(col, columnKey, value)
              continue
            }
            if (col[filterColumnKey] === filterColumnVal || fuzzy && col[filterColumnKey].indexOf(filterColumnVal) >= 0) {
              this.$set(col, columnKey, value)
            } else {
              this.$set(col, columnKey, oppositeValue)
            }
          }
        }
      })
    },
    warnAlert: function (str, needClose) {
      var para = {
        showClose: needClose,
        message: str,
        type: 'warning'
      }
      if (needClose) {
        para.duration = 0
      }
      this.$message(para)
    },
    // trans Data
    DragDataToServerData: function (needJson, ignoreComputed) {
      var fainalComputed = this.getDisableComputedColumn()
      var pos = {}
      var kylinData = {
        uuid: this.modelInfo.uuid,
        capacity: 'MEDIUM',
        owner: this.modelInfo.owner,
        lookups: [],
        partition_desc: {},
        dimensions: [],
        metrics: [],
        multilevel_partition_cols: [],
        is_draft: this.modelInfo.is_draft,
        last_modified: this.modelInfo.last_modified,
        filter_condition: this.modelInfo.filterStr,
        name: this.modelInfo.modelName,
        smart_model: this.modelInfo.smart_model,
        smart_model_sqls: this.modelInfo.smart_model_sqls,
        description: this.modelInfo.modelDiscribe,
        computed_columns: fainalComputed
      }
      if (this.partitionSelect.date_table && this.partitionSelect.date_column) {
        kylinData.partition_desc.partition_date_column = this.partitionSelect.date_table + '.' + this.partitionSelect.date_column
      }
      if (this.partitionSelect.time_table && this.partitionSelect.time_column) {
        kylinData.partition_desc.partition_time_column = this.partitionSelect.time_table + '.' + this.partitionSelect.time_column
      }
      if (this.partitionSelect.mutilLevel_table && this.partitionSelect.mutilLevel_column) {
        kylinData.multilevel_partition_cols.push(this.partitionSelect.mutilLevel_table + '.' + this.partitionSelect.mutilLevel_column)
      }
      kylinData.partition_desc.partition_date_format = this.partitionSelect.partition_date_format
      kylinData.partition_desc.partition_time_format = this.partitionSelect.partition_time_format
      kylinData.partition_desc.partition_type = 'APPEND'
      kylinData.partition_desc.partition_date_start = null

      // root table
      var rootFactTable = this.modelParserTool.getRootFact()
      if (rootFactTable.length) {
        kylinData.fact_table = rootFactTable[0].database + '.' + rootFactTable[0].name
        pos[rootFactTable[0].name] = rootFactTable[0].pos
      }
      // lookups
      var linkTables = {}
      for (var i = 0; i < this.links.length; i++) {
        var currentLink = this.links[i]
        var p1 = currentLink[0]
        var p2 = currentLink[1]
        var col1 = currentLink[2]
        var col2 = currentLink[3]
        var joinType = currentLink[4]
        var p1TableInfo = this.modelParserTool.getTableInfoByGuid(p1)
        var p2TableInfo = this.modelParserTool.getTableInfoByGuid(p2)
        pos[p1TableInfo.alias] = p1TableInfo.pos
        if (!linkTables[p1 + '$' + p2]) {
          linkTables[p1 + '$' + p2] = {
            table: p2TableInfo.database + '.' + p2TableInfo.name,
            kind: p2TableInfo.kind,
            alias: p2TableInfo.alias,
            join: {
              type: joinType,
              primary_key: [p2TableInfo.alias + '.' + col2],
              foreign_key: [p1TableInfo.alias + '.' + col1]
            }
          }
        } else {
          linkTables[p1 + '$' + p2].join.primary_key.push(p2TableInfo.alias + '.' + col2)
          linkTables[p1 + '$' + p2].join.foreign_key.push(p1TableInfo.alias + '.' + col1)
        }
      }
      for (var s in linkTables) {
        kylinData.lookups.push(linkTables[s])
      }
      // dimensions
      kylinData.dimensions = this.getDimensions(ignoreComputed)
      kylinData.pos = pos
      kylinData.metrics = this.getMeasures(ignoreComputed)
      if (needJson) {
        return kylinData
      }
      return JSON.stringify(kylinData)
    },
    // loadEditData (modelData) {
    //   if (!modelData.fact_table) {
    //     return
    //   }
    //   Object.assign(this.modelInfo, {
    //     uuid: modelData.uuid,
    //     modelDiscribe: modelData.description,
    //     modelName: modelData.name,
    //     is_draft: modelData.is_draft,
    //     last_modified: modelData.last_modified,
    //     owner: modelData.owner,
    //     project: modelData.project,
    //     smart_model: modelData.smart_model,
    //     smart_model_sqls: modelData.smart_model_sqls
    //   })
    //   // 加载原来设置的partition
    //   var partitionDate = modelData.partition_desc.partition_date_column ? modelData.partition_desc.partition_date_column.split('.') : [null, null]
    //   var partitionTime = modelData.partition_desc.partition_time_column ? modelData.partition_desc.partition_time_column.split('.') : [null, null]
    //   var mutilLevel = modelData.multilevel_partition_cols.length > 0 ? modelData.multilevel_partition_cols[0].split('.') : [null, null]
    //   Object.assign(this.partitionSelect, {
    //     'date_table': partitionDate[0],
    //     'date_column': partitionDate[1],
    //     'time_table': partitionTime[0],
    //     'time_column': partitionTime[1],
    //     'mutilLevel_table': mutilLevel[0],
    //     'mutilLevel_column': mutilLevel[1],
    //     'partition_date_column': modelData.partition_desc.partition_date_column,
    //     'partition_time_column': modelData.partition_desc.partition_time_column,
    //     'partition_date_start': 0,
    //     'partition_date_format': modelData.partition_desc.partition_date_format,
    //     'partition_time_format': modelData.partition_desc.partition_time_format,
    //     'partition_type': 'APPEND'
    //   })
    //   // 加载原来设置的partition
    //   this.modelInfo.filterStr = modelData.filter_condition
    //   this.modelInfo.computed_columns = modelData.computed_columns || []
    //   var lookups = modelData.lookups
    //   var baseTables = {}
    //   for (var i = 0; i < lookups.length; i++) {
    //     baseTables[lookups[i].alias] = {
    //       database: lookups[i].table.split('.')[0],
    //       table: lookups[i].table.split('.')[1],
    //       kind: lookups[i].kind,
    //       guid: sampleGuid(),
    //       alias: lookups[i].alias
    //     }
    //   }
    //   baseTables[modelData.fact_table.split('.')[1]] = {
    //     database: modelData.fact_table.split('.')[0],
    //     table: modelData.fact_table.split('.')[1],
    //     kind: 'ROOTFACT',
    //     guid: sampleGuid(),
    //     alias: modelData.fact_table.split('.')[1]
    //   }
    //   this.currentSelectTable = {
    //     database: modelData.fact_table.split('.')[0],
    //     tablename: modelData.fact_table.split('.')[1],
    //     columnname: ''
    //   }
    //   for (var table in baseTables) {
    //     this.createTableData(this.extraoption.project, baseTables[table].database, baseTables[table].table, {
    //       pos: modelData.pos && modelData.pos[modelData.fact_table.split('.')[1]] || {x: 20000, y: 20000},
    //       kind: baseTables[table].kind,
    //       guid: baseTables[table].guid,
    //       alias: baseTables[table].alias || baseTables[table].name
    //     })
    //   }
    //   var baseLinks = []
    //   for (let i = 0; i < lookups.length; i++) {
    //     for (let j = 0; j < lookups[i].join.primary_key.length; j++) {
    //       let priFullColumn = lookups[i].join.primary_key[j]
    //       let foriFullColumn = lookups[i].join.foreign_key[j]
    //       let priAlias = priFullColumn.split('.')[0]
    //       let foriAlias = foriFullColumn.split('.')[0]
    //       let pcolumn = priFullColumn.split('.')[1]
    //       let fcolumn = foriFullColumn.split('.')[1]
    //       let bArr = [baseTables[foriAlias].guid, baseTables[priAlias].guid, fcolumn, pcolumn, lookups[i].join.type]
    //       baseLinks.push(bArr)
    //     }
    //   }
    //   this.links = baseLinks
    //   this.$nextTick(() => {
    //     Scrollbar.initAll()
    //     for (let i = 0; i < lookups.length; i++) {
    //       let priAlias = lookups[i].alias
    //       let foriFullColumn = lookups[i].join.foreign_key[0]
    //       var pGuid = baseTables[priAlias].guid
    //       var fGuid = baseTables[foriFullColumn.split('.')[0]].guid
    //       var hisConn = this.showLinkCons[pGuid + '$' + fGuid]
    //       if (!hisConn) {
    //         var joinType = lookups[i].join.type
    //         this.addShowLink(fGuid, pGuid, joinType)
    //       }
    //     }
    //     if (!modelData.pos) {
    //       this.autoLayerPosition()
    //     }
    //   })
    //   var computedColumnsLen = modelData.computed_columns && modelData.computed_columns.length || 0
    //   // computed column add  new method to init computed column
    //   for (let i = 0; i < computedColumnsLen; i++) {
    //     var alias = modelData.computed_columns[i].tableAlias
    //     var tableInfo = this.modelParserTool.getTableInfoByAlias(alias)
    //     if (tableInfo) {
    //       this.computedColumn = {
    //         guid: tableInfo.guid,
    //         name: modelData.computed_columns[i].columnName,
    //         expression: modelData.computed_columns[i].expression,
    //         returnType: modelData.computed_columns[i].datatype
    //         // columnType: modelData.computed_columns[i].datatype
    //       }
    //       this.addComputedColumnToDatabase(() => {}, true)
    //     }
    //   }
    //   var modelDimensionsLen = modelData.dimensions && modelData.dimensions.length || 0
    //   for (let i = 0; i < modelDimensionsLen; i++) {
    //     var currentD = modelData.dimensions[i]
    //     for (let j = 0; j < currentD.columns.length; j++) {
    //       this.modelParserTool.editTableColumnInfo(baseTables[currentD.table].guid, 'name', currentD.columns[j], 'btype', 'D')
    //     }
    //   }
    //   var modelMetricsLen = modelData.metrics && modelData.metrics.length || 0
    //   for (let i = 0; i < modelMetricsLen; i++) {
    //     var currentM = modelData.metrics[i]
    //     this.modelParserTool.editTableColumnInfo(baseTables[currentM.split('.')[0]].guid, 'name', currentM.split('.')[1], 'btype', 'M')
    //   }
    //   // partition time setting
    //   this.getPartitionDateColumns()
    //   this.firstRenderServerData = true
    //   this.modelDataLoadEnd = true
    //   this.$nextTick(() => {
    //     this.resizeWindow(this.briefMenu)
    //   })
    // },
    serverDataToDragData () {
      if (this.$el !== null) {
        var project = this.extraoption.project
        this.modelParserTool.test(this.$store.state.datasource.dataSource[project], project, this, (p1, p2) => {
          this.prepareLinkData(p1, p2, true)
        })
        this.modelParserTool.render()
        return
      }
      // 添加模式
      if (!this.extraoption.uuid) {
        this.$set(this.modelInfo, 'modelDiscribe', this.extraoption.modelDesc)
        this.$set(this.modelInfo, 'modelName', this.extraoption.modelName)
        this.modelDataLoadEnd = true
        this.$nextTick(() => {
          this.resizeWindow(this.briefMenu)
        })
        return
      }
      // 编辑模式
      // var actionModelName = this.extraoption.status ? this.extraoption.modelName + '_draft' : this.extraoption.modelName
      // var actionModelName = this.extraoption.modelName
      // var _renderData = (modelData) => {
      //   setTimeout(() => {
      //     this.loadEditData(modelData)
      //   }, 100)
      // }
      // this.getModelByModelName({modelName: actionModelName, project: this.extraoption.project}).then((response) => {
      //   handleSuccess(response, (data) => {
      //     this.modelData = data.model
      //     this.draftData = data.draft
      //     var modelData = this.extraoption.status ? data.draft : data.model
      //     if (this.modelData && this.draftData && this.extraoption.mode !== 'view') {
      //       kapConfirm(this.$t('kylinLang.common.checkDraft'), {
      //         confirmButtonText: this.$t('kylinLang.common.ok'),
      //         cancelButtonText: this.$t('kylinLang.common.cancel')
      //       }).then(() => {
      //         modelData = data.draft
      //         _renderData(modelData)
      //       }).catch(() => {
      //         modelData = data.model
      //         _renderData(modelData)
      //       })
      //     } else {
      //       _renderData(modelData || {})
      //     }
      //   })
      // }, (res) => {
      //   handleError(res)
      // })
    },
    autoLayerPosition: function (rePosition) {
      this.modelParserTool.autoLayerPosition()
    },
    getCCList () {
      this.getComputedColumns(this.project).then((res) => {
        handleSuccess(res, (data) => {
          this.ccArray = data
        })
      }, (res) => {
        handleError(res)
      })
    },
    refreshComputed () {
      var guid = this.computedColumn.guid
      if (!guid) {
        return
      }
      this.currentTableComputedColumns.splice(0, this.currentTableComputedColumns.length)
      var tableInfo = this.modelParserTool.getTableInfoByGuid(guid)
      this.modelInfo.computed_columns.filter((computedColumn) => {
        if (computedColumn.tableAlias === tableInfo.alias && computedColumn.disabled !== false) {
          this.currentTableComputedColumns.push(computedColumn)
        }
      })
    },
    addZoom: function () {
      this.modelParserTool.addZoom()
    },
    subZoom: function () {
      this.modelParserTool.subZoom()
    },
    resizeWindow: function (newVal) {
      var wWidth = $(window).width()
      var wHeight = $(window).height()
      var modelEditTool = $(this.$el).find('.model_edit_tool')
      if (this.isFullScreen) {
        this.dockerScreen.w = wWidth
        this.dockerScreen.h = wHeight
      } else {
        if (newVal === 'brief_menu') {
          this.dockerScreen.w = wWidth - 56
          this.dockerScreen.h = wHeight - 176
          modelEditTool.addClass('smallScreen')
        } else {
          modelEditTool.removeClass('smallScreen')
          this.dockerScreen.w = wWidth - 138
          this.dockerScreen.h = wHeight - 176
        }
      }
    },
    changeSnapshotStatus (guid) {
      var tableInfo = this.modelParserTool.getTableInfoByGuid(guid)
      var tableIndex = indexOfObjWithSomeKey(this.tableList, 'guid', guid)
      if (tableInfo.kind === 'FACT') {
        var ccList = this.modelParserTool.getComputedColumnsByGuid(guid)
        if (ccList.length > 0) {
          kapConfirm(this.$t('changeSnapshotTip')).then(() => {
            ccList.forEach((col) => {
              this.delComputedColumn({tableAlias: tableInfo.alias, columnName: col.name}, true)
            })
            this.$set(tableInfo, 'kind', 'LOOKUP')
            this.tableList.splice(tableIndex, 1, tableInfo)
          })
        } else {
          this.$set(tableInfo, 'kind', 'LOOKUP')
          this.tableList.splice(tableIndex, 1, tableInfo)
        }
      } else {
        this.$set(tableInfo, 'kind', 'FACT')
        this.tableList.splice(tableIndex, 1, tableInfo)
      }
      this.getPartitionDateColumns()
    }
  },
  watch: {
    briefMenu: function (newVal, oldVal) {
      this.resizeWindow(newVal)
    }
  },
  mounted () {
    this.project = this.extraoption.project
    this.diagnose({project: this.extraoption.project, modelName: this.extraoption.modelName}).then((res) => {
      handleSuccess(res, (data) => {
        if (data.heathStatus !== 'NONE' && this.actionMode !== 'view') {
          kapConfirm(this.$t('kylinLang.model.resetCheckDataTip'))
        }
      })
    })
    this.resizeWindow(this.briefMenu)
    if (this.kapPlatform === 'cloud') {
      this.isFullScreen = false
      this.toggleFullScreen()
    }
    // this.timerSave()
    var lxsSt = null
    $(window).resize(() => {
      clearTimeout(lxsSt)
      lxsSt = setTimeout(() => {
        this.resizeWindow(this.briefMenu)
      }, 1000)
    })
    $(this.$el).on('mousedown', 'input', function (e) {
      e.stopPropagation()
    }).on('mousedown', function (e) {
      if ($(this).attr('class').indexOf('model_edit') >= 0) {
        document.activeElement.blur()
      }
    })
  },
  computed: {
    ...mapGetters([
      'selectedProjectDatasource'
    ]),
    kapPlatform () {
      return this.$store.state.config.platform
    },
    editLock () {
      for (let i = 0; i < this.cubesList.length; i++) {
        if (this.cubesList[i].segments.length > 0) {
          return true
        }
      }
      return false
    },
    briefMenu () {
      return this.$store.state.config.layoutConfig.briefMenu
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    },
    encodings () {
      var arr = []
      for (var i in this.$store.state.datasource.encodings) {
        arr.push({
          name: i
        })
      }
      return arr
    },
    renderCCList () {
      var result = []
      for (var key in this.ccArray) {
        if (this.ccArray[key].indexOf(this.extraoption.modelName) === -1) {
          result.push({
            value: key,
            label: key,
            model: this.ccArray[key][0]
          })
        }
      }
      return result
    },
    computedRules () {
      return {
        name: [
          { validator: this.checkCCName, trigger: 'change' }
        ],
        expression: [
          {required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change'},
          { validator: this.checkCCExpression, trigger: 'change' }
        ],
        returnType: [
          {required: true, message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'change'}
        ]
      }
    }
  },
  created () {
    this.getAutoModelSql({
      modelName: this.extraoption.modelName
    }).then((res) => {
      handleSuccess(res, (data) => {
        this.sqlString = data.sqls.join(';\r\n')
      })
    }, (res) => {
      handleError(res)
    })
    // this.reloadCubeTree()
  },
  destroyed () {
    clearTimeout(this.timerST)
    this.isFullScreen = false
    $('#fullBox').removeClass('fullLayoutForModelEdit')
    this.resizeWindow(this.briefMenu)
  },
  locales: {
    'en': {'addJoinCondition': 'Add join condition', 'addJoinConditionBtn': 'Join condition', 'hasRootFact': 'There is already a fact table.', 'cannotSetFact': 'This table has a primary key already, so it cannot be a fact table. Please remove or redefine the join condition.', 'cannotSetFTableToFKTable': 'In model, table join condition should start from fact table(foreign key), then pointing it to the lookup table(primary key).', 'tableHasOppositeLinks': 'There is an reverse link between tables.', 'tableHasOtherFKTable': 'There is already a foreign key within this table.', 'delTableTip': 'you should delete the links of other tables before delete this table', 'sameNameComputedColumn': 'There is already a column with the same name', 'addComputedColumnSuccess': 'Computed column added successfuly!', 'checkCompleteLink': 'Connect info is incomplete', hasNoFact: 'Fact Table is mandatory for model', 'checkDraft': 'Detected the unsaved content, are you going to continue the last edit?', filterPlaceHolder: 'Please input filter condition', filterCondition: 'Filter Condition', 'conditionExpress': 'Note that select one column should contain its table name(or alias table name).', changeUsedForConnectColumnTypeWarn: 'Table join key should be a dimension. Exchanging the column(join key) type from dimension to measure is not feasible.', needOneDimension: 'You must select at least one dimension column', needOneMeasure: 'You must select at least one measure column', 'longTimeTip': 'Expression check may take several seconds.', checkingTip: 'The expression check is about to complete, are you sure to break it and save?', checkSuccess: 'Great, the expression is valid.', continueCheck: 'Cancle', continueSave: 'Save', plsCheckReturnType: 'Please select a data type first!', 'autoModelTip1': '1. This function will help you generate a complete model according to entered SQL statements.', 'autoModelTip2': '2. Multiple SQL statements will be separated by ";".', 'autoModelTip3': '3. Please click "x" to check detailed error message after SQL checking.', sqlPatterns: 'SQL Patterns', validFail: 'Uh oh, some SQL went wrong. Click the failed SQL to learn why it didn\'t work and how to refine it.', validSuccess: 'Great! All SQL can perfectly work on this model.', ignoreErrorSqls: 'Ignore Error SQL(s)', ignoreTip: 'Ignored error SQL will have no impact on auto-modeling.', submitSqlTip: 'KAP is generating a new model and it will overwrite current content, are you sure to continue? ', checkIgnore: 'Please correct entered SQL and check again. If you want to submit SQL queries anyway, please check the "Ignore known SQL error" box first.', closeMutilTip: 'Exit', autoSuggestConfirm: 'Suggesting dimensions and measures will overwrite the current version, are you sure you want to continue?', expresscheck: 'Check', delCCTips: 'Deleting the column will remove the connection of the column at the same time, are you sure you want to continue?', addConnectTip: 'Isolated table (without join) cannot have computed column, please add join condition for the table first.', changeSnapshotTip: 'Lookup tables stored as snapshot cannot have computed column. If you check snapshot, it will cause all computed columns from the table deleted, are you sure to continue?', duplicateLinkTip: 'Please check if you has joined table {pktable} with another {fktable} more than once on the same key.', pleaseSelectDataType: 'Please select data type'},
    'zh-cn': {'addJoinCondition': '添加连接条件', 'addJoinConditionBtn': '连接条件', 'hasRootFact': '已经有一个事实表了。', 'cannotSetFact': '本表包含一个主键，因而无法被设置为事实表。请删除或重新定义该连接（join）。', 'cannotSetFTableToFKTable': '模型中，连接（join）条件的建立是从事实表（外键）开始，指向维度表（主键）。', 'tableHasOppositeLinks': '两表之间已经存在一个反向的连接了。', 'tableHasOtherFKTable': '该表已经有一个关联的外键表。', 'delTableTip': '请先删除掉该表和其他表的关联关系。', 'sameNameComputedColumn': '已经有一个同名的列', 'addComputedColumnSuccess': '计算列添加成功！', 'checkCompleteLink': '连接信息不完整', hasNoFact: '模型需要有一个事实表', 'checkDraft': '检测到上次有未保存的内容，是否继续上次进行编辑?', filterPlaceHolder: '请输入过滤条件', filterCondition: '过滤条件', 'conditionExpress': '请注意，表达式中选用某列时，格式为“表名.列名”。', changeUsedForConnectColumnTypeWarn: '表连接关系中的键只能是维度列，请勿在建立连接关系后更改该列类型。', needOneDimension: '至少选择一个维度列', needOneMeasure: '至少选择一个度量列', 'longTimeTip': '表达式校验需要进行十几秒，请稍候。', checkingTip: '表达式校验即将完成，您确定要现在保存？', checkSuccess: '表达式校验结果正确。', continueCheck: '继续校验', continueSave: '直接保存', plsCheckReturnType: '请先选择数据类型！', autoModelTip1: '1. 本功能将根据您输入的SQL语句自动补全建模，提交SQL即生成新模型。', autoModelTip2: '2. 输入多条SQL语句时将以“;”作为分隔。', autoModelTip3: '3. 语法检验后，点击“x”可以查看每条SQL语句的错误信息。', sqlPatterns: 'SQL', validFail: '有无法运行的SQL查询。请点击未验证成功的SQL，获得具体原因与修改建议。', validSuccess: '所有SQL都能被本模型验证。', ignoreErrorSqls: '忽略错误SQL', ignoreTip: '忽略错误的SQL将不会对后续建模产生影响。', submitSqlTip: '即将生成新模型，新模型将覆盖原有模型，您确认要继续吗？', checkIgnore: '请修复SQL错误，再重新检测。若希望忽略这些错误，请勾选“忽略已知的SQL错误”后，再提交。', closeMutilTip: '点击可退出设置', autoSuggestConfirm: '使用系统推荐维度和度量，将会覆盖您当前的设置，确认要继续吗？', expresscheck: '校验', delCCTips: '删除该列将会同时删除该列对应的连接关系，是否继续？', addConnectTip: '没有连接（Join）的独立的表无法建立可计算列，请先为其建立连接关系。', changeSnapshotTip: 'Snapshot存储的表无法建立可计算列。如果选择该配置将会删除该表上所有计算列，确认继续吗？', duplicateLinkTip: '表 {pktable} 与 表 {fktable} 以相同的连接条件（join key）连接了多次。', pleaseSelectDataType: '请选择数据类型'}
  }
}
</script>
<style lang="less">
   @import '../../assets/styles/variables.less';
   [data-scrollbar] .scrollbar-track-y, [scrollbar] .scrollbar-track-y, scrollbar .scrollbar-track-y{
     right: 4px;
   }
    .fullLayoutForModelEdit{
      .left-menu{
        display: none;
      }
      #scrollBox{
        left:0;
        top:0;
      }
      .model_panel .model_edit_tool{
        left: 0;
      }
      .topbar{
        display: none;
      }
      .model_panel .model_edit_tool.smallScreen{
        left: 0;
      }
      #modeltab>.el-tabs--card>.el-tabs__header{
        display: none;
      }
      #breadcrumb_box{
        display: none;
      }
      .btn_group{
        bottom:152px;
      }
    }
   .model_edit_box {
    .cc-addedit-form{
      border:solid 1px @line-border-color;
      padding: 10px 20px;
      margin-top: 20px;
    }
    .color_block{
      width: 9px;
      height: 9px;
      display: inline-block;
      vertical-align: middle;
    }
    .color_fact{
      background-color: @text-normal-color;
    }
    .color_lookup{
      background-color: @text-secondary-color;
    }
    .tree-menu-toggle{
       position: absolute;
       top: 240px;
       height: 84px;
       width: 24px;
       // padding: 5px;
       text-align:center;
       background-color: @modeledit-bg-color;
       // border: 1px solid @line-border-color;
       z-index: 2000;
       color: @text-normal-color;
       i {
        position: relative;
        top: 34px;
        color: @text-normal-color;
        font-size:14px;
      }
    }
    .treeBtnLeft {
      left: 0px;
      border-radius: 0 4px 4px 0;
      cursor: pointer;
      border: 1px solid @text-secondary-color;
      border-left: none;
    }
    .treeBtnRight {
      right: 0px;
      border-radius:4px 0 0 4px;
      border: 1px solid @text-secondary-color;
      cursor: pointer;
      border-right: none;
    }
    .checkresult {
       color: red;
       font-size: 12px;
       word-break: break-all;
       line-height: 20px;
       &.isvalid {
         color:green;
       }
     }
    .saveModel {
      .el-dialog__body {
        padding: 0;
      }
    }
    .computedColumn {
      .el-dialog {
        width: 720px;
        .el-dialog__body {
          // padding: 10px 30px 15px 30px;
          .el-form-item {
            margin-bottom: 10px;
            .el-form-item__error {
              position: relative;
              top: 0;
              color: #ff4949;
              font-size: 12px;
              line-height: 1;
              padding-top: 4px;
              left: 0;
              word-wrap: break-word;
            }
          }
          .el-form-item.cc-expression  .el-form-item__error{
            top: 5px;
          }
        }
      }
    }
    .notable_tip{
      position: absolute;
      top: 50%;
      left: 50%;
      width:400px;
      height:400px;
      text-align: center;
      margin-left: -100px;
      margin-top: -100px;
    }
    position: relative;
    background-color: @modeledit-bg-color;
    .tree_list {
      height: 100%;
      position: absolute;
      z-index: 2000;
      overflow-y: auto;
      overflow-x: hidden;
      padding-top: 10px;
      border-right: solid 1px @line-border-color;
      .filter-tree{
        overflow:hidden;
    }
    }


   .btn_group{
    position: absolute;
    bottom: 36px;
    right: 28px;
    z-index: 10;
   }
   .model_edit{
     display: inline-block;
     position: absolute;
     width: 100%;
     height: 100%;
     z-index: 9!important;
     width: 40000px;
     height: 40000px;
     cursor: move;
   }
   .links_dialog{
    .el-dialog__body {
      padding-top: 10px;
    }
   }
   .table_box{
        .el-input--small .el-input__inner {
          height: 28px;
        }
       .close_table{
        position: absolute;
        top: 8px;
        right: 6px;
        color:@text-normal-color;
        font-size: 18px;
        cursor: pointer;
       }
       .column_in {
        background-color: @modeledit-bg-color;
       }
       &.rootfact{
          .table_name{
            color:@fff;
            background-color: @text-normal-color;
            &:hover{
              background-color: #587380;
            }
          }
          .close_table {
            color: @modeledit-bg-color;
          }
       }
       .alias_input{
          position: absolute;
          top:4px;
          left: 10px;
          height: 26px;
          width: 200px;
          background:none;
          border:none;
          color:#fff;
          cursor: move;
          font-size: 14px;
       }
       width: 220px;
       left: 440px;
       z-index:1;
       position: absolute;
       min-height: 400px;
       background:@fff;
       box-shadow:0 2px 4px 0 rgba(0,0,0,0.12), 0 0 6px 0 rgba(0,0,0,0.04);
       border-radius:2px;
       .link_box{
         i{
           padding: 5px;
           cursor: pointer;
         }
         position: absolute;
         background-color: #58b7ff;
         height: 40px;
         width: 100%;
         top:-40px;
         color:#fff;
         line-height: 40px;
         // text-align: center;
         z-index: 11;
       }
       
       .more_tool{
          text-align: center;
          i{
            color: #fff;
            font-weight:lighter;
          }
       }
       .filter_box{
        padding: 5px 10px;
        margin-top:30px;
        border-bottom: solid 1px @line-border-color;
       }
       .table_name{
         height: 36px;
         line-height: 36px;
         background-color: @text-secondary-color;
         padding-left:10px;
         color:@text-title-color;
         &:hover{
          background-color: #bdccd3;
         }
       }
       .drag_bar{
        span{
          cursor: text;
          font-size: 14px;
          font-weight: bolder;
        }
       }
       .columns_box{
        height: 290px;
        overflow: hidden;
        cursor: default;
        padding: 0 2px;
        margin-bottom: 10px;
        &:hover{
          .scrollbar-track-y{
            opacity: 1;
          }
        }
       }
       ul.columns-body{
        // position: absolute;
        margin-top: 4px;
        li{
          list-style: none;
          font-size: 12px;
          height: 30px;
          line-height: 30px;
          color:@text-title-color;
          cursor: move;
          border-bottom: solid 1px @table-stripe-color;
          &.pointer{
            cursor:pointer;
          }
          &.is_computed{
             color:@tablelink-line-color;
           }
          // background-color: #2f3242;
          &.selected{
            background:@dm-bg-color;
            // box-shadow:inset 0 -1px 0 0 #333647;
            color:@base-color;
            .kind{
              background-color: @base-color-5;
            }
          }
          span{
            display: inline-block;
          }
          &.active_filter{
            // font-weight: bold;
            color:#218fea;
          }
          &:hover{
            background:@dm-bg-color;
            .kind {
              // background-color:@base-color-8;
            }
            .column{
              // background:@dm-bg-color;
            }
          }
          .kind{
            color: @fff;
            width: 16px;
            height: 16px;
            text-align: center;
            line-height: 16px;
            vertical-align: baseline;
            cursor:pointer;
            background:@base-color-6;
            border-radius:100%;
            font-size: 12px;
            &.dimension{

            }
            &.measure{

            }
            &:hover{
              background-color:@base-color-5;
              // color:@fff;
            }
          }
          .column_type{
            right:2px;
            position: absolute;
            color:@modeledit-columntype-color;
          }
        }
       }
   }
   .jtk-overlay {
    background-color: @tablelink-line-overlay-bg-color;
    padding: 0.4em;
    font: 12px sans-serif;
    z-index: 21;
    font-weight: bold;
    border: 2px solid @tablelink-line-color;
    cursor: pointer;
    min-width: 42px;
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
  }
}
</style>

