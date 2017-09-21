<template>
<div class="model_edit_box"  @drop='drop($event)' @dragover='allowDrop($event)' :style="{width:dockerScreen.w+'px', height:dockerScreen.h+'px'}">

    <div class="tree_list" >
<!--     <draggable  @start="drag=true" @end="drag=false"> -->
      <model-assets  v-on:drag="drag" :project="extraoption.project" @okFunc="serverDataToDragData" ></model-assets>
      <!-- <el-tree v-if="extraoption.uuid" @nodeclick="clickCube" :data="cubeDataTree" style="background-color: #f1f2f7;border:none;width:250px;" :render-content="renderCubeTree"></el-tree> -->
      <tree  v-if="extraoption.uuid && !isFullScreen && !modelInfo.is_draft" style="background-color: #f1f2f7;border:none;width:250px;" :treedata="cubeDataTree" :placeholder="$t('kylinLang.common.pleaseFilter')" maxLabelLen="20" :showfilter= "false" :expandall="true" @nodeclick="clickCube"  v-unselect :renderTree="renderCubeTree"></tree>
<!--     </draggable> -->
    </div>
    <ul class="sample_info">
      <li><span class="iconD">D </span><span class="info">Dimension</span></li>
      <li><span class="iconM">M </span><span class="info">Measure</span></li>
      <li><span class="iconDis">－ </span><span class="info">Disable</span></li>
    </ul>
    <div class="notable_tip" v-show="!(tableList&&tableList.length)">
      <img src="../../assets/img/dragtable.png">
      <div style="font-size:14px;color:#4f5473" class="ksd-mt-20">{{$t('kylinLang.model.dragTip')}}</div>
    </div>
    <ul class="model_tool">
        <li class="toolbtn tool_add" @click="addZoom" v-unselect :title="$t('kylinLang.common.zoomIn')" style="line-height:38px;"><img src="../../assets/img/fd.png"></li>
        <li class="toolbtn tool_jian" @click="subZoom" v-unselect :title="$t('kylinLang.common.zoomOut')" style="line-height:26px;"><img src="../../assets/img/sx.png"></li>
        <li class="toolbtn" @click="autoLayerPosition" v-unselect  :title="$t('kylinLang.common.automaticlayout')" style="line-height:42px;margin-top:10px"><img src="../../assets/img/layout.png"></li>
        <li class="toolbtn" @click="toggleFullScreen" v-unselect  :title="$t('kylinLang.common.automaticlayout')" style="line-height:42px;margin-top:10px"><img style="width:26px;height:22px;" src="../../assets/img/full-screen.png"></li>
      </ul>
    <div class="btn_group"  v-if="actionMode!=='view'">
      <!-- <el-button @click="saveDraft(true)" :loading="draftBtnLoading">{{$t('kylinLang.common.draft')}}</el-button> -->
      <el-button type="primary" @click="saveCurrentModel" :loading="saveBtnLoading">{{$t('kylinLang.common.save')}}</el-button>
    </div>
    <div class="tips_group">

    </div>
    <div class="model_edit" :style="{left:docker.x +'px'  ,top:docker.y + 'px'}">
      <div class="table_box" v-if="table&&table.kind" @drop='dropColumn' @dragover='allowDrop($event)'  v-for="table in tableList" :key="table.guid" :id="table.guid" v-bind:class="table.kind.toLowerCase()" v-bind:style="{ left: table.pos.x + 'px', top: table.pos.y + 'px' }" >
        <div class="tool_box">
            <span >
              <icon name="table" class="el-icon-menu" style="color:#fff" @click.native="openModelSubMenu('hide', table.database, table.name)"></icon>
            </span>
            <span v-show="table.kind !== 'LOOKUP'">
              <icon name="calculator"   class="el-icon-share" style="color:#fff" v-on:click.native="addComputedColumn(table.guid)"></icon>
            </span>
            <span>
              <icon name="sort-alpha-asc" v-on:click.native="sortColumns(table)"></icon>
            </span>
             <span v-show="table.kind == 'ROOTFACT'" style="line-height:26px;font-weight:bold;" @click="inputSql">
              SQL
            </span>
            <i class="fa fa-window-close"></i>
            <el-dropdown @command="selectTableKind" class="ksd-fright" v-if="actionMode!=='view'">
              <span class="el-dropdown-link">
               <i class="el-icon-setting" style="color:#fff"></i>
              </span>
              <el-dropdown-menu slot="dropdown" >
                <el-dropdown-item command="ROOTFACT" :data="table.guid">{{$t('kylinLang.common.fact')}}</el-dropdown-item>
                <el-dropdown-item command="FACT" v-show="useLimitFact" :data="table.guid">{{$t('kylinLang.common.limitfact')}}</el-dropdown-item>
                <el-dropdown-item command="LOOKUP" :data="table.guid">{{$t('kylinLang.common.lookup')}}</el-dropdown-item>
              </el-dropdown-menu>
              </el-dropdown>
        </div>
        <i class="el-icon-close close_table" v-on:click="removeTable(table.guid)" v-if="actionMode!=='view'"></i>
        <p class="table_name  drag_bar" v-on:dblclick="editAlias(table.guid)" v-visible="aliasEditTableId!=table.guid">
        <common-tip :tips="table.database+'.'+table.name" class="drag_bar">{{(table.alias)|omit(16,'...')}}</common-tip></p>
        <el-input v-model="table.alias" v-on:blur="cancelEditAlias(table.guid)" class="alias_input"  size="small" :placeholder="$t('kylinLang.common.enterAlias')" v-visible="aliasEdit&&aliasEditTableId==table.guid"></el-input>
        <p class="filter_box"><el-input v-model="table.filterName" v-on:change="filterColumnByInput1(table.filterName,table.guid)"  size="small" :placeholder="$t('kylinLang.common.pleaseFilter')"></el-input></p>
        <section data-scrollbar class="columns_box">
          <ul>
            <li draggable  @dragstart="dragColumns" @dragend="dragColumnsEnd"  v-for="column in table.columns" :key="column.guid"  class="column_li" v-show="column.isShow!==false"  v-bind:class="{'active_filter':column.isActive, 'is_computed': column.isComputed}" :data-guid="table.guid" :data-btype="column.btype" :data-isComputed="column.isComputed" :data-column="column.name" ><span class="kind" :class="{dimension:column.btype=='D',measure:column.btype=='M'}" v-on:click="changeColumnBType(table.guid,column.name,column.btype, column.isComputed)">{{column.btype}}</span><span class="column" @dragleave="dragColumnsLeave" @dragenter="dragColumnsEnter" v-on:click="selectFilterColumn(table.guid,column.name,column.datatype)"><common-tip trigger="click" :tips="column.name" placement="right-start" style="font-size:10px;">{{column.name|omit(14,'...')}}</common-tip></span><span class="column_type">{{column.datatype}}</span></style></li>
          </ul>
        </section>
        <div class="more_tool"></div>
      </div>


    </div>
     <el-dialog :title="$t('addJoinCondition')" v-model="dialogVisible" size="small" class="links_dialog" @close="saveLinks(currentLinkData.source.guid,currentLinkData.target.guid, true)" :close-on-press-escape="false" :close-on-click-modal="false">
        <span>
            <br/>
             <el-row :gutter="20" class="ksd-mb10" style="line-height:49px;">
             <el-col :span="24" style="text-align:center">
              <div class="grid-content bg-purple">
              <el-input v-model="currentLinkData.source.alias" :disabled="true" style="background: #393e53;"></el-input>
                <!-- <a style="color:#56c0fc">{{currentLinkData.source.alias}}</a> -->
              </div>
            </el-col>
            <el-col :span="24" style="text-align:center">
              <div class="grid-content bg-purple">
                  <el-select style="width:100%;background: #393e53;" v-model="currentLinkData.joinType"  :disabled = "actionMode ==='view'"  @change="switchJointType(currentLinkData.source.guid,currentLinkData.target.guid, currentLinkData.joinType)" :placeholder="$t('kylinLang.common.pleaseSelect')">
                    <el-option
                      v-for="item in joinTypes"
                      :key="item.label"
                      :label="item.label"
                      :value="item.value">
                    </el-option>
                  </el-select>
              </div>
            </el-col>
            <el-col :span="24" style="text-align:center">
              <div class="grid-content bg-purple">
              <el-input v-model="currentLinkData.target.alias" :disabled="true" style="background: #393e53;"></el-input>
                <!-- <a style="color:#56c0fc">{{currentLinkData.target.alias}}</a> -->
              </div>
            </el-col>
            </el-row>
          </el-row>
          <br/>
           <el-table
              :data="currentTableLinks"
              :show-header="false"
              style="width: 100%;border:none" class="linksTable">
              <el-table-column style="border:none"
                :label="$t('kylinLang.common.pk')"
                >
                <template scope="scope">
                <el-select v-model="scope.row[2]" :placeholder="$t('kylinLang.common.pleaseSelect')" style="width:100%;background: #393e53;" :disabled = "actionMode ==='view'">
                  <el-option
                    v-for="item in currentLinkData.source.columns"
                    :key="item.name"
                    :label="item.name"
                    :value="item.name">
                  </el-option>
                </el-select>
                </template>
              </el-table-column>
              <el-table-column  style="border:none" label="" align="center" width="40">
                 <template scope="scope" align="center">＝</template>
              </el-table-column>
              <el-table-column style="border:none"
                :label="$t('kylinLang.common.fk')"
                >
                <template scope="scope">
                <el-select v-model="scope.row[3]" :placeholder="$t('kylinLang.common.pleaseSelect')" style="width:100%;background: #393e53;" :disabled = "actionMode ==='view'">
                  <el-option
                    v-for="item in currentLinkData.target.columns"
                    :key="item.name"
                    :label="item.name"
                    :value="item.name">
                  </el-option>
                </el-select>
                </template>
              </el-table-column>
              <el-table-column style="border:none" :label="$t('kylinLang.common.action')" width="80" v-if="actionMode!=='view'">
                <template scope="scope" >
                  <confirm-btn  v-on:okFunc='delConnect(scope.row)' :tips="$t('kylinLang.common.confirmDel')"><el-button size="small"
          type="danger">{{$t('kylinLang.common.drop')}}</el-button></confirm-btn>
                </template>
              </el-table-column>
            </el-table>
            <br/>
            <el-button type="blue" v-if="actionMode!=='view'" @click="addJoinCondition(currentLinkData.source.guid,currentLinkData.target.guid, '', '', currentLinkData.joinType,true)">{{$t('addJoinCondition')}}</el-button>
        </span>
         <span slot="footer" class="dialog-footer">
            <el-button type="primary" @click="saveLinks(currentLinkData.source.guid,currentLinkData.target.guid)">{{$t('kylinLang.common.ok')}}</el-button>
          </span>
      </el-dialog>
       <el-dialog :title="$t('kylinLang.common.computedColumn')" v-model="computedColumnFormVisible" size="small" :close-on-press-escape="false" :close-on-click-modal="false">
          <div>
            <el-button type="trans" class="ksd-mb-10 radius" icon="plus" v-show="!openAddComputedColumnForm&&actionMode!=='view'" @click="addComputedForm">{{$t('kylinLang.common.add')}}</el-button>
            <el-form label-position="top" :model="computedColumn"  ref="computedColumnForm"  :rules="computedRules" v-show="openAddComputedColumnForm">
              <el-form-item :label="$t('kylinLang.dataSource.columns')" prop="name">
                <el-input  auto-complete="off" v-model="computedColumn.name"></el-input>
              </el-form-item>
              <el-form-item :label="$t('kylinLang.dataSource.expression')" prop="expression">
                <span slot="label">{{$t('kylinLang.dataSource.expression')}} <common-tip :content="$t('conditionExpress')" ><icon name="question-circle-o"></icon></common-tip></span>
                <!-- <el-input type="textarea"  auto-complete="off" v-model="computedColumn.expression"></el-input> -->
               <editor v-model="computedColumn.expression" @click.native="completeInput" ref="expressionBox" lang="sql" theme="monokai" width="100%" height="100" useWrapMode="true"></editor>
               <p :class="{isvalid:checkExpressResult.isValid}" v-if="checkExpressResult.msg" class="checkresult">
                 {{checkExpressResult.msg}}
               </p>
               <div v-show="computedColumn.expression">
                <el-button size="small" @click="checkExpression" :loading="checkExpressionBtnLoad">Expression Check</el-button>
                <common-tip :content="$t('longTimeTip')" >
                   <icon name="question-circle-o"></icon>
                </common-tip>
                <el-button type="text" size="mini" v-show="checkExpressionBtnLoad" @click="cancelCheckExpression" >{{$t('kylinLang.common.cancel')}}</el-button>
              </div>
              </el-form-item>
              <el-form-item :label="$t('kylinLang.dataSource.returnType')" prop="returnType">
                <el-select v-model="computedColumn.returnType">
                  <el-option
                    v-for="(item, index) in computedDataTypeSelects"
                    :key="item"
                    :label="item"
                    :value="item">
                  </el-option>
                </el-select>
                <!-- <el-input  auto-complete="off" v-model="computedColumn.returnType"></el-input> -->
              </el-form-item>
              <el-form-item>
                <el-button type="trans" class="radius" @click="cancelComputedEditForm">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="trans" class="radius" @click="saveComputedColumn">{{$t('kylinLang.common.submit')}}</el-button>
              </el-form-item>
                <div class="line-primary" style="margin: 0px -20px;"></div>
            </el-form>
            <kap-nodata v-if="!(currentTableComputedColumns && currentTableComputedColumns.length)"></kap-nodata>
            <el-table border v-if="currentTableComputedColumns && currentTableComputedColumns.length"
              :data="currentTableComputedColumns"
              style="width: 100%; font-size:12px;">
              <el-table-column
                prop="columnName"
                :label="$t('kylinLang.dataSource.columns')"
                width="180">
              </el-table-column>
              <el-table-column
                prop="expression"
                :label="$t('kylinLang.dataSource.expression')"
                >
                 <template scope="scope" >
                    <el-tooltip class="item" effect="dark" :content="scope.row&&scope.row.expression" placement="top">
                        <span >{{scope.row.expression|omit(24, '...')}}</span>
                    </el-tooltip>
                 </template>
              </el-table-column>
              <el-table-column
              width="100"
                prop="datatype"
                :label="$t('kylinLang.dataSource.returnType')">
              </el-table-column>
              <el-table-column
                 width="140"
                :label="$t('kylinLang.common.action')" v-if="actionMode!=='view'">
                <template scope="scope">
                  <el-button  icon="edit" size="small" v-on:click='editComputedColumn(scope.row)'></el-button>
                   <confirm-btn  v-on:okFunc='delComputedColumn(scope.row)' :tips="$t('kylinLang.common.confirmDel')">
                   <el-button size="small" icon="delete" ></el-button></confirm-btn>
                </template>
              </el-table-column>
            </el-table>
          </div>
            <span slot="footer" class="dialog-footer">
            <el-button @click="computedColumnFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
            <el-button type="primary" @click="computedColumnFormVisible = false">{{$t('kylinLang.common.ok')}}</el-button>
          </span>
        </el-dialog>
      <model-tool @changeColumnType="changeColumnBType" v-if="modelDataLoadEnd" :modelInfo="modelInfo" :actionMode="actionMode" :editLock="editLock" :compeleteModelId="modelData&&modelData.uuid||null" :columnsForTime="timeColumns" :columnsForDate="dateColumns"  :activeName="submenuInfo.menu1" :activeNameSub="submenuInfo.menu2" :tableList="tableList" :partitionSelect="partitionSelect"  :selectTable="currentSelectTable" ref="modelsubmenu"></model-tool>

       <!-- 添加cube -->

    <el-dialog :title="$t('kylinLang.cube.addCube')"  v-model="createCubeVisible" size="tiny" :close-on-press-escape="false" :close-on-click-modal="false">
      <el-form :model="cubeMeta" :rules="createCubeFormRule" ref="addCubeForm">
        <el-form-item label="Cube Name" prop="cubeName">
          <el-input v-model="cubeMeta.cubeName" auto-complete="off"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="createCubeVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="createCube">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog :title="$t('kylinLang.common.save')" v-model="addModelDialogDisable" size="small" :close-on-press-escape="false" :close-on-click-modal="false" >
       <partition-column :comHeight="450" :modelInfo="modelInfo" :actionMode="actionMode" :columnsForTime="timeColumns" :columnsForDate="dateColumns" :tableList="tableList" :partitionSelect="partitionSelect" ></partition-column>
       <el-checkbox v-show="!hasStreamingTable" v-model="openModelCheck">{{$t('kylinLang.model.checkModel')}}</el-checkbox>
       <common-tip v-show="!hasStreamingTable" :content="$t('kylinLang.model.modelCheck')" >
             <icon name="question-circle-o"></icon>
       </common-tip>
     <!--    <el-slider v-model="modelStaticsRange" :max="100" :format-tooltip="formatTooltip" :disabled = '!openModelCheck'></el-slider> -->
        <!-- <slider label="Check Model" @changeBar="changeModelBar" :show="addModelDialogDisable"></slider> -->
       <div slot="footer" class="dialog-footer">
        <el-button @click="addModelDialogDisable = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="saveAndCheckModel" :loading="saveBtnLoading">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog :title="$t('sqlPatterns')" v-model="addSQLFormVisible" :before-close="sqlClose" :close-on-press-escape="false" :close-on-click-modal="false">
    <p style="font-size:12px">{{$t('autoModelTip1')}}</p>
    <p style="font-size:12px">{{$t('autoModelTip2')}}</p>
    <p style="font-size:12px">{{$t('autoModelTip3')}}</p>
    <div :class="{hasCheck: hasCheck}">
    <editor v-model="sqlString" ref="sqlbox" theme="chrome"  class="ksd-mt-20" width="95%" height="200" ></editor>
    </div>
    <!-- <div class="checkSqlResult">{{errorMsg}}</div> -->
    <!-- <div> <icon v-if="result && result.length === 0" name="check" style="color:green"></icon></div> -->
    <transition name="fade">
    <div v-if="errorMsg">
     <el-alert class="ksd-mt-10 trans"
        :title="errorMsg"
        show-icon
        :closable="false"
        type="error">
      </el-alert>
    </div>
    </transition>
    <transition name="fade">
    <div v-if="successMsg">
     <el-alert class="ksd-mt-10 trans"
        :title="successMsg"
        show-icon
        :closable="false"
        type="success">
      </el-alert>
    </div>
    </transition>
    <div class="ksd-mt-4">
      <el-button  style="width: 70px;height:30px;" :loading="checkSqlLoadBtn" size="mini" @click="validateSql" :disabled="actionMode==='view' || sqlString === ''" >{{$t('kylinLang.common.check')}}</el-button>
      <el-button type="text" v-show="checkSqlLoadBtn" @click="cancelCheckSql">{{$t('kylinLang.common.cancel')}}</el-button></div>
    <span slot="footer" class="dialog-footer">
      <el-checkbox class="ksd-fleft" v-model="ignoreErrorSql" v-show="hasValidFailSql && hasValidSuccessSql">{{$t('ignoreErrorSqls')}}</el-checkbox>
      <common-tip :content="$t('ignoreTip')" class="ksd-fleft ksd-ml-4" v-show="hasValidFailSql && hasValidSuccessSql">
        <icon name="question-circle-o"></icon>
      </common-tip>
      <el-button @click="sqlClose()">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" :loading="sqlBtnLoading" @click="autoModel" :disabled="actionMode==='view' || (ignoreErrorSql === false && hasValidSuccessSql && hasValidFailSql)|| !hasValidSuccessSql || !hasCheck" >{{$t('kylinLang.common.submit')}}</el-button>
    </span>
  </el-dialog>
</div>
</template>
<script>
import { jsPlumb } from 'jsplumb'
import { sampleGuid, indexOfObjWithSomeKey, filterObjectArray, objectArraySort, objectClone, getNextValInArray, isIE } from '../../util/index'
import { NamedRegex, DatePartitionRule, permissions, TimePartitionRule, IntegerType } from '../../config'
import { mapActions } from 'vuex'
import $ from 'jquery'
import Scrollbar from 'smooth-scrollbar'
import modelassets from './model_assets'
import Draggable from 'draggable'
import modelEditTool from 'components/model/model_edit_panel'
import partitionColumn from 'components/model/model_partition.vue'
import { handleSuccess, handleError, hasRole, filterNullValInObj, kapConfirm, hasPermission, filterMutileSqlsToOneLine } from 'util/business'
export default {
  name: 'modeledit',
  components: {
    'model-assets': modelassets,
    'model-tool': modelEditTool,
    'partition-column': partitionColumn
  },
  props: ['extraoption'],
  data () {
    return {
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
      result: [],
      checkExpressionBtnLoad: false,
      checkExpressResult: {},
      currentTableComputedColumns: [],
      computedRules: {
        name: [
          {required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change'},
          { trigger: 'blur', validator: this.checkName }
        ],
        expression: [
          {required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'change'}
        ],
        returnType: [
          {required: true, message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'change'}
        ]
      },
      modelDataLoadEnd: false,
      openAddComputedColumnForm: false,
      createCubeVisible: false,
      openModelCheck: true,
      modelStaticsRange: 100,
      addModelDialogDisable: false,
      isFullScreen: false,
      hasStreamingTable: false,
      createCubeFormRule: {
        cubeName: [
          {required: true, message: this.$t('kylinLang.cube.inputCubeName'), trigger: 'blur'},
          {validator: this.checkName, trigger: 'blur'}
        ]
      },
      cubeMeta: {
        cubeName: '',
        modelName: '',
        projectName: ''
      },
      actionMode: 'edit',
      firstRenderServerData: false,
      // 编辑锁
      editLock: false,
      linksLock: false,
      columnKindLock: false,
      // table 添加 修改类型  修改别名
      tableLock: false,
      // 定时保存配置
      saveConfig: {
        timer: 0,
        limitTimer: 5
      },
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
        }
      },
      // encodings: loadBaseEncodings(this.$store.state.datasource),
      partitionSelect: {
        'date_table': '',
        'date_column': '',
        'time_table': '',
        'time_column': '',
        'partition_date_column': '',
        'partition_time_column': '',
        'partition_date_start': 0,
        'partition_date_format': '',
        'partition_time_format': '',
        'partition_type': 'APPEND'
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
      columnBType: ['D', 'M', '－'],
      joinTypes: [{label: 'Inner Join', value: 'inner'}, {label: 'Left Join', value: 'left'}],
      timerST: null,
      baseLineL: 20000,
      baseLineT: 20000,
      hisModelJsonStr: '',
      modelAssets: [],
      cubeDataTree: [{
        id: 1,
        label: 'Cube',
        children: []
      }],
      currentZoom: 0.7,
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
      currentMenuStatus: 'hide',
      columnUsedInfo: [],
      noMoreAlert: false
    }
  },
  beforeDestroy () {
    this.removeAllEndpoints(this.plumbInstance)
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
      autoModelApi: 'AUTO_MODEL',
      checkSql: 'VALID_AUTOMODEL_SQL',
      getAutoModelSql: 'GET_AUTOMODEL_SQL'
    }),
    resetModelEditArea () {
      this.tableList = []
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
        }
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
        confirmButtonText: this.$t('kylinLang.common.close'),
        cancelButtonText: this.$t('kylinLang.common.cancel')
      }).then(() => {
        this.addSQLFormVisible = false
      })
    },
    submitSqlPatterns () {
      kapConfirm(this.$t('submitSqlTip'), {
        confirmButtonText: this.$t('kylinLang.common.continue')
      }).then(() => {
        this.autoModel()
      })
    },
    autoModel () {
      var sqls = filterMutileSqlsToOneLine(this.sqlString)
      if (sqls.length === 0) {
        return
      }
      this.sqlBtnLoading = true
      var rootFact = this.getRootFact()
      if (rootFact.length) {
        var rootFactName = rootFact[0].database + '.' + rootFact[0].name
        this.autoModelApi({ modelName: this.modelInfo.modelName, project: this.project, sqls: sqls, factTable: rootFactName }).then((res) => {
          handleSuccess(res, (data) => {
            this.sqlBtnLoading = false
            this.addSQLFormVisible = false
            this.removeAllLinks()
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
      var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor || ''
      this.renderEditerRender(editor)
      editor && editor.removeListener('change', this.editerChangeHandle)
      this.errorMsg = false
      this.checkSqlLoadBtn = true
      editor.setOption('wrap', 'free')
      this.sqlString = sqls.join(';\r\n')
      var rootFact = this.getRootFact()
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
      this.sqlString = ''
      this.errorMsg = ''
      this.successMsg = ''
      this.ignoreErrorSql = false
      this.addSQLFormVisible = true
      this.$nextTick(() => {
        var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor
        if (editor) {
          editor && editor.removeListener('change', this.editerChangeHandle)
          if (this.actionMode === 'view') {
            editor.setReadOnly(true)
          } else {
            editor.setReadOnly(false)
          }
          this.getAutoModelSql({
            modelName: this.modelInfo.modelName
          }).then((res) => {
            handleSuccess(res, (data) => {
              var result = data
              var sqls = result.sqls
              var errorInfo = result.results
              // for (var i in result) {
              //   sqls.push(i)
              //   errorInfo.push(result[i])
              //   this.hasCheck = true
              // }
              this.hasCheck = true
              this.sqlString = sqls.join(';\r\n')
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
    checkExpression () {
      if (!this.computedColumn.returnType) {
        this.$message(this.$t('plsCheckReturnType'))
        return
      }
      this.$message(this.$t('longTimeTip'))
      var editor = this.$refs.expressionBox.editor
      editor.setReadOnly(true)
      this.checkExpressionBtnLoad = true
      var checkData = JSON.parse(this.DragDataToServerData(false, true))
      var needCheckComputedObj = {}
      var guid = this.computedColumn.guid
      var databaseInfo = this.getTableInfoByGuid(guid)
      needCheckComputedObj = {
        tableIdentity: databaseInfo.database + '.' + databaseInfo.name,
        tableAlias: databaseInfo.alias,
        columnName: this.computedColumn.name,
        expression: this.computedColumn.expression,
        comment: this.computedColumn.comment,
        datatype: this.computedColumn.returnType,
        disabled: true
      }
      checkData.computed_columns = [needCheckComputedObj]
      this.checkComputedExpression({
        modelDescData: JSON.stringify(checkData),
        project: this.project
      }).then((res) => {
        this.checkExpressionBtnLoad = false
        editor.setReadOnly(false)
        this.checkExpressResult.isValid = true
        handleSuccess(res, (data) => {
          this.checkExpressResult.msg = this.$t('checkSuccess')
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
      var editor = this.$refs.expressionBox.editor
      editor.setReadOnly(false)
      this.checkExpressionBtnLoad = false
    },
    toggleFullScreen () {
      this.isFullScreen = !this.isFullScreen
      $('#fullBox').toggleClass('fullLayoutForModelEdit')
      this.resizeWindow(this.briefMenu)
    },
    addComputedForm () {
      this.openAddComputedColumnForm = true
      this.computedColumn.name = ''
      this.computedColumn.expression = ''
      this.computedColumn.returnType = ''
      this.checkExpressResult = {isValid: '', msg: ''}
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
    // 检查是否上锁了
    checkLock (msg) {
      if (this.editLock && msg !== false) {
        this.warnAlert(msg || '编辑模式下不允许该操作')
      }
      return this.editLock
    },
    changeModelBar (val) {
      this.modelStaticsRange = val
      this.openModelCheck = !!val
    },
    // 保存正式
    saveCurrentModel () {
      var rootFact = this.getRootFact()
      if (!rootFact.length) {
        this.warnAlert(this.$t('hasNoFact'))
        return
      }
      var dimension = this.getDimensions()
      if (dimension.length === 0) {
        this.$message({
          type: 'warning',
          message: this.$t('needOneDimension')
        })
        return
      }
      this.hasStreamingTable = this.getTableList('source_type', 1).length > 0
      if (this.draftBtnLoading) {
        this.$message({
          type: 'warning',
          message: this.$t('kylinLang.common.saveDraft')
        })
        return
      }
      this.addModelDialogDisable = true
      // }
    },
    saveAndCheckModel () {
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
        if (this.openModelCheck && !this.hasStreamingTable) {
          this.statsModel({
            project: this.project,
            modelname: this.modelInfo.modelName,
            data: {
              ratio: (this.modelStaticsRange / 100).toFixed(2)
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
    // 保存草稿
    saveDraft: function (tipChangestate) {
      // 正在保存正式的时候不允许保存draft
      if (this.saveBtnLoading) {
        return
      }
      if (this.actionMode === 'view') {
        return
      }
      if (this.getRootFact().length <= 0) {
        return
      }
      if (!this.checkModelMetaHasChange()) {
        if (tipChangestate) {
          this.$message({
            type: 'warning',
            message: this.$t('kylinLang.common.checkNoChange')
          })
        }
        return
      }
      this.draftBtnLoading = true
      this.saveModelDraft({
        modelDescData: filterNullValInObj(this.DragDataToServerData()),
        project: this.project,
        modelName: this.modelInfo.name
      }).then((res) => {
        this.draftBtnLoading = false
        handleSuccess(res, (data) => {
          this.modelInfo.uuid = data.uuid
          this.modelInfo.is_draft = true
          this.modelInfo.last_modified = JSON.parse(data.modelDescData).last_modified
          this.$emit('reload', 'modelList')
        })
      }, (res) => {
        handleError(res)
        this.draftBtnLoading = false
      })
    },
    addCube () {
      this.createCubeVisible = true
      this.initCubeMeta()
      this.cubeMeta.projectName = this.project
      this.cubeMeta.modelName = this.modelInfo.modelName
    },
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    },
    initCubeMeta () {
      this.cubeMeta = {
        cubeName: '',
        modelName: '',
        projectName: ''
      }
    },
    createCube () {
      this.$refs['addCubeForm'].validate((valid) => {
        if (valid) {
          this.checkCubeName({cubeName: this.cubeMeta.cubeName, project: this.cubeMeta.projectName}).then((res) => {
            handleSuccess(res, (data) => {
              if (data && data.size > 0) {
                this.$message({
                  message: this.$t('kylinLang.cube.sameCubeName'),
                  type: 'warning'
                })
              } else {
                this.createCubeVisible = false
                this.$emit('addtabs', 'cube', this.cubeMeta.cubeName, 'cubeEdit', {
                  project: this.cubeMeta.projectName,
                  cubeName: this.cubeMeta.cubeName,
                  modelName: this.cubeMeta.modelName,
                  isEdit: false
                })
              }
            })
          }, (res) => {
            handleError(res)
          })
        }
      })
    },
    getProjectIdByName (pname) {
      var projectList = this.$store.state.project.allProject
      var len = projectList && projectList.length || 0
      var projectId = ''
      for (var s = 0; s < len; s++) {
        if (projectList[s].name === pname) {
          projectId = projectList[s].uuid
        }
      }
      return projectId
    },
    hasSomePermissionOfProject () {
      var projectId = this.getProjectIdByName(this.project)
      return hasPermission(this, projectId, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
    },
    renderCubeTree (h, {node, data, store}) {
      var _this = this
      var addCubeDom = (this.isAdmin || this.hasSomePermissionOfProject()) ? '<span style="width:40px;height:60px; text-align:center;margin-left:30px;font-size:18px;" class="addCube" >+</span>' : ''
      return this.$createElement('div', {
        class: [{'el-tree-node__label': true, 'leaf-label': node.isLeaf && node.level !== 1}, node.icon],
        domProps: {
          innerHTML: node.level === 1 ? '<span>' + data.label + '</span>' + addCubeDom : '<span>' + data.label + '</span>'
        },
        attrs: {
          title: data.label,
          class: node.icon || ''
        },
        on: {
          click: function (event) {
            if (event.target.className === 'addCube') {
              event.stopPropagation()
              _this.addCube(event)
            }
          }
        }
      })
    },
    openModelSubMenu: function (currentMeunStatus, database, tablename) {
      // this.$refs['modelsubmenu'].menuStatus = this.$refs['modelsubmenu'].menuStatus === 'hide' ? 'show' : 'hide'
      this.$refs['modelsubmenu'].$emit('menu-toggle', 'hide')
      this.submenuInfo.menu1 = 'second'
      this.$set(this.currentSelectTable, 'database', database || '')
      this.$set(this.currentSelectTable, 'tablename', tablename || '')
    },
    completeInput: function () {
      var editor = this.$refs.expressionBox.editor
      editor.execCommand('startAutocomplete')
    },
    addComputedColumn: function (guid) {
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
      this.$nextTick(() => {
        var editor = this.$refs.expressionBox.editor
        editor.insert(' ')
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
        editor.setOption('wrap', 'free')
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
    editComputedColumn (row) {
      this.checkExpressResult = {
        isvalid: '',
        msg: ''
      }
      this.openAddComputedColumnForm = true
      var tableInfo = this.getTableInfo('alias', row.tableAlias)
      if (tableInfo) {
        this.computedColumn = {
          guid: tableInfo.guid,
          name: row.columnName,
          expression: row.expression,
          returnType: row.datatype
        }
      }
    },
    delComputedColumn (column) {
      this.changeComputedColumnDisable(column.tableAlias, column.columnName, false)
      var tableInfo = this.getTableInfo('alias', column.tableAlias)
      this.delColumn(tableInfo.guid, 'name', column.columnName)
      // var tableList = this.getSameOriginTables(column.tableIdentity.split('.')[0], column.tableIdentity.split('.')[1])
      // for (var i = 0; i < tableList.length; i++) {
      //   var guid = tableList[i].guid
      //   this.delColumn(guid, 'name', column.columnName)
      // }
      // this.editTableColumnInfo(tableList[0].guid, 'name', column.columnName, 'btype', '-')
      this.refreshComputed()
      this.getPartitionDateColumns()
    },
    pushComputedColumnToMeta () {
      this.addComputedColumnToDatabase((columnName) => {
        this.$message({message: this.$t('addComputedColumnSuccess'), type: 'success'})
        this.$refs.computedColumnForm.resetFields()
        this.openAddComputedColumnForm = false
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
              this.pushComputedColumnToMeta()
            })
          } else {
            this.pushComputedColumnToMeta()
          }
        }
      })
    },
    cancelComputedEditForm: function (argument) {
      var editor = this.$refs.expressionBox.editor
      editor.insert(' ')
      this.openAddComputedColumnForm = false
      this.$refs.computedColumnForm.resetFields()
    },
    getPartitionDateColumns: function () {
      var dateColumns = {}
      var timeColumns = {}
      var tableList = this.getTableList('kind', 'ROOTFACT').concat(this.getTableList('kind', 'FACT'))
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
            dateColumns[tabeFullName].push({name: tableList[i].columns[k].name, isFormat: needFormat, database: tableList[i].database, table: tableList[i].name})
          }
          if (TimePartitionRule.indexOf(datatype) >= 0) {
            timeColumns[tabeFullName] = timeColumns[tabeFullName] || []
            timeColumns[tabeFullName].push({name: tableList[i].columns[k].name, isFormat: true, database: tableList[i].database, table: tableList[i].name})
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
      var databaseInfo = this.getTableInfoByGuid(guid)
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
        databaseInfo.columns.push(columnObj)
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
          this.warnAlert(this.$t('sameNameComputedColumn'))
        }
      }
      this.getPartitionDateColumns()
    },
    checkSameColumnName: function (guid, column) {
      var columns = this.getTableInfoByGuid(guid).columns
      return indexOfObjWithSomeKey(columns, 'name', column) !== -1
    },
    checkSameComputedName (guid, column) {
      var columns = this.getTableInfoByGuid(guid).columns
      for (var s = 0; s < columns.length; s++) {
        if (columns[s].isComputed && columns[s].name === column) {
          return true
        }
      }
    },
    checkSameNormalName (guid, column) {
      var columns = this.getTableInfoByGuid(guid).columns
      for (var s = 0; s < columns.length; s++) {
        if (!columns[s].isComputed && columns[s].name === column) {
          return true
        }
      }
    },
    // dimension and measure and disable
    changeColumnBType: function (id, columnName, columnBType, isComputed) {
      if (this.actionMode === 'view') {
        return
      }
      // var tableInfo = this.getTableInfoByGuid(id)
      // var alias = tableInfo.alias
      // var usedCubes = this.checkColsUsedStatus(alias, columnName)
      // if (usedCubes && usedCubes.length) {
      //   this.warnAlert('已经在名称为' + usedCubes.join(',').replace(/cube\[name=(.*?)\]/gi, '$1') + '的cube中用过该列，不允许修改')
      //   return
      // }
      if (columnBType === 'D' && this.isColumnUsedInConnect(id, columnName)) {
        this.warnAlert(this.$t('changeUsedForConnectColumnTypeWarn'))
        return
      }
      var willSetType = getNextValInArray(this.columnBType, columnBType)
      if (willSetType === '－' && isComputed) {
        willSetType = this.columnBType[0]
      }
      this.editTableColumnInfo(id, 'name', columnName, 'btype', willSetType)
      // var fullName = tableInfo.database + '.' + tableInfo.name
    },
    changeComputedColumnDisable (alias, column) {
      var len = this.modelInfo.computed_columns && this.modelInfo.computed_columns.length || 0
      for (var i = 0; i < len; i++) {
        var calcColumn = this.modelInfo.computed_columns[i]
        if (calcColumn.tableAlias === alias && calcColumn.columnName === column) {
          this.modelInfo.computed_columns.splice(i, 1)
          break
        }
      }
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
      var tableInfo = this.getTableInfoByGuid(id)
      this.$set(this.currentSelectTable, 'database', tableInfo.database || '')
      this.$set(this.currentSelectTable, 'tablename', tableInfo.name || '')
      this.$set(this.currentSelectTable, 'columnname', columnName)
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
    getColumnData: function (database, tablename, column) {
      var result = null
      this.tableList.forEach(function (table) {
        if (table.database === database && table.name === tablename) {
          for (let i = 0; i < table.columns.length; i++) {
            var col = table.columns[i]
            if (col.name === column) {
              result = col
            }
          }
        }
      })
      return result
    },
    autoScroll (instance, topSize, aim, id) {
      instance.scrollTo(100, topSize, 300, function (scrollbar) {})
    },
    linkFilterColumnAnimate (sourceId, targetId, callback) {
      callback()
    },
    /*
    *  Table Func
    *  ==========================================================================
    */
    createTableData: function (project, database, tableName, other) {
      var obj = {}
      var projectDataSource = this.$store.state.datasource.dataSource[project] || []
      for (var i = 0; i < projectDataSource.length; i++) {
        if (projectDataSource[i].database === database && projectDataSource[i].name === tableName) {
          var tableList = this.getTableList('source_type', 0)
          var streamingTableList = this.getTableList('source_type', 1)
          if (tableList.length && projectDataSource[i].source_type === 1 || streamingTableList.length && projectDataSource[i].source_type === 0) {
            this.$message(this.$t('kylinLang.common.streamingConnectHiveError'))
            return
          }
          obj = objectClone(projectDataSource[i])
          obj.guid = sampleGuid()
          obj.pos = {x: 300, y: 400}
          obj.alias = obj.alias || obj.name
          obj.kind = 'LOOKUP'
          obj.sourceType = projectDataSource[i].source_type
          Object.assign(obj, other)
          break
        }
      }
      // this.modelInfo.computed_columns.forEach((k) => {
      //   if (k.tableIdentity === database + '.' + tableName) {
      //     var columnObj = this.getColumnData(database, tableName, k.columnName)
      //     if (columnObj) {
      //       obj.columns.push(columnObj)
      //     }
      //   }
      // })
      this.tableList.push(obj)
      var uniqueName = this.createUniqueName(obj.guid, obj.alias)
      this.$set(obj, 'alias', uniqueName)
      this.$nextTick(() => {
        this.draggleTable(jsPlumb.getSelector('.table_box'))
        Scrollbar.init($('#' + obj.guid).find('section')[0])
      })
      return obj
    },
    removeTable: function (guid) {
      // if (this.checkLock()) {
      //   return
      // }
      var count = this.getConnectsByTableId(guid)
      if (count > 0) {
        this.warnAlert(this.$t('delTableTip'))
        return
      }
      for (var i = 0; i < this.tableList.length; i++) {
        if (this.tableList[i].guid === guid) {
          // var k = this.getSameOriginTables(this.tableList[i].database, this.tableList[i].name)
          // if (k.length === 1) {
          this.modelInfo.computed_columns.forEach((com, i) => {
            if (com.tableAlias === this.tableList[i].alias) {
              this.modelInfo.computed_columns.splice(i, 1)
            }
          })
          // }
          this.tableList.splice(i, 1)
          this.removePoint(guid)
          break
        }
      }
    },
    draggleTable: function (idList) {
      var _this = this
      this.plumbInstance.draggable(idList, {
        handle: '.drag_bar,.drag_bar span',
        drop: function (e) {
        },
        stop: function (e) {
          _this.editTableInfoByGuid(idList, {
            pos: {x: e.pos[0], y: e.pos[1]}
          })
        }
      })
    },
    getTableInfoByGuid: function (guid) {
      return this.getTableInfo('guid', guid)
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
          table: dragName.split('$')[0],
          columnName: dragName.split('$')[1]
        }
      }
    },
    dragColumns (event) {
      if (this.actionMode === 'view') {
        this.$message(this.$t('kylinLang.model.viewModeLimit'))
        return
      }
      // event.preventDefault()
      // event.dataTransfer && event.dataTransfer.setData('Text', '')
      this.currentDragDom = event.srcElement ? event.srcElement : event.target
      event.dataTransfer.effectAllowed = 'move'
      if (!isIE()) {
        event.dataTransfer && event.dataTransfer.setData && event.dataTransfer.setData('column', this.currentDragDom.innerHTML)
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
      // event.preventDefault()
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
        var newTableData = this.createTableData(this.extraoption.project, this.currentDragData.table, this.currentDragData.columnName, {pos: pos})
        this.suggestColumnDtype(newTableData)
      }
      return false
    },
    suggestColumnDtype (newTableData) {
      if (!newTableData) {
        return
      }
      this.suggestDM({'table': newTableData.database + '.' + newTableData.name, project: this.project}).then((response) => {
        handleSuccess(response, (data) => {
          for (var i in data) {
            var setColumnBType
            if (data[i].indexOf('MEASURE') === 0) {
              setColumnBType = 'M'
            } else if (data[i].indexOf('DIMENSION') === 0) {
              setColumnBType = 'D'
            }
            this.editTableColumnInfo(newTableData.guid, 'name', i, 'btype', setColumnBType)
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
    dropColumn: function (event) {
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
      var isComputed = columnBox.attr('data-isComputed')
      var isComputedStart = $(this.currentDragDom).attr('data-isComputed')
      var btypeStart = $(this.currentDragDom).attr('data-btype')
      if (btype !== 'D' || btypeStart !== 'D') {
        this.$message(this.$t('kylinLang.model.dimensionLinkLimit'))
        return
      }
      if (isComputed || isComputedStart) {
        this.$message(this.$t('kylinLang.model.computedLinkLimit'))
        return
      }
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
      var sourceTableInfo = this.getTableInfoByGuid(p1)
      var targetTableInfo = this.getTableInfoByGuid(p2)
      this.currentLinkData.source.guid = sourceTableInfo.guid
      this.currentLinkData.source.database = sourceTableInfo.database
      this.currentLinkData.source.name = sourceTableInfo.name
      this.currentLinkData.source.alias = sourceTableInfo.alias
      this.currentLinkData.source.columns = sourceTableInfo.columns.filter((co) => {
        return co.btype === 'D' && co.isComputed !== true
      })
      this.currentLinkData.target.guid = targetTableInfo.guid
      this.currentLinkData.target.database = targetTableInfo.database
      this.currentLinkData.target.name = targetTableInfo.name
      this.currentLinkData.target.alias = targetTableInfo.alias
      this.currentLinkData.target.columns = targetTableInfo.columns.filter((co) => {
        return co.btype === 'D' && co.isComputed !== true
      })
      this.currentLinkData.joinType = this.getConnectType(p1, p2)
      this.getConnectsByTableIds(p1, p2)
      if (!justShow) {
        this.addJoinCondition(sourceTableInfo.guid, targetTableInfo.guid, this.currentDragData.columnName, this.currentDropData.columnName, this.currentLinkData.joinType, true)
      }
    },
    allowDrop: function (event) {
      event.preventDefault()
    },
    /*
    *  Endpoint Func
    *  ==========================================================================
    */
    endpointConfig: function (type, isShowLinkPoint) {
      var connectorPaintStyle = {
        strokeWidth: 2,
        stroke: '#f5a623',
        joinstyle: 'round'
      }
      if (type !== 'left') {
        connectorPaintStyle.stroke = '#f5a623'
      }
      var sourceEndpoint = {
        endpoint: 'Dot',
        paintStyle: {
          stroke: '#7AB02C',
          fill: 'transparent',
          radius: 7,
          strokeWidth: 1
        },
        isSource: true,
        isTarget: true,
        connector: [ 'Bezier', { curviness: 22 } ], // 设置连线为贝塞尔曲线
          // connector: [ 'Flowchart', { stub: [40, 60], gap: 10, cornerRadius: 5, alwaysRespectStubs: true } ],
        connectorStyle: connectorPaintStyle,
        dragOptions: {}
      }
      if (isShowLinkPoint) {
        sourceEndpoint.paintStyle.radius = 1
      }
      return sourceEndpoint
    },
    createEndpointConfig: function (newEndpointconfig, type, isShowLinkPoint) {
      return Object.assign({}, this.endpointConfig(type, isShowLinkPoint), newEndpointconfig)
    },
    removeAllEndpoints (plumb) {
      plumb.deleteEveryEndpoint()
    },
    addSelectPoints: function (guid, jsplumb, pointType, columnName, columnType, isShowLinkPoint) {
      var anchor = [[1.0, -0.05, 1.5, 0], [0, -0.05, -1, 0]]
      var scope = 'link'
      if (isShowLinkPoint) {
        anchor = [[0.5, 0, 0.6, 0], [0.1, 0, 0.2, 0], [0.8, 0, 0.9, 0], [0.5, 1, 0.6, 1], [0.1, 1, 0.2, 1], [0.8, 1, 0.9, 1], [0, 0.8, 0, 0.9], [1, 0.8, 1, 0.9]]
        scope = 'showlink'
      }
      jsplumb.addEndpoint(guid, {anchor: anchor}, this.createEndpointConfig({
        scope: scope,
        parameters: {
          data: {
            guid: guid,
            column: {
              columnName: columnName,
              columnType: columnType
            }}
        },
        uuid: guid + columnName
      }, pointType, isShowLinkPoint))
      this.draggleTable([guid])
      // this.refreshPlumbObj(jsplumb)
    },
    removePoint: function (uuid) {
      this.plumbInstance.deleteEndpoint(uuid)
    },
    /*
    *  Connect Func
    *  ==========================================================================
    */
    connect: function (p1, p2, jsplumb, otherProper) {
      var _this = this
      var defaultPata = {uuids: [p1, p2],
        deleteEndpointsOnDetach: true,
        editable: true,
        overlays: [['Label', {id: p1 + (p2 + 'label'),
          // location: 0.1,
          events: {
            tap: function () {
              _this.prepareLinkData(p1, p2, true)
            }
          } }]]}
      $.extend(defaultPata, otherProper)
      jsplumb.connect(defaultPata)
    },
    addConnect: function (p1, p2, col1, col2, type, con) {
      var links = this.links
      var hasSame = false
      for (var i = 0; i < links.length; i++) {
        if (links[i][0] === '' + p1 && links[i][1] === '' + p2 && links[i][2] === col1 && links[i][3] === col2) {
          links[i][4] = type
          links[i][5] = con
          hasSame = true
        }
      }
      if (!hasSame) {
        this.links.push([p1, p2, col1, col2, type, con])
      }
    },
    getConnectJoinType: function (p1, p2) {
      var links = this.links
      for (var i = 0; i < links.length; i++) {
        if (links[i][0] === '' + p1 && links[i][1] === '' + p2) {
          return links[i][4]
        }
      }
      return true
    },
    delConnect: function (connect) {
      var links = this.links
      for (var i = 0; i < links.length; i++) {
        if (links[i][0] === connect[0] && links[i][1] === connect[1] && links[i][2] === connect[2] && links[i][3] === connect[3]) {
          this.links.splice(i, 1)
          break
        }
      }
      this.refreshConnectCountText(connect[0], connect[1])
      var linkCount = this.getConnectsByTableId(connect[1])
      if (linkCount === 0) {
        this.dialogVisible = false
      }
    },
    delBrokenConnect (p1, p2) {
      var links = this.links
      for (var i = 0; i < links.length; i++) {
        if (links[i][0] === p1 && links[i][1] === p2 && (links[i][2] === undefined || links[i][2] === '' || links[i][3] === undefined || links[i][3] === '')) {
          this.links.splice(i, 1)
          i = i - 1
        }
      }
      this.refreshConnectCountText(p1, p2)
    },
    checkBrokenConnect (p1, p2) {
      var links = this.links
      for (var i = 0; i < links.length; i++) {
        if (links[i][0] === p1 && links[i][1] === p2 && (links[i][2] === undefined || links[i][2] === '' || links[i][3] === undefined || links[i][3] === '')) {
          return true
        }
      }
      return false
    },
    refreshConnectCountText (p1, p2, joinType) {
      this.getConnectsByTableIds(p1, p2)
      var showLinkCon = this.showLinkCons[p1 + '$' + p2]
      if (showLinkCon) {
        if (this.currentTableLinks.length) {
          this.setConnectLabelText(showLinkCon, p1, p2, '' + this.currentTableLinks.length)
        } else {
          this.plumbInstance.deleteConnection(showLinkCon)
          delete this.showLinkCons[p1 + '$' + p2]
          // this.removePoint(showLinkCon.sourceId)
        }
      } else {
        if (joinType) {
          this.addShowLink(p1, p2, joinType)
        }
      }
    },
    removeAllLinks () {
      for (var i in this.showLinkCons) {
        var conn = this.showLinkCons[i]
        this.plumbInstance.deleteConnection(conn)
      }
    },
    addJoinCondition: function (p1, p2, col1, col2, joinType, newCondition) {
      var link = [p1, p2, col1, col2, joinType, newCondition]
      this.links.push(link)
      this.refreshConnectCountText(p1, p2, joinType)
    },
    saveLinks (p1, p2, delError) {
      var isBroken = this.checkBrokenConnect(p1, p2)
      if (isBroken) {
        this.warnAlert(this.$t('checkCompleteLink'))
        if (delError) {
          this.delBrokenConnect(p1, p2)
        }
        return
      }
      this.dialogVisible = false
      this.delBrokenConnect(p1, p2)
      this.getConnectsByTableIds(p1, p2)
      // var showLinkCon = this.showLinkCons[p1 + '$' + p2]
      // if (showLinkCon) {
      //   this.setConnectLabelText(showLinkCon, p1, p2, '' + this.currentTableLinks.length)
      // }
    },
    switchJointType: function (p1, p2, status) {
      for (var i = 0; i < this.links.length; i++) {
        if (this.links[i][0] === p1 && this.links[i][1] === p2) {
          this.links[i][4] = status
          var connObj = this.showLinkCons[p1 + '$' + p2]
          var labelObj = connObj.getOverlay(p1 + (p2 + 'label'))
          var labelDom = $(labelObj.canvas)
          labelDom.removeClass('label_left').removeClass('label_inner')
          if (status === 'left') {
            labelDom.addClass('label_left')
            labelObj.setLabel(labelObj.labelText.replace(/Inner/, 'Left'))
          } else {
            labelDom.addClass('label_inner')
            labelObj.setLabel(labelObj.labelText.replace(/Left/, 'Inner'))
          }
          connObj.setPaintStyle({'stroke': status ? '#f5a623' : '#f5a623'})
        }
      }
    },
    setConnectLabelText: function (conn, p1, p2, text) {
      var labelObj = conn.getOverlay(p1 + (p2 + 'label'))
      this.showLinkCons[p1 + '$' + p2] = conn
      var joinType = this.getConnectJoinType(p1, p2)
      if (joinType === 'left') {
        $(labelObj.canvas).addClass('label_left')
        // labelObj.setLabel('Left: ' + text)
        labelObj.setLabel('Left')
      } else {
        $(labelObj.canvas).addClass('label_inner')
        // labelObj.setLabel('Inner: ' + text)
        labelObj.setLabel('Inner')
      }
    },
    getConnectCountByTableIds: function (p1, p2) {
      var count = 0
      for (var i = 0; i < this.links.length; i++) {
        if (this.links[i][0] === '' + p1 && this.links[i][1] === '' + p2) {
          count = count + 1
        }
      }
      return count
    },
    getConnectsByTableIds: function (p1, p2) {
      this.currentTableLinks = []
      for (var i = 0; i < this.links.length; i++) {
        if (this.links[i][0] === p1 && this.links[i][1] === p2) {
          this.currentTableLinks.push(this.links[i])
        }
      }
      return this.currentTableLinks
    },
    getConnectType: function (p1, p2) {
      var joinType = 'inner'
      for (var i = 0; i < this.links.length; i++) {
        if (this.links[i][0] === p1 && this.links[i][1] === p2) {
          joinType = this.links[i][4]
          break
        }
      }
      return joinType
    },
    getConnectsByTableId: function (guid) {
      var count = 0
      for (var i = 0; i < this.links.length; i++) {
        if (this.links[i][0] === guid || this.links[i][1] === guid) {
          count = count + 1
        }
      }
      return count
    },
    isColumnUsedInConnect: function (guid, columnName) {
      var linkLen = this.links && this.links.length || 0
      for (var i = 0; i < linkLen; i++) {
        if (this.links[i][0] === guid && this.links[i][2] === columnName || this.links[i][1] === guid && this.links[i][3] === columnName) {
          return true
        }
      }
      return false
    },
    addShowLink: function (p1, p2, joinType) {
      this.addSelectPoints(p1, this.plumbInstance, joinType, '', '', true)
      this.addSelectPoints(p2, this.plumbInstance, joinType, '', '', true)
      this.connect(p1, p2, this.plumbInstance, {})
      this.refreshPlumbObj()
    },
    getConnectsCountByGuid (p1, p2) {
      var obj = {}
      var count = 0
      for (var i = 0; i < this.links.length; i++) {
        if (this.links[i][0] !== p1 && this.links[i][1] === p2) {
          obj[this.links[i][1]] = true
        }
      }
      for (var k in obj) {
        if (k) {
          count++
        }
      }
      return count
    },
    // 检查不合理的链接
    checkIncorrectLink: function (p1, p2) {
      // 检查是否是rootfact指向fact和lookup
      var resultTag = false
      if (this.checkIsRootFact(p2)) {
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
      if (this.getConnectsCountByGuid(p1, p2) >= 1) {
        this.warnAlert(this.$t('tableHasOtherFKTable'))
        resultTag = true
      }
      return resultTag
    },
    editAlias: function (guid) {
      if (this.actionMode === 'view') {
        return
      }
      var rootFact = this.getRootFact()
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
      var tableInfo = this.getTableInfo('guid', guid)
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
      var tableInfo = this.getTableInfoByGuid(guid)
      if (kind === 'ROOTFACT') {
        if (this.checkHasFactKindTable(guid)) {
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
        this.editTableInfoByGuid(guid, 'alias', tableInfo.name)
        if (tableInfoOldAlias.toUpperCase() !== tableInfo.name.toUpperCase()) {
          var otherHasSameNameTableGuid = this.filterSameAliasTb(guid, tableInfo.name)
          if (otherHasSameNameTableGuid) {
            var otherHasSameNameTableInfo = this.getTableInfo('guid', otherHasSameNameTableGuid)
            this.$set(otherHasSameNameTableInfo, 'alias', tableInfoOldAlias)
          }
        }
      }
      this.editTableInfoByGuid(guid, 'kind', kind)
      this.currentSelectTable = {
        database: tableInfo.database,
        tablename: tableInfo.tableName,
        columnname: ''
      }
      this.getPartitionDateColumns()
    },
    checkHasFactKindTable: function (guid) {
      var hasFact = false
      this.tableList.forEach(function (table) {
        if (table.guid !== guid) {
          if (table.kind === 'ROOTFACT') {
            hasFact = true
          }
        }
      })
      return hasFact
    },
    getRootFact: function () {
      return this.getTableList('kind', 'ROOTFACT')
    },
    checkIsRootFact: function (guid) {
      for (var i = 0; i < this.tableList.length; i++) {
        if (this.tableList[i].guid === guid) {
          if (this.tableList[i].kind === 'ROOTFACT') {
            return true
          }
        }
      }
      return false
    },
    refreshPlumbObj: function (plumb) {
      plumb = plumb || this.plumbInstance
      plumb.repaintEverything()
    },
    getTableInfo: function (filterKey, filterValue) {
      for (var i = 0; i < this.tableList.length; i++) {
        if (this.tableList[i][filterKey] === filterValue) {
          return this.tableList[i]
        }
      }
    },
    getDimensions: function (ignoreComputed) {
      var resultArr = []
      for (var i = 0; i < this.tableList.length; i++) {
        if (!this.getConnectsByTableId(this.tableList[i].guid) && this.tableList[i].kind !== 'ROOTFACT' && this.tableList[i].kind !== 'FACT') {
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
        if (!this.getConnectsByTableId(this.tableList[i].guid) && this.tableList[i].kind !== 'ROOTFACT' && this.tableList[i].kind !== 'FACT') {
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
    getTableList: function (filterKey, filterValue) {
      return filterObjectArray(this.tableList, filterKey, filterValue)
    },
    getSameOriginTables: function (database, tableName) {
      var resultArr = this.tableList.filter((table) => {
        return table.database === database && table.name === tableName
      })
      return resultArr || []
    },
    editTableInfoByGuid: function (guid, key, value) {
      for (var i = 0; i < this.tableList.length; i++) {
        if (this.tableList[i].guid === guid) {
          this.tableList[i][key] = value
          break
        }
      }
    },
    editTableColumnInfo: function (guid, filterColumnKey, filterColumnVal, columnKey, value) {
      var _this = this
      this.tableList.forEach(function (table) {
        if (table.guid === guid) {
          var len = table.columns && table.columns.length || 0
          for (let i = 0; i < len; i++) {
            var col = table.columns[i]
            if (col[filterColumnKey] === filterColumnVal || filterColumnVal === '*') {
              _this.$set(col, columnKey, value)
            }
          }
        }
      })
    },
    editTableComputedColumnInfo: function (guid, column, key, value) {
      var tableInfo = this.getTableInfoByGuid(guid)
      var tableList = this.getSameOriginTables(tableInfo.database, tableInfo.name)
      tableList.forEach((table) => {
        var curGuid = table.guid
        this.editTableColumnInfo(curGuid, 'name', column, key, value)
      })
    },
    delColumn: function (guid, filterColumnKey, filterColumnVal) {
      this.tableList.forEach(function (table) {
        if (table.guid === guid) {
          var len = table.columns && table.columns.length || 0
          for (let i = 0; i < len; i++) {
            var col = table.columns[i]
            if (col[filterColumnKey] === filterColumnVal) {
              table.columns.splice(i, 1)
              break
            }
          }
        }
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
    warnAlert: function (str) {
      this.$message(str)
    },
    // trans Data
    DragDataToServerData: function (needJson, ignoreComputed) {
      var fainalComputed = this.getDisableComputedColumn()
      var pos = {}
      var kylinData = {
        uuid: this.modelInfo.uuid,
        capacity: 'MEDIUM',
        owner: 'ADMIN',
        lookups: [],
        partition_desc: {},
        dimensions: [],
        metrics: [],
        is_draft: this.modelInfo.is_draft,
        last_modified: this.modelInfo.last_modified,
        filter_condition: this.modelInfo.filterStr,
        name: this.modelInfo.modelName,
        description: this.modelInfo.modelDiscribe,
        computed_columns: fainalComputed
      }
      if (this.partitionSelect.date_table && this.partitionSelect.date_column) {
        kylinData.partition_desc.partition_date_column = this.partitionSelect.date_table + '.' + this.partitionSelect.date_column
      }
      if (this.partitionSelect.time_table && this.partitionSelect.time_column) {
        kylinData.partition_desc.partition_time_column = this.partitionSelect.time_table + '.' + this.partitionSelect.time_column
      }
      kylinData.partition_desc.partition_date_format = this.partitionSelect.partition_date_format
      kylinData.partition_desc.partition_time_format = this.partitionSelect.partition_time_format
      kylinData.partition_desc.partition_type = 'APPEND'
      kylinData.partition_desc.partition_date_start = null

      // root table
      var rootFactTable = this.getRootFact()
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
        var p1TableInfo = this.getTableInfoByGuid(p1)
        var p2TableInfo = this.getTableInfoByGuid(p2)
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
    loadEditData (modelData) {
      this.editLock = !!(this.modelData && this.modelData.uuid)
      if (!modelData.fact_table) {
        return
      }
      Object.assign(this.modelInfo, {
        uuid: modelData.uuid,
        modelDiscribe: modelData.description,
        modelName: modelData.name,
        is_draft: modelData.is_draft,
        last_modified: modelData.last_modified,
        owner: modelData.owner,
        project: modelData.project
      })
      // 加载原来设置的partition
      var partitionDate = modelData.partition_desc.partition_date_column ? modelData.partition_desc.partition_date_column.split('.') : [null, null]
      var partitionTime = modelData.partition_desc.partition_time_column ? modelData.partition_desc.partition_time_column.split('.') : [null, null]
      Object.assign(this.partitionSelect, {
        'date_table': partitionDate[0],
        'date_column': partitionDate[1],
        'time_table': partitionTime[0],
        'time_column': partitionTime[1],
        'partition_date_column': modelData.partition_desc.partition_date_column,
        'partition_time_column': modelData.partition_desc.partition_time_column,
        'partition_date_start': 0,
        'partition_date_format': modelData.partition_desc.partition_date_format,
        'partition_time_format': modelData.partition_desc.partition_time_format,
        'partition_type': 'APPEND'
      })
      // 加载原来设置的partition
      this.modelInfo.filterStr = modelData.filter_condition
      this.modelInfo.computed_columns = modelData.computed_columns || []
      var lookups = modelData.lookups
      var baseTables = {}
      for (var i = 0; i < lookups.length; i++) {
        baseTables[lookups[i].alias] = {
          database: lookups[i].table.split('.')[0],
          table: lookups[i].table.split('.')[1],
          kind: lookups[i].kind,
          guid: sampleGuid(),
          alias: lookups[i].alias
        }
      }
      baseTables[modelData.fact_table.split('.')[1]] = {
        database: modelData.fact_table.split('.')[0],
        table: modelData.fact_table.split('.')[1],
        kind: 'ROOTFACT',
        guid: sampleGuid(),
        alias: modelData.fact_table.split('.')[1]
      }
      if (modelData.fact_table) {
        this.currentSelectTable = {
          database: modelData.fact_table.split('.')[0],
          tablename: modelData.fact_table.split('.')[1],
          columnname: ''
        }
      }
      for (var table in baseTables) {
        this.createTableData(this.extraoption.project, baseTables[table].database, baseTables[table].table, {
          pos: modelData.pos && modelData.pos[modelData.fact_table.split('.')[1]] || {x: 20000, y: 20000},
          kind: baseTables[table].kind,
          guid: baseTables[table].guid,
          alias: baseTables[table].alias || baseTables[table].name
        })
      }
      var baseLinks = []
      for (let i = 0; i < lookups.length; i++) {
        for (let j = 0; j < lookups[i].join.primary_key.length; j++) {
          let priFullColumn = lookups[i].join.primary_key[j]
          let foriFullColumn = lookups[i].join.foreign_key[j]
          let priAlias = priFullColumn.split('.')[0]
          let foriAlias = foriFullColumn.split('.')[0]
          let pcolumn = priFullColumn.split('.')[1]
          let fcolumn = foriFullColumn.split('.')[1]
          let bArr = [baseTables[foriAlias].guid, baseTables[priAlias].guid, fcolumn, pcolumn, lookups[i].join.type]
          baseLinks.push(bArr)
        }
      }
      this.links = baseLinks
      this.$nextTick(() => {
        Scrollbar.initAll()
        for (let i = 0; i < lookups.length; i++) {
          let priAlias = lookups[i].alias
          let foriFullColumn = lookups[i].join.foreign_key[0]
          var pGuid = baseTables[priAlias].guid
          var fGuid = baseTables[foriFullColumn.split('.')[0]].guid
          var hisConn = this.showLinkCons[pGuid + '$' + fGuid]
          if (!hisConn) {
            var joinType = lookups[i].join.type
            this.addShowLink(fGuid, pGuid, joinType)
          }
        }
        if (!modelData.pos) {
          this.autoLayerPosition()
        }
      })
      var computedColumnsLen = modelData.computed_columns && modelData.computed_columns.length || 0
      // computed column add  new method to init computed column
      for (let i = 0; i < computedColumnsLen; i++) {
        var alias = modelData.computed_columns[i].tableAlias
        var tableInfo = this.getTableInfo('alias', alias)
        if (tableInfo) {
          this.computedColumn = {
            guid: tableInfo.guid,
            name: modelData.computed_columns[i].columnName,
            expression: modelData.computed_columns[i].expression,
            returnType: modelData.computed_columns[i].datatype
            // columnType: modelData.computed_columns[i].datatype
          }
          this.addComputedColumnToDatabase(() => {}, true)
        }
      }
      var modelDimensionsLen = modelData.dimensions && modelData.dimensions.length || 0
      for (let i = 0; i < modelDimensionsLen; i++) {
        var currentD = modelData.dimensions[i]
        for (let j = 0; j < currentD.columns.length; j++) {
          this.editTableColumnInfo(baseTables[currentD.table].guid, 'name', currentD.columns[j], 'btype', 'D')
        }
      }
      var modelMetricsLen = modelData.metrics && modelData.metrics.length || 0
      for (let i = 0; i < modelMetricsLen; i++) {
        var currentM = modelData.metrics[i]
        this.editTableColumnInfo(baseTables[currentM.split('.')[0]].guid, 'name', currentM.split('.')[1], 'btype', 'M')
      }
      // partition time setting
      this.getPartitionDateColumns()
      this.firstRenderServerData = true
      this.modelDataLoadEnd = true
      this.$nextTick(() => {
        this.resizeWindow(this.briefMenu)
      })
    },
    serverDataToDragData: function () {
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
      var actionModelName = this.extraoption.modelName
      this.getModelByModelName({modelName: actionModelName, project: this.extraoption.project}).then((response) => {
        handleSuccess(response, (data) => {
          this.modelData = data.model
          this.draftData = data.draft
          var modelData = this.extraoption.status ? data.draft : data.model
          if (this.modelData && this.draftData && this.extraoption.mode !== 'view') {
            kapConfirm(this.$t('kylinLang.common.checkDraft'), {
              confirmButtonText: this.$t('kylinLang.common.ok'),
              cancelButtonText: this.$t('kylinLang.common.cancel')
            }).then(() => {
              modelData = data.draft
              this.loadEditData(modelData)
            }).catch(() => {
              modelData = data.model
              this.loadEditData(modelData)
            })
          } else {
            this.loadEditData(modelData || {})
          }
        })
      }, (res) => {
        handleError(res)
      })
    },
    // checkColsUsedStatus (alias, columnName) {
    //   if (this.columnUsedInfo) {
    //     for (var i in this.columnUsedInfo) {
    //       if (alias + '.' + columnName === i) {
    //         return this.columnUsedInfo[i]
    //       }
    //     }
    //   }
    //   return null
    // },
    // checkTableUsedStatus (alias) {
    //   if (this.columnUsedInfo) {
    //     for (var i in this.columnUsedInfo) {
    //       if (alias === i.split('.')[0]) {
    //         return true
    //       }
    //     }
    //   }
    //   return false
    // },
    autoCalcLayer: function (root, result, deep) {
      var rootGuid = root || this.getRootFact().length && this.getRootFact()[0].guid
      var index = ++deep || 1
      var resultArr = result || [[rootGuid]]
      var tempArr = []
      for (var i in this.showLinkCons) {
        if (rootGuid === i.split('$')[0]) {
          resultArr[index] = resultArr[index] || []
          resultArr[index].push(i.split('$')[1])
          tempArr.push(i.split('$')[1])
        }
      }
      for (var s = 0; s < tempArr.length; s++) {
        this.autoCalcLayer(tempArr[s], resultArr, index)
      }
      return resultArr
    },
    autoLayerPosition: function (rePosition) {
      var layers = this.autoCalcLayer()
      var baseL = this.baseLineL + 400
      var baseT = this.baseLineT + 100
      var boxW = 220
      var boxH = 420
      var boxML = 100
      var boxMT = 150
      for (let k = 0; k < layers.length; k++) {
        for (let m = 0; m < layers[k].length; m++) {
          var currentT = baseT + (boxH + boxMT) * k
          var currentL = baseL + (boxW + boxML) * m
          if (k === 0) {
            currentL = baseT + $(window).width() / 2 - 90
          }
          this.editTableInfoByGuid(layers[k][m], 'pos', {
            x: currentL,
            y: currentT
          })
          if (rePosition) {
            $('#' + layers[k][m]).css({
              left: currentL + 'px',
              top: currentT + 'px'
            })
          }
          this.$nextTick(() => {
            this.refreshPlumbObj()
          })
        }
      }
      this.refreshPlumbObj()
      // var minHeightPer = (layers.length * (boxH + boxMT) - this.dockerScreen.y) / this.baseLineT
      // console.log(minHeightPer, 99)
      // this.jsplumbZoom(minHeightPer, this.plumbInstance)
    },
    autoLayerPosition_2: function () {
    },
    // saveData: function () {
    //   this.DragDataToServerData()
    // },
    refreshComputed () {
      var guid = this.computedColumn.guid
      if (!guid) {
        return
      }
      // this.getCurrentTableComputedColumns()
      this.currentTableComputedColumns.splice(0, this.currentTableComputedColumns.length)
      var tableInfo = this.getTableInfoByGuid(guid)
      this.modelInfo.computed_columns.filter((computedColumn) => {
        if (computedColumn.tableAlias === tableInfo.alias && computedColumn.disabled !== false) {
          this.currentTableComputedColumns.push(computedColumn)
        }
      })
    },
    jsplumbZoom: function (zoom, instance, transformOrigin, el) {
      transformOrigin = transformOrigin || [0.5 + 460 / 40000, 0.5 + 180 / 40000]
      instance = instance || jsPlumb
      el = el || instance.getContainer()
      var p = [ 'webkit', 'moz', 'ms', 'o' ]
      var s = 'scale(' + zoom + ')'
      var oString = (transformOrigin[0] * 100) + '% ' + (transformOrigin[1] * 100) + '%'
      for (var i = 0; i < p.length; i++) {
        el.style[p[i] + 'Transform'] = s
        el.style[p[i] + 'TransformOrigin'] = oString
      }
      el.style['transform'] = s
      el.style['transformOrigin'] = oString
      instance.setZoom(zoom)
    },
    addZoom: function () {
      this.currentZoom += 0.03
      this.jsplumbZoom(this.currentZoom, this.plumbInstance)
    },
    subZoom: function () {
      if (this.currentZoom <= 0.03) {
        return
      }
      this.currentZoom -= 0.03
      this.jsplumbZoom(this.currentZoom, this.plumbInstance)
    },
    // 检测数据有没有发生变化
    checkModelMetaHasChange () {
      var jsonData = this.DragDataToServerData(true)
      delete jsonData.pos
      delete jsonData.uuid
      delete jsonData.last_modified
      delete jsonData.is_draft
      jsonData = JSON.stringify(jsonData)
      if (jsonData !== this.hisModelJsonStr) {
        this.hisModelJsonStr = jsonData
        if (this.firstRenderServerData) {
          this.firstRenderServerData = false
          return false
        }
        return true
      }
      return false
    },
    // 定时保存
    timerSave: function () {
      this.timerST = setTimeout(() => {
        if (this.saveConfig.timer > this.saveConfig.limitTimer && !this.draftBtnLoading) {
          this.saveDraft()
          this.saveConfig.timer = 0
        } else {
          this.saveConfig.timer++
        }
        this.timerSave()
      }, 1000)
    },
    reloadCubeTree () {
      this.actionMode = this.extraoption.mode
      this.getCubesList({pageSize: 100000, pageOffset: 0, projectName: this.extraoption.project, modelName: this.extraoption.modelName}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.cubesList = data.cubes || []
          data.cubes.forEach((cube) => {
            this.cubeDataTree[0].children.push({
              id: cube.uuid,
              label: cube.name
            })
          })
        })
      })
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
          this.dockerScreen.w = wWidth - 100
          this.dockerScreen.h = wHeight - 176
          modelEditTool.addClass('smallScreen')
        } else {
          modelEditTool.removeClass('smallScreen')
          this.dockerScreen.w = wWidth - 200
          this.dockerScreen.h = wHeight - 176
        }
      }
    },
    clickCube: function (leaf) {
      if (leaf.children) {
        return
      }
      this.$emit('addtabs', 'viewcube', '[view] ' + event.target.textContent, 'cubeView', {
        project: this.project,
        cubeName: event.target.textContent,
        modelName: this.modelInfo.modelName,
        isEdit: false
      })
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
    this.timerSave()
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
    jsPlumb.ready(() => {
      this.plumbInstance = jsPlumb.getInstance({
        DragOptions: { cursor: 'pointer', zIndex: 2000 },
        HoverPaintStyle: { stroke: '#fdd9a4' },
        ConnectionOverlays: [
          [ 'Arrow', {
            location: 1,
            visible: true,
            width: 11,
            length: 11,
            id: 'ARROW',
            events: {
              click: function () {}
            }
          } ]],
        Container: $(this.$el).find('.model_edit')
      })
      this.jsplumbZoom(this.currentZoom, this.plumbInstance)
      /* eslint-disable no-new */
      new Draggable($(this.$el).find('.model_edit')[0], {
        onDrag: (s) => {
        },
        onDragEnd: (s, x, y) => {
          this.docker.x = x
          this.docker.y = y
        },
        filterTarget: (dom) => {
          if (dom) {
            if (!dom.className || typeof dom.className !== 'string') {
              return false
            }
            return dom.tagName !== 'INPUT' && dom.className !== 'tool_box' && dom.tagName !== 'SECTION' && dom.className !== 'toolbtn' && dom.className.indexOf('column_li') < 0 && dom.className.indexOf('column') < 0 && dom.className.indexOf('el-tooltip') < 0
          }
        },
        useGPU: true
      })
      this.plumbInstance.bind('connection', (info, originalEvent) => {
        this.setConnectLabelText(info.connection, info.connection.sourceId, info.connection.targetId, this.getConnectCountByTableIds(info.connection.sourceId, info.connection.targetId))
      })
    })
    jsPlumb.fire('jsPlumbDemoLoaded', this.plumbInstance)
  },
  computed: {
    briefMenu () {
      return this.$store.state.config.layoutConfig.briefMenu
    },
    // usedInCube () {
    //   for (var i in this.columnUsedInfo) {
    //     return true
    //   }
    //   return false
    // },
    useLimitFact () {
      return this.$store.state.system.limitlookup === 'false'
    },
    computedDataTypeSelects () {
      var result = []
      for (var i in this.$store.state.datasource.encodingMatchs) {
        result.push(i)
      }
      return result
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
    }
  },
  created () {
    this.reloadCubeTree()
    // 初始化project下的datasouce信息供tablestatice使用
    // var project = this.extraoption.project || localStorage.getItem('selected_project')
    // if (!this.$store.state.datasource.dataSource[project]) {
    //   this.loadDataSource(project)
    // }
    // this.$store.state.model.modelEditCache[this.project + '$' + this.modelGuid] = {}
    // this.getUsedCols(this.extraoption.modelName).then((res) => {
    //   handleSuccess(res, (data) => {
    //     this.columnUsedInfo = data
    //   })
    // })
    // this.actionMode = this.extraoption.mode
    // this.getCubesList({pageSize: 100000, pageOffset: 0, projectName: this.extraoption.project, modelName: this.extraoption.modelName}).then((res) => {
    //   handleSuccess(res, (data, code, status, msg) => {
    //     this.cubesList = data.cubes
    //     data.cubes.forEach((cube) => {
    //       this.cubeDataTree[0].children.push({
    //         id: cube.uuid,
    //         label: cube.name
    //       })
    //     })
    //   })
    // })
  },
  destroyed () {
    clearTimeout(this.timerST)
    this.isFullScreen = false
    $('#fullBox').removeClass('fullLayoutForModelEdit')
    this.resizeWindow(this.briefMenu)
  },
  locales: {
    'en': {'addJoinCondition': 'Add join condition', 'hasRootFact': 'There is already a fact table.', 'cannotSetFact': 'This table has a primary key already, so it cannot be a fact table. Please remove or redefine the join condition.', 'cannotSetFTableToFKTable': 'In model, table join condition should start from fact table(foreign key), then pointing it to the lookup table(primary key).', 'tableHasOppositeLinks': 'There is an reverse link between tables.', 'tableHasOtherFKTable': 'There is already a foreign key within this table.', 'delTableTip': 'you should delete the links of other tables before delete this table', 'sameNameComputedColumn': 'There is already a column with the same name', 'addComputedColumnSuccess': 'Computed column added successfuly!', 'checkCompleteLink': 'Connect info is incomplete', hasNoFact: 'Fact Table is mandatory for model', 'checkDraft': 'Detected the unsaved content, are you going to continue the last edit?', filterPlaceHolder: 'Please input filter condition', filterCondition: 'Filter Condition', 'conditionExpress': 'Note that select one column should contain its table name(or alias table name).', changeUsedForConnectColumnTypeWarn: 'Table join key should be a dimension. Exchanging the column(join key) type from dimension to measure is not feasible.', needOneDimension: 'You must select at least one dimension column', needOneMeasure: 'You must select at least one measure column', 'longTimeTip': 'Expression check may take several seconds.', checkingTip: 'The expression check is about to complete, are you sure to break it and save?', checkSuccess: 'Great, the expression is valid.', continueCheck: 'Cancle', continueSave: 'Save', plsCheckReturnType: 'Please select a data type first!', 'autoModelTip1': '1. This function will help you generate a complete model according to entered SQL statements.', 'autoModelTip2': '2. Multiple SQL statements will be separated by ";".', 'autoModelTip3': '3. Please click "x" to check detailed error message after SQL checking.', sqlPatterns: 'SQL Patterns', validFail: 'Uh oh, some SQL went wrong. Click the failed SQL to learn why it didn\'t work and how to refine it.', validSuccess: 'Great! All SQL can perfectly work on this model.', ignoreErrorSqls: 'Ignore Error SQL(s)', ignoreTip: 'Ignored error SQL will have no impact on auto-modeling.', submitSqlTip: 'KAP is generating a new model and it will overwrite current content, are you sure to continue? '},
    'zh-cn': {'addJoinCondition': '添加连接条件', 'hasRootFact': '已经有一个事实表了。', 'cannotSetFact': '本表包含一个主键，因而无法被设置为事实表。请删除或重新定义该连接（join）。', 'cannotSetFTableToFKTable': '模型中，连接（join）条件的建立是从事实表（外键）开始，指向维度表（主键）。', 'tableHasOppositeLinks': '两表之间已经存在一个反向的连接了。', 'tableHasOtherFKTable': '该表已经有一个关联的外键表。', 'delTableTip': '请先删除掉该表和其他表的关联关系。', 'sameNameComputedColumn': '已经有一个同名的列', 'addComputedColumnSuccess': '计算列添加成功！', 'checkCompleteLink': '连接信息不完整', hasNoFact: '模型需要有一个事实表', 'checkDraft': '检测到上次有未保存的内容，是否继续上次进行编辑?', filterPlaceHolder: '请输入过滤条件', filterCondition: '过滤条件', 'conditionExpress': '请注意，表达式中选用某列时，格式为“表名.列名”。', changeUsedForConnectColumnTypeWarn: '表连接关系中的键只能是维度列，请勿在建立连接关系后更改该列类型。', needOneDimension: '至少选择一个维度列', needOneMeasure: '至少选择一个度量列', 'longTimeTip': '表达式校验需要进行十几秒，请稍候。', checkingTip: '表达式校验即将完成，您确定要现在保存？', checkSuccess: '表达式校验结果正确。', continueCheck: '继续校验', continueSave: '直接保存', plsCheckReturnType: '请先选择数据类型！', autoModelTip1: '1. 本功能将根据您输入的SQL语句自动补全建模，提交SQL即生成新模型。', autoModelTip2: '2. 输入多条SQL语句时将以“;”作为分隔。', autoModelTip3: '3. 语法检验后，点击“x”可以查看每条SQL语句的错误信息。', sqlPatterns: 'SQL', validFail: '有无法运行的SQL查询。请点击未验证成功的SQL，获得具体原因与修改建议。', validSuccess: '所有SQL都能被本模型验证。', ignoreErrorSqls: '忽略错误SQL', ignoreTip: '忽略错误的SQL将不会对后续建模产生影响。', submitSqlTip: '即将生成新模型，新模型将覆盖原有模型，您确认要继续吗？'}
  }
}
</script>
<style lang="less">
   [data-scrollbar] .scrollbar-track-y, [scrollbar] .scrollbar-track-y, scrollbar .scrollbar-track-y{
     right: 4px;
   }
   .linksTable{
     &.el-table::after {
       background-color:#fff;
     }
   }
   .save_model_box{
      .el-form-item__label{
        float:left!important;
      }
    }
    .fullLayoutForModelEdit{
      .left_menu{
        display: none;
      }
      #scrollBox{
        left:0;
        top:0;
      }
      .model_edit_tool{
        left: 0;
      }
      .topbar{
        display: none;
      }
      .model_edit_tool.smallScreen{
        left: 0;
      }
      #modeltab>.el-tabs--card>.el-tabs__header{
        display: none;
      }
      #breadcrumb_box{
        display: none;
      }
      .btn_group{
        bottom:46px;
      }
    }
   .model_edit_box {
    .checkresult {
       color: red;
       font-size: 12px;
       word-break: break-all;
       line-height: 20px;
       &.isvalid {
         color:green;
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
    .el-form-item__label {
      float: none;
    }
    // margin-top: 20px;
    position: relative;
    background-color: #20222e;
    // background-image: url('../../assets/img/jsplumb.png');
    // background-repeat:repeat;
    overflow: hidden;
    .tree_list {
      height: 100%;
      background-color: #393e52;
      position: absolute;
      z-index: 2000;
      overflow-y: auto;
      overflow-x: hidden;
      padding-top: 20px;
      .el-input{
        background-color: #393e52;
      }
    }
   }
   .sample_info{
    position: absolute;
    right: 6px;
    z-index: 2000;
    top:10px;
    right: 10px;
    li{
      display: inline-block;
      font-size: 12px;
      margin-right: 4px;
      .iconD{
        color:#20a0ff;
      }
      .iconM{
        color:#48b5cd;
      }
      .iconDis{
        color:#20a0ff;
      }
      .info{
        color: #d0d0d0;
      }
    }
   }
   .model_tool{
     position: absolute;
     right: 6px;
     z-index: 2000;
     top:40px;
     li{
      -webkit-select:none;
      user-select:none;
      -moz-user-select:none;
      width: 36px;
      height: 36px;
      line-height: 36px;
      position: relative;
      box-shadow: 1px 2px 1px rgba(0,0,0,.15);
      cursor: pointer;
      overflow: hidden;
      background-color: #393e52;
      &:hover{
        background-color: #4e5574;
      }
      span{
        display: block;
        width: 10px;
        height: 10px;
        position: absolute;
        top: 8px;
        left: 8px;
        background-size: 40px 10px;
      }
     }
     .tool_add{
      span{
        background-image: url('../../assets/img/outin.png');
        background-position: 0 0;
      }

     }
     .tool_jian{
       span {
        background-image: url('../../assets/img/outin.png');
        background-position: -10px 0
       }

     }
   }
   .toolbtn{
      list-style: none;
      width: 36px;
      height: 36px;
      background-color: #6d798a;
      color: #fff;
      line-height: 36px;
      text-align: center;
      margin-top: 10px;
      font-weight: bold;
      cursor: pointer;
      font-size: 20px;
   }
   .btn_group{
    position: absolute;
    bottom: 36px;
    right: 10px;
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
     .icon_modelinfo{
       display: inline-block;
       width: 12px;
       height: 14px;
       background-image: url('../../assets/img/tableStats.png');
     }
     .icon_computed{
       display: inline-block;
       width: 12px;
       height: 14px;
       background-image: url('../../assets/img/computed.png');
     }
     .icon_sorted{
       display: inline-block;
       width: 12px;
       height: 14px;
       background-image: url('../../assets/img/order.png');
     }
   }
   .links_dialog{
    .el-dialog__body {
      padding-top: 10px;
    }
    .el-table__row{
      background-color: #393e53;
    }
    .el-input.is-disabled .el-input__inner {
      background-color: #20222e;
    }
    .linksTable.el-table::after {
      background-color: #393e53;
    }
    .el-dialog__body .el-input {
      // background-color: #292b38;
      padding: 0;
    }
     p{
       color:green;
     }
     .el-table {
      background:none;
      tr{
        background-color: #393e53;
        height: 54px;
      }
      border-top:solid 1px #ccc;
       td {
         border:none;
       }
       table {
         border:none;
       }
     }
   }
   .table_box{
       .is_computed{
         color:#f5a623;
       }
       .close_table{
        position: absolute;
        top: 10px;
        right: 6px;
        color:#fff;
        font-size: 12px;
        cursor: pointer;
       }
       .column_in {
        background-color: #000;
       }
       &.rootfact{
          .table_name{
            background-color: #2b91e7;
            &:hover{
              background-color: #49adf8;
            }
          }
       }
       &.fact{
        .table_name{
         background-color: #52b9dc;
        }
       }
       &.lookup{

       }
       .el-input__inner{
         background-color:#5a6a80;
         color:#fff;
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
          background-color:#5a6a80;
       }
       width: 220px;
       left: 440px;
       z-index:1;
       background-color: #2f3242;
       position: absolute;
       height: 420px;
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
       .tool_box{
          position: absolute;
          // padding:8px 6px;
          top:36px;
          height: 30px;
          line-height: 30px;
          left: 0;
          right: 0;
          font-size: 12px;
          color:#fff;
          cursor: pointer;
          background-color: #4f5572;
          span{
            &:hover{
              background-color: #3ad6e8;
            }
          }
          .el-dropdown{
            width: 28px;
            text-align: center;
            &:hover{
              background-color: #3ad6e8;
            }
          }
          >span{
            float: left;
            margin-right: 4px;
            display: block;
            height: 26px;
            margin-top: 0px;
            padding-top: 4px;
            font-weight: blod;
            // line-height: 30px;
            width: 28px;
            text-align:center;
          }
          i{
            cursor: pointer;
          }
          .fa_icon{
            cursor: pointer;
            vertical-align: center;
          }
       }
       .more_tool{
          text-align: center;
          i{
            color: #fff;
            font-weight:lighter;
          }
       }
       .filter_box{
        padding: 10px;
        padding-bottom: 10px;
        margin-top: 30px;
        // padding-top: 12px;
       }
       .table_name{
         height: 36px;
         line-height: 28px;
         background-color: #8492a6;
         padding-left:10px;
         color:#fff;
         &:hover{
          background-color: #a1b1c3;
         }
       }
       .drag_bar{
        span{
          cursor: text;
        }
       }
       .columns_box{
        height: 280px;
        overflow: hidden;
        cursor: default;
       }
       ul{
        // position: absolute;
        margin-top: 10px;
        li{
          list-style: none;
          font-size: 12px;
          height: 30px;
          line-height: 30px;
          color:#fff;
          cursor: move;
          background-color: #2f3242;

          span{
            display: inline-block;
          }
          &.active_filter{
            font-weight: bold;
            color:#2eb3fc;
          }
          &:hover{
            .column{
              color:#218fea;
            }
          }
          .kind{
            color: #20a0ff;
            width: 20px;
            height: 20px;
            text-align: center;
            line-height: 20px;
            vertical-align: sub;
            border-radius: 10px;
            cursor:pointer;
            font-weight: bold;
            &.dimension{

            }
            &.measure{
              color: #48b5cd;
            }
            &:hover{
              background-color:#6d707e;
            }
          }
          .column_type{
            right:2px;
            position: absolute;
            color:#ccc;
          }
        }
       }
   }
   .jtk-overlay {
    background-color: #475568;
    padding: 0.4em;
    font: 12px sans-serif;
    color: #444;
    z-index: 21;
    font-weight: bold;
    border: 2px solid #f5a623;
    cursor: pointer;
    min-width: 42px;
    height: 18px;
    border-radius: 10px;
    text-align: center;
    line-height: 18px;
    &.label_left{
      border: 2px solid #f5a623;
      color:#fff;
      &:hover{
        border: 2px solid #fdd9a4;
        background-color: #6f8daf;
      }
    }
    &.label_inner{
      border: 2px solid #f5a623;
      color:#fff;
      &:hover{
        background-color: #6f8daf;
        border: 2px solid #fdd9a4;
      }
    }
  }
</style>

