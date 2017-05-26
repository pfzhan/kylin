<template>
<div class="model_edit_box"  @drop='drop($event)' @dragover='allowDrop($event)' :style="{width:dockerScreen.w+'px', height:dockerScreen.h+'px'}">

    <div class="tree_list" >
<!--     <draggable  @start="drag=true" @end="drag=false"> -->
      <model-assets  v-on:drag="drag" :project="extraoption.project" @okFunc="serverDataToDragData" ></model-assets>
      <!-- <el-tree v-if="extraoption.uuid" @nodeclick="clickCube" :data="cubeDataTree" style="background-color: #f1f2f7;border:none;width:250px;" :render-content="renderCubeTree"></el-tree> -->
      <tree  v-if="extraoption.uuid" style="background-color: #f1f2f7;border:none;width:250px;" :treedata="cubeDataTree" placeholder="输入关键字过滤Data Source" maxLabelLen="20" :showfilter= "false" :expandall="true" @nodeclick="clickCube" emptytext="无数据" v-unselect :renderTree="renderCubeTree"></tree>
<!--     </draggable> -->
    </div>
    <ul class="model_tool">
        <li class="toolbtn tool_add" @click="addZoom" v-unselect title="放大视图"><span></span></li>
        <li class="toolbtn tool_jian" @click="subZoom" v-unselect title="缩小视图"><span></span></li>
        <li class="toolbtn" @click="autoLayerPosition" v-unselect style="line-height:26px;" title="自动布局"><icon style="color:#383838" name="life-bouy"></icon></li>
      </ul>
    <div class="btn_group"  v-if="actionMode!=='view'">
      <el-button @click="saveDraft(true)" :loading="draftBtnLoading">Draft</el-button>
      <el-button type="primary" @click="saveCurrentModel" :loading="saveBtnLoading">Save</el-button>
    </div>  
    <div class="tips_group">
      
    </div>
    <div class="model_edit" :style="{left:docker.x +'px'  ,top:docker.y + 'px'}">
      <div class="table_box" v-if="table&&table.kind" @drop='dropColumn' @dragover='allowDrop($event)'  v-for="table in tableList" :key="table.guid" :id="table.guid" v-bind:class="table.kind.toLowerCase()" v-bind:style="{ left: table.pos.x + 'px', top: table.pos.y + 'px' }" >
        <div class="tool_box" >
            <icon name="table" class="el-icon-menu" style="color:#fff" @click.native="openModelSubMenu('hide', table.database, table.name)"></icon>
            <icon name="gears" v-if="actionMode!=='view'"  class="el-icon-share" style="color:#fff" v-on:click.native="addComputedColumn(table.guid)"></icon>
            <icon name="sort-alpha-asc" v-on:click.native="sortColumns(table)"></icon>
            <i class="fa fa-window-close"></i>
            <el-dropdown @command="selectTableKind" class="ksd-fright" v-if="actionMode!=='view'">
              <span class="el-dropdown-link">
               <i class="el-icon-setting" style="color:#fff"></i>
              </span>
              <el-dropdown-menu slot="dropdown" >
                <el-dropdown-item command="ROOTFACT" :data="table.guid">Fact</el-dropdown-item>
                <el-dropdown-item command="LOOKUP" :data="table.guid">Lookup</el-dropdown-item>
              </el-dropdown-menu>
              </el-dropdown> 
        </div>
        <i class="el-icon-close close_table" v-on:click="removeTable(table.guid)" v-if="actionMode!=='view'"></i> 
        <p class="table_name  drag_bar" v-on:dblclick="editAlias(table.guid)" v-visible="aliasEditTableId!=table.guid">
        <common-tip :tips="table.database+'.'+table.name" class="drag_bar">{{(table.alias)|omit(16,'...')}}</common-tip></p>
        <el-input v-model="table.alias" v-on:blur="cancelEditAlias(table.guid)" class="alias_input"  size="small" placeholder="enter alias..." v-visible="aliasEdit&&aliasEditTableId==table.guid"></el-input>
        <p class="filter_box"><el-input v-model="table.filterName" v-on:change="filterColumnByInput(table.filterName,table.guid)"  size="small" placeholder="enter filter..."></el-input></p>
        <section data-scrollbar class="columns_box">
          <ul>
            <li draggable @dragstart="dragColumns" @dragend="dragColumnsEnd"  v-for="column in table.columns" :key="column.guid"  class="column_li"  v-bind:class="{'active_filter':column.isActive}" :data-guid="table.guid" :data-column="column.name" ><span class="kind" :class="{dimension:column.btype=='D',measure:column.btype=='M'}" v-on:click="changeColumnBType(table.guid,column.name,column.btype)">{{column.btype}}</span><span class="column" v-on:click="selectFilterColumn(table.guid,column.name,column.datatype)"><common-tip trigger="click" :tips="column.name" style="font-size:10px;">{{column.name|omit(14,'...')}}</common-tip></span><span class="column_type">{{column.datatype}}</span></style></li>
          </ul>
        </section>
        <div class="more_tool"></div>
      </div>
     

    </div>
     <el-dialog title="主外键关系" v-model="dialogVisible" size="small" class="links_dialog">
        <span>
        <!--   <el-row :gutter="20" class="ksd-mb10">
            <el-col :span="24">
              <div class="grid-content bg-purple">
                <a style="color:#56c0fc">{{currentLinkData.source.alias}}</a>
              </div>
            </el-col>
            </el-row> -->
            <br/>
             <el-row :gutter="20" class="ksd-mb10" style="line-height:49px;">
             <el-col :span="9" style="text-align:center">
              <div class="grid-content bg-purple">
                <a style="color:#56c0fc">{{currentLinkData.source.alias}}</a>
              </div>
            </el-col>
            <el-col :span="6" style="text-align:center">
              <div class="grid-content bg-purple">
                  <el-select v-model="currentLinkData.joinType" :disabled = "actionMode ==='view'" style="width:120px;" @change="switchJointType(currentLinkData.source.guid,currentLinkData.target.guid, currentLinkData.joinType)" placeholder="请选择连接类型">
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
                <a style="color:#56c0fc">{{currentLinkData.target.alias}}</a>
              </div>
            </el-col>
            </el-row>
            <br/>
            <!--  <el-row :gutter="20">
            <el-col :span="24">
              <div class="grid-content bg-purple">
                <a style="color:#56c0fc">{{currentLinkData.target.alias}}</a>
              </div>
            </el-col>
            </el-row> -->
          </el-row>
          <br/>
           <el-table
              :data="currentTableLinks"
              :show-header="false"
              style="width: 100%;border:none" class="linksTable">
              <el-table-column style="border:none"
                label="主键"
                >
                <template scope="scope">
                <el-select v-model="scope.row[2]" placeholder="请选择" style="width:100%" :disabled = "actionMode ==='view'">
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
                label="外键"
                >
                <template scope="scope">
                <el-select v-model="scope.row[3]" placeholder="请选择" style="width:100%" :disabled = "actionMode ==='view'">
                  <el-option
                    v-for="item in currentLinkData.target.columns"
                    :key="item.name"
                    :label="item.name"
                    :value="item.name">
                  </el-option>
                </el-select>
                </template>
              </el-table-column>
              <br/>
              <el-table-column style="border:none" label="操作" width="80" v-if="actionMode!=='view'">
                <template scope="scope" >
                  <confirm-btn v-if="scope.row[5]" v-on:okFunc='delConnect(scope.row)' :tips="deleteLinkTips"><el-button size="small"
          type="danger">删除</el-button></confirm-btn>
                </template>
              </el-table-column>
            </el-table>
            <br/>
            <el-button type="primary" v-if="actionMode!=='view'" @click="addJoinCondition(currentLinkData.source.guid,currentLinkData.target.guid, '', '', currentLinkData.joinType,true)">添加join条件</el-button>
        </span>
         <span slot="footer" class="dialog-footer">
            <el-button type="primary" @click="saveLinks(currentLinkData.source.guid,currentLinkData.target.guid)">确 定</el-button>
          </span> 
      </el-dialog>
       <el-dialog title="Computed Column" v-model="computedColumnFormVisible" size="small">
          <div>
            <el-form label-position="top"  ref="computedColumnForm">
              <el-form-item label="name" >
                <el-input  auto-complete="off" v-model="computedColumn.name"></el-input>
              </el-form-item>
              <el-form-item label="expression" >
                <el-input type="textarea"  auto-complete="off" v-model="computedColumn.expression"></el-input>
              </el-form-item>
              <el-form-item label="returnType" >
                <el-input  auto-complete="off" v-model="computedColumn.returnType"></el-input>
              </el-form-item>  
              <el-form-item label="comment" >
                <el-input type="textarea"  auto-complete="off" v-model="computedColumn.comment"></el-input>
              </el-form-item>  
            </el-form>
          </div>
          <span slot="footer" class="dialog-footer">
            <el-button @click="computedColumnFormVisible = false">取 消</el-button>
            <el-button type="primary" @click="saveComputedColumn">确 定</el-button>
          </span>     
        </el-dialog>
      <model-tool :modelInfo="modelInfo" :actionMode="actionMode" :editLock="editLock" :compeleteModelId="modelData&&modelData.uuid||null" :columnsForTime="timeColumns" :columnsForDate="dateColumns"  :activeName="submenuInfo.menu1" :activeNameSub="submenuInfo.menu2" :tableList="tableList" :partitionSelect="partitionSelect"  :selectTable="currentSelectTable" ref="modelsubmenu"></model-tool>

       <!-- 添加cube -->

    <el-dialog title="Add Cube" v-model="createCubeVisible" size="tiny">
      <el-form :model="cubeMeta" :rules="createCubeFormRule" ref="addCubeForm">
        <el-form-item label="Cube Name" prop="cubeName">
          <el-input v-model="cubeMeta.cubeName" auto-complete="off"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="createCubeVisible = false">取 消</el-button>
        <el-button type="primary" @click="createCube">添 加</el-button>
      </div>
    </el-dialog>

    <el-dialog title="Confirm Add" v-model="addModelDialogDisable" >
       <partition-column :modelInfo="modelInfo" :actionMode="actionMode" :columnsForTime="timeColumns" :columnsForDate="dateColumns" :tableList="tableList" :partitionSelect="partitionSelect" ></partition-column>

       <el-checkbox v-model="openModelCheck">Check Model</el-checkbox>

     <!--    <el-slider v-model="modelStaticsRange" :max="100" :format-tooltip="formatTooltip" :disabled = '!openModelCheck'></el-slider> -->
        <!-- <slider label="Check Model" @changeBar="changeModelBar" :show="addModelDialogDisable"></slider> -->
       <div slot="footer" class="dialog-footer">
        <el-button @click="addModelDialogDisable = false">取 消</el-button>
        <el-button type="primary" @click="saveAndCheckModel" :loading="saveBtnLoading">添 加</el-button>
      </div>
    </el-dialog>
</div>
</template>
<script>
import { jsPlumb } from 'jsplumb'
import { sampleGuid, indexOfObjWithSomeKey, filterObjectArray, objectArraySort, objectClone, getNextValInArray } from '../../util/index'
import { mapActions } from 'vuex'
import $ from 'jquery'
import Scrollbar from 'smooth-scrollbar'
import modelassets from './model_assets'
import Draggable from 'draggable'
import modelEditTool from 'components/model/model_edit_panel'
import partitionColumn from 'components/model/model_partition.vue'
import { handleSuccess, handleError } from 'util/business'
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
      createCubeVisible: false,
      openModelCheck: true,
      modelStaticsRange: 100,
      addModelDialogDisable: false,
      createCubeFormRule: {
        cubeName: [
          {required: true, message: '请输入cube名字', trigger: 'blur'},
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
      joinTypes: [{label: 'Left Join', value: 'left'}, {label: 'Inner Join', value: 'inner'}],
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
        joinType: 'left'
      },
      joinType: false,
      aliasEdit: false,
      aliasEditTableId: '',
      switchWidth: 100,
      dialogVisible: false,
      computedColumnFormVisible: false,
      deleteLinkTips: '你确认要删除该连接吗？',
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
      columnUsedInfo: []
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
      // 数据缓冲到vuex里
      cacheModelEdit: 'CACHE_MODEL_EDIT',
      getUsedCols: 'GET_USED_COLS',
      getCubesList: 'GET_CUBES_LIST',
      checkCubeName: 'CHECK_CUBE_NAME_AVAILABILITY',
      statsModel: 'COLLECT_MODEL_STATS'
    }),
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
      var sortedColumns = objectArraySort(tableInfo.columns, squence, key)
      tableInfo.columns = sortedColumns
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
      // if (this.extraoption.actionMode !== 'add') {
      //   this.saveAndCheckModel()
      // } else {
      if (this.draftBtnLoading) {
        this.$message({
          type: 'warning',
          message: '系统正在响应Draft的保存请求，请稍后!'
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
        modelDescData: this.DragDataToServerData()
      }).then(() => {
        this.saveBtnLoading = false
        this.$message({
          type: 'success',
          message: '保存成功!'
        })
        if (this.openModelCheck) {
          this.statsModel({
            project: this.project,
            modelname: this.modelInfo.modelName,
            data: {
              ratio: (this.modelStaticsRange / 100).toFixed(2)
            }
          })
        }
        this.$emit('removetabs', 'model' + this.extraoption.modelName)
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
      var _this = this
      if (this.getRootFact().length <= 0) {
        return
      }
      if (!this.checkModelMetaHasChange()) {
        if (tipChangestate) {
          this.$message({
            type: 'warning',
            message: '未检测到任何改动!'
          })
        }
        return
      }
      this.draftBtnLoading = true
      this.saveModelDraft({
        modelDescData: _this.DragDataToServerData(),
        project: _this.project,
        modelName: _this.modelInfo.name
      }).then((res) => {
        this.draftBtnLoading = false
        handleSuccess(res, (data) => {
          this.modelInfo.uuid = data.uuid
          this.modelInfo.status = 'DRAFT'
          this.modelInfo.last_modified = JSON.parse(data.modelDescData).last_modified
          this.$emit('reload', 'modelList')
          this.$notify({
            title: '成功',
            message: '定时保存为草稿',
            type: 'success'
          })
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
      // var _this = this
      // event.stopPropagation()
      // this.$prompt('请输入Cube名称', '提示', {
      //   confirmButtonText: '确定',
      //   cancelButtonText: '取消'
      //     // inputPattern: /[\w!#$%&'*+/=?^_`{|}~-]+(?:\.[\w!#$%&'*+/=?^_`{|}~-]+)*@(?:[\w](?:[\w-]*[\w])?\.)+[\w](?:[\w-]*[\w])?/,
      //     // inputErrorMessage: '邮箱格式不正确'
      // }).then(({ value }) => {
      //   _this.$emit('addtabs', 'cube', value, 'cubeEdit', {
      //     project: localStorage.getItem('selected_project'),
      //     cubeName: value,
      //     modelName: this.modelInfo.modelName,
      //     isEdit: false
      //   })
      // }).catch(() => {
      // })
    },
    checkName (rule, value, callback) {
      if (!/^\w+$/.test(value)) {
        callback(new Error(this.$t('名字格式有误')))
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
          this.checkCubeName(this.cubeMeta.cubeName).then((data) => {
            this.$message({
              message: '已经存在同名的Cube了',
              type: 'warning'
            })
          }, (res) => {
            handleError(res, (data, code, status, msg) => {
              if (status === 400) {
                this.createCubeVisible = false
                this.$emit('addtabs', 'cube', this.cubeMeta.cubeName, 'cubeEdit', {
                  project: this.cubeMeta.projectName,
                  cubeName: this.cubeMeta.cubeName,
                  modelName: this.cubeMeta.modelName,
                  isEdit: false
                })
              } else {
                this.$message({
                  message: msg,
                  type: 'warning'
                })
              }
            })
          })
        }
      })
    },
    renderCubeTree (h, {node, data, store}) {
      var _this = this
      return this.$createElement('div', {
        class: [{'el-tree-node__label': true, 'leaf-label': node.isLeaf && node.level !== 1}, node.icon],
        domProps: {
          innerHTML: node.level === 1 ? '<span>' + data.label + '</span><span style="width:40px;height:60px; text-align:center;margin-left:30px;font-size:18px;" class="addCube" >+</span>' : '<span>' + data.label + '</span>'
        },
        attrs: {
          title: data.label,
          class: node.icon || ''
        },
        on: {
          click: function (event) {
            if (event.target.className === 'addCube') {
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
    addComputedColumn: function (guid) {
      this.computedColumn.guid = guid
      this.computedColumnFormVisible = true
    },
    saveComputedColumn: function (guid) {
      var _this = this
      this.addComputedColumnToDatabase(function (columnName) {
        _this.$notify({
          title: '成功',
          message: columnName + ' 计算列创建成功',
          type: 'success'
        })
      })
    },
    getPartitionDateColumns: function () {
      var canSetDatePartion = ['date', 'timestamp', 'string', 'bigint', 'int', 'integer', 'varchar']
      var canSetTimePartion = ['time', 'timestamp', 'string', 'varchar']
      var needNotSetDateFormat = ['bigint', 'int', 'integer']
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
          if (canSetDatePartion.indexOf(datatype) >= 0) {
            var needFormat = true
            if (needNotSetDateFormat.indexOf(datatype) >= 0) {
              needFormat = false
            }
            var tabeFullName = tableList[i].alias
            dateColumns[tabeFullName] = dateColumns[tabeFullName] || []
            dateColumns[tabeFullName].push({name: tableList[i].columns[k].name, isFormat: needFormat})
          }
          if (canSetTimePartion.indexOf(datatype) >= 0) {
            timeColumns[tabeFullName] = timeColumns[tabeFullName] || []
            timeColumns[tabeFullName].push({name: tableList[i].columns[k].name, isFormat: true})
          }
        }
      }
      this.dateColumns = {}
      this.timeColumns = {}
      this.dateColumns = Object.assign({}, this.dateColumns, dateColumns)
      this.timeColumns = Object.assign({}, this.timeColumns, timeColumns)
    },
    addComputedColumnToDatabase: function (callback, isInit) {
      var guid = this.computedColumn.guid
      var databaseInfo = this.getTableInfoByGuid(guid)
      var sameTables = this.getSameOriginTables(databaseInfo.database, databaseInfo.name)
      if (!this.checkSameColumnName(guid, this.computedColumn.name)) {
        var columnObj = {
          name: this.computedColumn.name,
          datatype: this.computedColumn.returnType,
          btype: this.computedColumn.columnType || 'D',
          expression: this.computedColumn.expression
        }
        var computedObj = {
          tableIdentity: databaseInfo.database + '.' + databaseInfo.name,
          columnName: this.computedColumn.name,
          expression: this.computedColumn.expression,
          comment: this.computedColumn.comment,
          datatype: this.computedColumn.returnType,
          disabled: true
        }
        sameTables.forEach((table) => {
          table.columns.push(columnObj)
        })
        if (!isInit) {
          this.modelInfo.computed_columns.push(computedObj)
        }
        this.computedColumnFormVisible = false
        this.computedColumn = {
          guid: '',
          name: '',
          expression: '',
          returnType: ''
        }
        if (typeof callback === 'function') {
          callback(computedObj.columnName)
        }
      } else {
        this.warnAlert('该计算列的名称已经存在！')
      }
    },
    checkSameColumnName: function (guid, column) {
      var columns = this.getTableInfoByGuid(guid).columns
      return indexOfObjWithSomeKey(columns, 'columnName', column) !== -1
    },
    // dimension and measure and disable
    changeColumnBType: function (id, columnName, columnBType) {
      if (this.actionMode === 'view') {
        return
      }
      var tableInfo = this.getTableInfoByGuid(id)
      var alias = tableInfo.alias
      var usedCubes = this.checkColsUsedStatus(alias, columnName)
      if (usedCubes && usedCubes.length) {
        this.warnAlert('已经在名称为' + usedCubes.join(',').replace(/cube\[name=(.*?)\]/gi, '$1') + '的cube中用过该列，不允许修改')
        return
      }
      var willSetType = getNextValInArray(this.columnBType, columnBType)
      this.editTableColumnInfo(id, 'name', columnName, 'btype', willSetType)
      var fullName = tableInfo.database + tableInfo.name
      if (willSetType === '－') {
        this.changeComputedColumnDisable(fullName, columnName, false)
      } else {
        this.changeComputedColumnDisable(fullName, columnName, true)
      }
    },
    changeComputedColumnDisable (fullName, column, status) {
      var len = this.modelInfo.computed_columns && this.modelInfo.computed_columns.length || 0
      for (var i = 0; i < len; i++) {
        var calcColumn = this.modelInfo.computed_columns[i]
        if (calcColumn.tableIdentity === fullName && calcColumn.columnName === column) {
          this.modelInfo.computed_columns[i] = status
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
        if (calcColumn.disabled === true) {
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
          obj = objectClone(projectDataSource[i])
          obj.guid = sampleGuid()
          obj.pos = {x: 300, y: 400}
          obj.alias = obj.alias || obj.name
          obj.kind = 'LOOKUP'
          Object.assign(obj, other)
          break
        }
      }
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
      if (this.checkLock()) {
        return
      }
      var count = this.getConnectsByTableId(guid)
      if (count > 0) {
        this.warnAlert('该table有关联的链接，请先删除连接再删除表！')
        return
      }
      for (var i = 0; i < this.tableList.length; i++) {
        if (this.tableList[i].guid === guid) {
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
        return
      }
      // event.preventDefault()
      // event.dataTransfer && event.dataTransfer.setData('Text', '')
      this.currentDragDom = event.srcElement ? event.srcElement : event.target
      event.dataTransfer.effectAllowed = 'move'
      event.dataTransfer.setData('column', this.currentDragDom.innerHTML)
      event.dataTransfer.setDragImage(this.currentDragDom, 0, 0)
      this.dragType = 'createLinks'
      // var dt = event.originalEvent.dataTransfer
      // dt.effectAllowed = 'copyMove'
      this.currentDragData = {
        table: $(this.currentDragDom).attr('data-guid'),
        columnName: $(this.currentDragDom).attr('data-column')
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
      this.suggestDM({'table': newTableData.database + '.' + newTableData.name}).then((response) => {
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
    dropColumn: function (event) {
      // e.preventDefault()
      if (this.dragType !== 'createLinks') {
        return
      }
      var target = event.srcElement ? event.srcElement : event.target
      var dataBox = $(target).parents('.table_box')
      var columnBox = $(target).parents('.column_li')
      var targetId = dataBox.attr('id')
      var columnName = columnBox.attr('data-column')
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
      this.currentLinkData.source.columns = sourceTableInfo.columns

      this.currentLinkData.target.guid = targetTableInfo.guid
      this.currentLinkData.target.database = targetTableInfo.database
      this.currentLinkData.target.name = targetTableInfo.name
      this.currentLinkData.target.alias = targetTableInfo.alias
      this.currentLinkData.target.columns = targetTableInfo.columns
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
        anchor = [[0.5, 0, 0.6, 0], [0.5, 1, 0.6, 1], [0, 0.5, 0, 0.6], [1, 0.5, 1, 0.6]]
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
      var linkCount = this.getConnectsByTableId(connect[0])
      if (linkCount === 0) {
        this.dialogVisible = false
      }
    },
    delBrokenConnect (p1, p2) {
      var links = this.links
      for (var i = 0; i < links.length; i++) {
        if (links[i][0] === p1 && links[i][1] === p2 && (links[i][2] === '' || links[i][3] === '')) {
          this.links.splice(i, 1)
          i = i - 1
        }
      }
    },
    refreshConnectCountText (p1, p2, joinType) {
      this.getConnectsByTableIds(p1, p2)
      var showLinkCon = this.showLinkCons[p1 + '$' + p2]
      if (showLinkCon) {
        if (this.currentTableLinks.length) {
          this.setConnectLabelText(showLinkCon, p1, p2, '' + this.currentTableLinks.length)
        } else {
          jsPlumb.detach(showLinkCon)
          delete this.showLinkCons[p1 + '$' + p2]
          this.removePoint(showLinkCon.sourceId)
        }
      } else {
        this.addShowLink(p1, p2, joinType)
      }
    },
    addJoinCondition: function (p1, p2, col1, col2, joinType, newCondition) {
      var link = [p1, p2, col1, col2, joinType, newCondition]
      this.links.push(link)
      this.refreshConnectCountText(p1, p2, joinType)
    },
    saveLinks (p1, p2) {
      this.dialogVisible = false
      this.delBrokenConnect(p1, p2)
      this.getConnectsByTableIds(p1, p2)
      var showLinkCon = this.showLinkCons[p1 + '$' + p2]
      if (showLinkCon) {
        this.setConnectLabelText(showLinkCon, p1, p2, '' + this.currentTableLinks.length)
      } else {

      }
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
    },
    getConnectType: function (p1, p2) {
      var joinType = 'left'
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
    addShowLink: function (p1, p2, joinType) {
      this.addSelectPoints(p1, this.plumbInstance, joinType, '', '', true)
      this.addSelectPoints(p2, this.plumbInstance, joinType, '', '', true)
      this.connect(p1, p2, this.plumbInstance, {})
    },
    getConnectsCountByGuid (p1, p2) {
      var obj = {}
      var count = 0
      for (var i = 0; i < this.links.length; i++) {
        if (this.links[i][0] === p1 && this.links[i][1] !== p2) {
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
      if (this.checkIsRootFact(p1)) {
        this.warnAlert('Fact Table不能作为主键表')
        resultTag = true
      }
      // 检查是否有反向链接
      for (var i = 0; i < this.links.length; i++) {
        if (this.links[i][0] === p2 && this.links[i][1] === p1) {
          this.warnAlert('这两个表已经被反向关联过')
          resultTag = true
          break
        }
      }
      if (this.getConnectsCountByGuid(p1, p2) >= 1) {
        this.warnAlert('该表已经关联过其他外键表')
        resultTag = true
      }
      return resultTag
    },
    editAlias: function (guid) {
      if (this.actionMode === 'view') {
        return
      }
      var rootFact = this.getRootFact()
      console.log(rootFact, 'sdde')
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
    },
    selectTableKind: function (command, licompon) {
      var kind = command
      var guid = licompon.$el.getAttribute('data')
      if (kind === 'ROOTFACT') {
        if (this.checkHasFactKindTable(guid)) {
          this.warnAlert('已经有一个Fact Table了')
          return
        }
        for (var i in this.links) {
          if (this.links[i][0] === guid) {
            this.warnAlert('有一个连接的主键已经在该表上，该表不能再被设置成Fact，请删除连接重试！')
            return
          }
        }
      }
      var tableInfo = this.getTableInfoByGuid(guid)
      this.editTableInfoByGuid(guid, 'kind', kind)
      this.editTableInfoByGuid(guid, 'alias', tableInfo.name)
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
    getDimensions: function () {
      var resultArr = []
      for (var i = 0; i < this.tableList.length; i++) {
        var obj = {
          table: this.tableList[i].alias,
          columns: []
        }
        var columns = this.tableList[i].columns
        var len = columns && columns.length || 0
        for (var j = 0; j < len; j++) {
          if (columns[j].btype === 'D') {
            obj.columns.push(columns[j].name)
          }
        }
        if (obj.columns.length) {
          resultArr.push(obj)
        }
      }
      return resultArr
    },
    getMeasures: function () {
      var resultArr = []
      for (var i = 0; i < this.tableList.length; i++) {
        var columns = this.tableList[i].columns
        var len = columns && columns.length || 0
        for (var j = 0; j < len; j++) {
          if (columns[j].btype === 'M') {
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
    // 设置所有列中只有一个列有一种属性，其他都为另一种属性
    editTableColumnsUniqueInfo: function (guid, filterColumnKey, filterColumnVal, columnKey, value, oppositeValue) {
      var _this = this
      this.tableList.forEach(function (table) {
        if (table.guid === guid) {
          for (let i = 0; i < table.columns.length; i++) {
            var col = table.columns[i]
            if (col[filterColumnKey] === filterColumnVal) {
              _this.$set(col, columnKey, value)
            } else {
              _this.$set(col, columnKey, oppositeValue)
            }
          }
        }
      })
    },
    warnAlert: function (str) {
      this.$message(str)
    },
    // trans Data
    DragDataToServerData: function (needJson) {
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
        status: this.modelInfo.status,
        last_modified: this.modelInfo.last_modified,
        filter_condition: this.modelInfo.filterStr,
        name: this.modelInfo.modelName,
        description: this.modelInfo.modelDiscribe,
        computed_columns: fainalComputed
      }
      // console.log(this.partitionSelect, '2456')
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
            table: p1TableInfo.database + '.' + p1TableInfo.name,
            kind: p1TableInfo.kind,
            alias: p1TableInfo.alias,
            join: {
              type: joinType,
              primary_key: [p1TableInfo.alias + '.' + col1],
              foreign_key: [p2TableInfo.alias + '.' + col2]
            }
          }
        } else {
          linkTables[p1 + '$' + p2].join.primary_key.push(p1TableInfo.alias + '.' + col1)
          linkTables[p1 + '$' + p2].join.foreign_key.push(p2TableInfo.alias + '.' + col2)
        }
      }
      for (var s in linkTables) {
        kylinData.lookups.push(linkTables[s])
      }
      // dimensions
      kylinData.dimensions = this.getDimensions()
      kylinData.pos = pos
      kylinData.metrics = this.getMeasures()
      if (needJson) {
        return kylinData
      }
      return JSON.stringify(kylinData)
    },
    serverDataToDragData: function () {
      var _this = this
      // 添加模式
      console.log(this.extraoption, 'ewewe')
      if (!this.extraoption.uuid) {
        this.$set(this.modelInfo, 'modelDiscribe', this.extraoption.modelDesc)
        this.$set(this.modelInfo, 'modelName', this.extraoption.modelName)
        return
      }
      // 编辑模式
      this.getModelByModelName(this.extraoption.modelName).then((response) => {
        handleSuccess(response, (data) => {
          this.modelData = data.model
          this.draftData = data.draft
          this.editLock = !!(this.modelData && this.modelData.uuid)
          var modelData = this.extraoption.status === null ? data.model : data.draft
          if (this.extraoption.status !== null) {
            modelData.name = modelData.name.replace(/_draft/, '')
          }
          if (!modelData.fact_table) {
            return
          }
          Object.assign(_this.modelInfo, {
            uuid: modelData.uuid,
            modelDiscribe: modelData.description,
            modelName: modelData.name,
            status: modelData.status,
            last_modified: modelData.last_modified
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
          _this.modelInfo.filterStr = modelData.filter_condition
          _this.modelInfo.computed_columns = modelData.computed_columns || []
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
          for (var table in baseTables) {
            _this.createTableData(this.extraoption.project, baseTables[table].database, baseTables[table].table, {
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
              let bArr = [baseTables[priAlias].guid, baseTables[foriAlias].guid, pcolumn, fcolumn, lookups[i].join.type]
              baseLinks.push(bArr)
            }
          }
          _this.links = baseLinks
          this.$nextTick(function () {
            Scrollbar.initAll()
            for (let i = 0; i < lookups.length; i++) {
              let priAlias = lookups[i].alias
              let foriFullColumn = lookups[i].join.foreign_key[0]
              var pGuid = baseTables[priAlias].guid
              var fGuid = baseTables[foriFullColumn.split('.')[0]].guid
              var hisConn = _this.showLinkCons[pGuid + '$' + fGuid]
              if (!hisConn) {
                var joinType = lookups[i].join.type
                _this.addShowLink(pGuid, fGuid, joinType)
              }
            }
            if (!modelData.pos) {
              _this.autoLayerPosition()
            }
          })
          // computed column add
          var computedColumnsLen = modelData.computed_columns && modelData.computed_columns.length || 0
          for (let i = 0; i < computedColumnsLen; i++) {
            var fullName = modelData.computed_columns[i].tableIdentity.split('.')
            var tableList = this.getSameOriginTables(fullName[0], fullName[1])
            if (tableList && tableList.length) {
              this.computedColumn = {
                guid: tableList[0].guid,
                name: modelData.computed_columns[i].columnName,
                expression: modelData.computed_columns[i].expression,
                returnType: modelData.computed_columns[i].datatype,
                columnType: modelData.computed_columns[i].datatype
              }
              this.addComputedColumnToDatabase(() => {}, true)
            }
          }

          var modelDimensionsLen = modelData.dimensions && modelData.dimensions.length || 0
          for (let i = 0; i < modelDimensionsLen; i++) {
            var currentD = modelData.dimensions[i]
            for (let j = 0; j < currentD.columns.length; j++) {
              _this.editTableColumnInfo(baseTables[currentD.table].guid, 'name', currentD.columns[j], 'btype', 'D')
            }
          }
          var modelMetricsLen = modelData.metrics && modelData.metrics.length || 0
          for (let i = 0; i < modelMetricsLen; i++) {
            var currentM = modelData.metrics[i]
            _this.editTableColumnInfo(baseTables[currentM.split('.')[0]].guid, 'name', currentM.split('.')[1], 'btype', 'M')
          }
          // partition time setting
          _this.getPartitionDateColumns()
          _this.firstRenderServerData = true
          // _this.autoLayerPosition()
        })
      })
    },
    checkColsUsedStatus (alias, columnName) {
      if (this.columnUsedInfo) {
        for (var i in this.columnUsedInfo) {
          if (alias + '.' + columnName === i) {
            return this.columnUsedInfo[i]
          }
        }
      }
      return null
    },
    checkTableUsedStatus (alias) {
      if (this.columnUsedInfo) {
        for (var i in this.columnUsedInfo) {
          if (alias === i.split('.')[0]) {
            return true
          }
        }
      }
      return false
    },
    autoCalcLayer: function (root, result, deep) {
      var rootGuid = root || this.getRootFact().length && this.getRootFact()[0].guid
      var index = ++deep || 1
      var resultArr = result || [[rootGuid]]
      var tempArr = []
      for (var i in this.showLinkCons) {
        if (rootGuid === i.split('$')[1]) {
          resultArr[index] = resultArr[index] || []
          resultArr[index].push(i.split('$')[0])
          tempArr.push(i.split('$')[0])
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
      var _this = this
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
          this.$nextTick(function () {
            _this.refreshPlumbObj()
          })
        }
      }
      this.refreshPlumbObj()
    },
    autoLayerPosition_2: function () {
    },
    // saveData: function () {
    //   this.DragDataToServerData()
    // },
    jsplumbZoom: function (zoom, instance, transformOrigin, el) {
      transformOrigin = transformOrigin || [ 0.5, 0.5 ]
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
      this.currentZoom += 0.01
      this.jsplumbZoom(this.currentZoom, this.plumbInstance)
    },
    subZoom: function () {
      this.currentZoom -= 0.01
      this.jsplumbZoom(this.currentZoom, this.plumbInstance)
    },
    // 检测数据有没有发生变化
    checkModelMetaHasChange () {
      var jsonData = this.DragDataToServerData(true)
      delete jsonData.pos
      delete jsonData.uuid
      delete jsonData.last_modified
      delete jsonData.status
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
        this.saveConfig.timer++
        if (this.saveConfig.timer > this.saveConfig.limitTimer) {
          this.saveDraft()
          this.saveConfig.timer = 0
        }
        this.timerSave()
      }, 1000)
    },
    resizeWindow: function (newVal) {
      var wWidth = $(window).width()
      var wHeight = $(window).height()
      var modelEditTool = $(this.$el).find('.model_edit_tool')
      if (newVal === 'brief_menu') {
        this.dockerScreen.w = wWidth - 140
        this.dockerScreen.h = wHeight - 176
        modelEditTool.addClass('smallScreen')
      } else {
        modelEditTool.removeClass('smallScreen')
        this.dockerScreen.w = wWidth - 240
        this.dockerScreen.h = wHeight - 176
      }
    },
    clickCube: function (leaf) {
      console.log(leaf, 12220)
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
    this.diagnose({project: this.extraoption.project, modelName: this.extraoption.modelName}).then((response) => {
      console.log(response)
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
      var draggablePlugin = new Draggable($(this.$el).find('.model_edit')[0], {
        onDrag: (s) => {
        },
        onDragEnd: (s, x, y) => {
          this.docker.x = x
          this.docker.y = y
        },
        filterTarget: (dom) => {
          console.log(dom.className)
          if (dom) {
            if (!dom.className || typeof dom.className !== 'string') {
              return false
            }
            return dom.tagName !== 'INPUT' && dom.className !== 'tool_box' && dom.tagName !== 'SECTION' && dom.className !== 'toolbtn' && dom.className.indexOf('column_li') < 0 && dom.className.indexOf('column') < 0 && dom.className.indexOf('el-tooltip') < 0
          }
        },
        useGPU: true
      })
      console.log(draggablePlugin)
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
    usedInCube () {
      for (var i in this.columnUsedInfo) {
        return true
      }
      return false
    }
  },
  created () {
    // this.$store.state.model.modelEditCache[this.project + '$' + this.modelGuid] = {}
    this.getUsedCols(this.extraoption.modelName).then((res) => {
      handleSuccess(res, (data) => {
        this.columnUsedInfo = data
      })
    })
    this.actionMode = this.extraoption.mode
    this.getCubesList({pageSize: 100000, pageOffset: 0, projectName: this.extraoption.project, modelName: this.extraoption.modelName}).then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        this.cubesList = data.cubes
        data.cubes.forEach((cube) => {
          this.cubeDataTree[0].children.push({
            id: cube.uuid,
            label: cube.name
          })
        })
      })
    })
  },
  destroyed () {
    clearTimeout(this.timerST)
  },
  updated () {
    // console.log(1)
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
   .model_edit_box {
    margin-top: 20px;
    position: relative;
    background-color: #475568;
    background-image: url('../../assets/img/jsplumb.png');
    background-repeat:repeat;
    overflow: hidden;
   }
   .tree_list {
      height: 100%;
      background-color: #f1f2f7;
      position: absolute;
      z-index: 2000;
      overflow-y: auto;
      overflow-x: hidden;
      
   }
   .model_tool{
     position: absolute;
     right: 6px;
     z-index: 2000;
     li{
      -webkit-select:none;
      user-select:none;
      -moz-user-select:none;
      width: 26px;
      height: 26px;
      position: relative;
     box-shadow: 1px 2px 1px rgba(0,0,0,.15);
    cursor: pointer;
    overflow: hidden;
    background-color: #FFF;
 
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
    bottom: 60px;
    right: 6px;
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
   }
   .links_dialog{
     p{
       color:green;
     }
     .el-table { 
      tr{
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
       .close_table{
        position: absolute;
        top: 10px;
        right: 6px;
        color:#fff;
        font-size: 12px;
        cursor: pointer;
       }
       &.rootfact{
          .table_name{
            background-color: #2eb3fc;
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
          font-size: 14px;
          background-color:#5a6a80;
       }
       width: 220px;
       left: 440px;
       background-color: #64748a; 
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
          padding:2px 6px;
          top:36px;
          left: 0;
          right: 0;
          font-size: 12px;
          color:#fff;
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
        margin-top: 10px;
        padding-top: 12px;
       }
       .table_name{
         height: 36px;
         line-height: 36px;
         background-color: #8492a6;
         padding-left:10px;
         color:#fff;
       }
       .columns_box{
        height: 300px;
        overflow: hidden;
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
          cursor: pointer;
          background-color: #64748a;
          span{
            display: inline-block;
          }
          &.active_filter{
            font-weight: bold;
            color:#2eb3fc;
          }
          .kind{
            color: #20a0ff;
            width: 20px;
            height: 20px;
            text-align: center;
            line-height: 20px;
            border-radius: 10px;
            cursor:pointer;
            font-weight: bold;
            &.dimension{
              
            }
            &.measure{
              color: #48b5cd;
            }
            &:hover{
              background-color:#59697f;
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
      color:#fff
    }
    &.label_inner{
      border: 2px solid #f5a623;
      color:#fff
    }
  }
</style>

