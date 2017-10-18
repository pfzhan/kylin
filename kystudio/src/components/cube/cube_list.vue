<template>
<div class="paddingbox ksd-border-tab cube-list" id="cube-list">
  <el-row class="cubeSearch" v-show="!isViewCubeMode">
    <el-select v-model="currentModel" style="float: left;margin-left: 0!important;" class="ksd-ml-20" :placeholder="$t('chooseModel')">
      <el-option
        v-for="item in modelsList"
        :key="item.name"
        :label="item.name"
        :value="item.name">
      </el-option>
    </el-select>
    <el-input v-model="filterCube" icon="search" class="ksd-mb-10 ksd-ml-10 ksd-fleft" @change="filterChange" :placeholder="$t('kylinLang.common.pleaseFilter')"></el-input>
    <el-button type="trans" icon="plus" class="ksd-mb-10 ksd-ml-20 ksd-fleft radius fleft" v-if="isAdmin || hasSomePermissionOfProject(selected_project)" @click.native="addCube">{{$t('kylinLang.common.cube')}}</el-button>
  </el-row>

  <el-table id="cube-list-table"  v-show="cubesList&&cubesList.length"
    :data="cubesList"
    :default-expand-all="isViewCubeMode"
    :row-class-name="showRowClass"
    border
    style="width:100%">
    <el-table-column type="expand" width="30">
      <template scope="props">
        <el-tabs activeName="first" class="el-tabs--default" id="cube-view" @tab-click="changeTab">
          <el-tab-pane label="Grid" name="first" v-if="!props.row.is_draft">
            <cube_desc_view :cube="props.row" :index="props.$index"></cube_desc_view>
          </el-tab-pane>
          <el-tab-pane label="SQL Patterns" name="second" v-if="!props.row.is_draft">
            <show_sql :cube="props.row"></show_sql>
          </el-tab-pane>
          <el-tab-pane label="JSON" name="third" v-if="!props.row.is_draft">
            <show_json :json="props.row.desc" ></show_json>
          </el-tab-pane>
          <!-- <el-tab-pane label="Access" name="fourth" v-if="!props.row.is_draft">
            <access_edit  :accessId="props.row.uuid" own='cube'></access_edit>
          </el-tab-pane> -->
          <el-tab-pane :label="$t('storage')" name="fifth" v-if="!props.row.is_draft">
            <segments :cube="props.row"></segments>
          </el-tab-pane>
        </el-tabs>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('name')"
      sortable
      show-overflow-tooltip
      width="130"
      prop="name">
    </el-table-column>
    <el-table-column
      :label="$t('model')"
      sortable
      show-overflow-tooltip
      prop="model">
    </el-table-column>
    <el-table-column
      :label="$t('status')"
      sortable
      width="90"
      prop="status">
      <template scope="scope">
        <el-tag  :type="scope.row.status === 'DISABLED' ? 'danger' : scope.row.status === 'DESCBROKEN'? 'warning' : 'success'">{{scope.row.status}}</el-tag>
      </template>
    </el-table-column>
    <el-table-column
      sortable
      width="100"
      :label="$t('cubeSize')">
      <template scope="scope">
        <el-tooltip class="item" effect="dark" placement="top">
          <div slot="content">
            {{$t('sourceTableSize')}}{{scope.row.input_records_size|dataSize}}<br/>
            {{$t('expansionRate')}}{{(scope.row.input_records_size>0? scope.row.size_kb*1024/scope.row.input_records_size : 0) * 100 | number(2)}}%
          </div>
          <span>{{(totalSizeList[scope.row.name]||0) | dataSize}}</span>

        </el-tooltip>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('sourceRecords')"
      sortable
      show-overflow-tooltip
      prop="input_records_count">
    </el-table-column>
    <el-table-column
      sortable
      show-overflow-tooltip
      :label="$t('lastBuildTime')">
      <template scope="scope">
        <span v-if="scope.row.segments[scope.row.segments.length-1]">{{scope.row.buildGMTTime}}</span>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('owner')"
      sortable
      show-overflow-tooltip
      width="90"
      prop="owner">
    </el-table-column>
    <el-table-column
      :label="$t('updateTime')"
      sortable
      show-overflow-tooltip
      prop="lastModifiedGMT">
    </el-table-column>
    <el-table-column
       width="70"
      :label="$t('actions')">
      <template scope="scope">
      <span v-if="!(isAdmin || hasSomePermissionOfProject(scope.row.project) || hasOperationPermissionOfProject(selected_project))"> N/A</span>
        <el-dropdown trigger="click" v-show="isAdmin || hasSomePermissionOfProject(scope.row.project) || hasOperationPermissionOfProject(selected_project) ">
          <el-button class="el-dropdown-link">
            <i class="el-icon-more"></i>
          </el-button>
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item @click.native="openValidateSql(scope.row)" v-show="!scope.row.is_draft">{{$t('kylinLang.common.verifySql')}}</el-dropdown-item>
            <el-dropdown-item v-show="scope.row.status !=='READY' && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="drop(scope.row)">{{$t('drop')}}</el-dropdown-item>
            <el-dropdown-item @click.native="edit(scope.row)" v-show="isAdmin || hasSomePermissionOfProject(selected_project)">{{$t('edit')}}</el-dropdown-item>
            <el-dropdown-item v-show="scope.row.status !== 'DESCBROKEN' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project)) " @click.native="build(scope.row)">{{$t('build')}}</el-dropdown-item>
            <el-dropdown-item v-show="scope.row.status!=='DISABLED' && scope.row.status!=='DESCBROKEN' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project)) " @click.native="refresh(scope.row)">{{$t('refresh')}}</el-dropdown-item>
            <el-dropdown-item v-show="scope.row.status!== 'DESCBROKEN'&& !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project)) " @click.native="merge(scope.row)">{{$t('merge')}}</el-dropdown-item>
            <el-dropdown-item v-show="scope.row.status=='DISABLED' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="enable(scope.row.name)">{{$t('enable')}}</el-dropdown-item>
            <el-dropdown-item v-show="scope.row.status ==='READY' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="disable(scope.row.name)">{{$t('disable')}}</el-dropdown-item>
            <el-dropdown-item v-show="scope.row.status==='DISABLED' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="purge(scope.row.name)">{{$t('purge')}}</el-dropdown-item>
            <el-dropdown-item v-show="scope.row.status!=='DESCBROKEN' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="clone(scope.row)">{{$t('clone')}}</el-dropdown-item>

            <el-dropdown-item @click.native="view(scope.row)" v-show="isAdmin" style="border-top:solid 1px rgb(68, 75, 103)">{{$t('viewCube')}}</el-dropdown-item>
            <el-dropdown-item @click.native="backup(scope.row.name)" v-show="!scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project))  ">{{$t('backup')}}</el-dropdown-item>
            <el-dropdown-item v-show="isAdmin || hasSomePermissionOfProject(selected_project)" @click.native="editCubeDesc(scope.row)">{{$t('editCubeDesc')}}</el-dropdown-item>
            </el-dropdown-menu>
        </el-dropdown>
      </template>
    </el-table-column>
   <!--  <el-table-column
      sortable
      label="Admin">
      <template scope="scope">
      <span v-show="!isAdmin"> N/A</span>
        <el-dropdown trigger="click" v-show="isAdmin">
          <el-button class="el-dropdown-link">
            <i class="el-icon-more"></i>
          </el-button >
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item v-show="scope.row.status==='DISABLED' " @click.native="editCubeDesc(scope.row)">{{$t('editCubeDesc')}}</el-dropdown-item>
            <el-dropdown-item @click.native="view(scope.row)">{{$t('viewCube')}}</el-dropdown-item>
            <el-dropdown-item @click.native="backup(scope.row.name)" v-show="!scope.row.is_draft ">{{$t('backup')}}</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </template>
    </el-table-column> -->
  </el-table>
   <pager ref="pager"  :totalSize="totalCubes"  v-on:handleCurrentChange='currentChange' ></pager>

  <div class="null_pic_box" v-if="!(cubesList && cubesList.length)"><img src="../../assets/img/no_cube.png" class="null_pic_2"></div>

  <el-dialog :title="$t('cubeBuildConfirm')" v-model="buildCubeFormVisible" :close-on-press-escape="false" :close-on-click-modal="false" @close="resetCubeBuildField">
    <build_cube :cubeDesc="selected_cube" ref="buildCubeForm" v-on:validSuccess="buildCubeValidSuccess"></build_cube>
    <div slot="footer" class="dialog-footer">
      <el-button @click="buildCubeFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" @click="checkBuildCubeForm">{{$t('yes')}}</el-button>
    </div>
  </el-dialog>

  <el-dialog :title="$t('cubeCloneConfirm')" v-model="cloneCubeFormVisible">
    <clone_cube :cubeDesc="selected_cube" ref="cloneCubeForm" v-on:validSuccess="cloneCubeValidSuccess"></clone_cube>
    <div slot="footer" class="dialog-footer">
      <el-button @click="cloneCubeFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" @click="checkCloneCubeForm">{{$t('yes')}}</el-button>
    </div>
  </el-dialog>

  <el-dialog :title="$t('cubeMergeConfirm')" v-model="mergeCubeFormVisible">
    <merge_cube :cubeDesc="selected_cube" ref="mergeCubeForm" v-on:validSuccess="mergeCubeValidSuccess"></merge_cube>
    <div slot="footer" class="dialog-footer">
      <el-button @click="mergeCubeFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" @click="checkMergeCubeForm">{{$t('yes')}}</el-button>
    </div>
  </el-dialog>

  <el-dialog :title="$t('cubeRefreshConfirm')" v-model="refreshCubeFormVisible">
    <refresh_cube :cubeDesc="selected_cube" ref="refreshCubeForm" v-on:validSuccess="refreshCubeValidSuccess"></refresh_cube>
    <div slot="footer" class="dialog-footer">
      <el-button @click="refreshCubeFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" @click="checkRefreshCubeForm">{{$t('yes')}}</el-button>
    </div>
  </el-dialog>
   <!-- 添加cube -->

    <el-dialog class="add-m" title="Add Cube" v-model="createCubeVisible" size="tiny" @close="resetCubeForm">
      <el-form :model="cubeMeta" :rules="createCubeFormRule" ref="addCubeForm">
        <el-form-item :label="$t('kylinLang.cube.cubeName')" prop="cubeName" style="margin-top: 10px;">
          <span slot="label">{{$t('kylinLang.cube.cubeName')}}
            <common-tip :content="$t('kylinLang.cube.cubeNameTip')" ><icon name="question-circle" class="ksd-question-circle"></icon></common-tip>
          </span>
          <el-input v-model="cubeMeta.cubeName" auto-complete="off"></el-input>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.model.modelName')" prop="modelName" style="margin-top: 30px;">
           <el-select v-model="cubeMeta.modelName" style="width: 100%;" :placeholder="$t('kylinLang.common.pleaseSelect')">
            <el-option
              v-for="item in allModels"
              :key="item.name"
              :label="item.name"
              :value="item.name">
            </el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="createCubeVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="createCube" :loading="btnLoading">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>



    <el-dialog :title="$t('kylinLang.common.verifySql')" v-model="checkSQLFormVisible" :before-close="sqlClose" :close-on-press-escape="false" :close-on-click-modal="false">
        <p style="font-size:12px">{{$t('verifyModelTip1')}}</p>
        <p style="font-size:12px">{{$t('verifyModelTip2')}}</p>
        <div :class="{hasCheck: hasCheck}">
        <editor v-model="sqlString" ref="sqlbox" theme="chrome"  class="ksd-mt-20" width="95%" height="200" ></editor>
        </div>
        <div class="ksd-mt-10">
          <el-button :disabled="sqlString === ''" :loading="checkSqlLoadBtn" @click="validateSql" >{{$t('kylinLang.common.verify')}}</el-button><el-button type="text" v-show="checkSqlLoadBtn" @click="cancelCheckSql">{{$t('kylinLang.common.cancel')}}</el-button></div>
        <div class="line" v-if="currentSqlErrorMsg && currentSqlErrorMsg.length || successMsg || errorMsg"></div>
        <div v-if="currentSqlErrorMsg && currentSqlErrorMsg.length || successMsg || errorMsg" class="suggestBox">
          <div v-if="successMsg">
           <el-alert class="pure"
              :title="successMsg"
              show-icon
              :closable="false"
              type="success">
            </el-alert>
          </div>
          <div v-if="errorMsg">
           <el-alert class="pure"
              :title="errorMsg"
              show-icon
              :closable="false"
              type="error">
            </el-alert>
          </div>
          <div v-for="sug in currentSqlErrorMsg">
            <h3>{{$t('kylinLang.common.errorDetail')}}</h3>
            <p>{{sug.incapableReason}}</p>
            <h3>{{$t('kylinLang.common.suggest')}}</h3>
            <p v-html="sug.suggestion"></p>
          </div>
        </div>

        <span slot="footer" class="dialog-footer">
          <!-- <el-button @click="sqlClose()">{{$t('kylinLang.common.cancel')}}</el-button> -->
          <el-button type="primary" :loading="sqlBtnLoading" @click="sqlClose()">{{$t('kylinLang.common.close')}}</el-button>
        </span>
      </el-dialog>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import { pageCount, permissions, NamedRegex } from '../../config'
import showJson from './json'
import showSql from './sql'
import segments from './segments'
import cubeDescView from './view/cube_desc_view'
import buildCube from './dialog/build_cube'
import cloneCube from './dialog/clone_cube'
import mergeCube from './dialog/merge_cube'
import accessEdit from '../project/access_edit'
import refreshCube from './dialog/refresh_cube'
import { handleSuccess, handleError, transToGmtTimeAfterAjax, hasRole, hasPermission, kapConfirm, filterMutileSqlsToOneLine } from '../../util/business'
import { objectClone } from '../../util/index'
export default {
  name: 'cubeslist',
  props: ['extraoption'],
  data () {
    return {
      sqlString: '',
      checkSQLFormVisible: false,
      sqlBtnLoading: false,
      checkSqlLoadBtn: false,
      hasCheck: false,
      successMsg: '',
      errorMsg: '',
      checkSqlResult: [],
      currentSqlErrorMsg: [],
      lockST: null,
      btnLoading: false,
      cubesList: [],
      currentPage: 1,
      totalCubes: 0,
      createCubeVisible: false,
      buildCubeFormVisible: false,
      cloneCubeFormVisible: false,
      mergeCubeFormVisible: false,
      refreshCubeFormVisible: false,
      selected_cube: {},
      selected_project: this.$store.state.project.selected_project,
      filterCube: '',
      currentModel: 'ALL',
      allModels: [],
      totalSizeList: {},
      cubeMeta: {
        cubeName: '',
        modelName: '',
        projectName: ''
      },
      createCubeFormRule: {
        cubeName: [
          {required: true, message: this.$t('kylinLang.cube.inputCubeName'), trigger: 'blur'},
          {validator: this.checkName, trigger: 'blur'}
        ],
        modelName: [
          {required: true, message: this.$t('kylinLang.cube.selectModelName'), trigger: 'change'}
        ]
      }
    }
  },
  components: {
    'show_json': showJson,
    'show_sql': showSql,
    'segments': segments,
    'cube_desc_view': cubeDescView,
    'build_cube': buildCube,
    'clone_cube': cloneCube,
    'merge_cube': mergeCube,
    'refresh_cube': refreshCube,
    'access_edit': accessEdit
  },
  watch: {
    currentModel (val) {
      this.loadCubesList(0)
    }
  },
  methods: {
    ...mapActions({
      getConf: 'GET_CONF',
      getCubesList: 'GET_CUBES_LIST',
      getCubesSegmentsList: 'GET_CUBES_SEGMENTS_LIST',
      deleteCube: 'DELETE_CUBE',
      rebuildCube: 'REBUILD_CUBE',
      rebuildStreamingCube: 'REBUILD_STREAMING_CUBE',
      enableCube: 'ENABLE_CUBE',
      disableCube: 'DISABLE_CUBE',
      purgeCube: 'PURGE_CUBE',
      cloneCube: 'CLONE_CUBE',
      checkCubeName: 'CHECK_CUBE_NAME_AVAILABILITY',
      backupCube: 'BACKUP_CUBE',
      getCubeSql: 'GET_CUBE_SQL',
      deleteRawTable: 'DELETE_RAW_TABLE',
      deleteScheduler: 'DELETE_SCHEDULER',
      loadModels: 'LOAD_ALL_MODEL',
      loadCubeDesc: 'LOAD_CUBE_DESC',
      getHbaseInfo: 'GET_HBASE_INFO',
      getColumnarInfo: 'GET_COLUMNAR_INFO',
      verifyCubeSql: 'VERIFY_CUBE_SQL'
    }),
    sqlClose () {
      this.checkSQLFormVisible = false
      // if (this.sqlString === '') {
      //   this.checkSQLFormVisible = false
      //   return
      // }
      // kapConfirm(this.$t('kylinLang.common.willClose'), {
      //   confirmButtonText: this.$t('kylinLang.common.close'),
      //   cancelButtonText: this.$t('kylinLang.common.cancel')
      // }).then(() => {
      //   this.checkSQLFormVisible = false
      // })
    },
    resetCubeForm () {
      this.$refs.addCubeForm.resetFields()
    },
    openValidateSql (cube) {
      this.selected_cube = cube
      this.checkSQLFormVisible = true
      this.sqlString = ''
      this.hasCheck = false
      this.currentSqlErrorMsg = false
      this.errorMsg = ''
      this.successMsg = ''
      this.$nextTick(() => {
        var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor || ''
        if (editor) {
          editor.setOption('wrap', 'free')
        }
      })
    },
    // 渲染编辑器行号列尺寸
    renderEditerRender (editor) {
      // var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor || ''
      if (!(editor && editor.sesssion)) {
        return
      }
      editor.sesssion.gutterRenderer = {
        getWidth: (session, lastLineNumber, config) => {
          return lastLineNumber.toString().length * config.characterWidth
        },
        getText: (session, row) => {
          return row
        }
      }
    },
    // 取消校验sql
    cancelCheckSql () {
      this.checkSqlLoadBtn = false
    },
    // 校验sql
    validateSql () {
      var sqls = filterMutileSqlsToOneLine(this.sqlString)
      if (sqls.length === 0) {
        return
      }
      this.hasCheck = false
      var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor || ''
      editor && editor.removeListener('change', this.editerChangeHandle)
      this.renderEditerRender(editor)
      this.errorMsg = false
      this.checkSqlLoadBtn = true
      editor.setOption('wrap', 'free')
      // this.sqlString = sqls.join(';\r\n')
      this.sqlString = sqls.length > 0 ? sqls.join(';\r\n') + ';' : ''
      this.verifyCubeSql({
        sqls: sqls,
        cubeName: this.selected_cube.name
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.hasCheck = true
          this.checkSqlResult = data
          this.currentSqlErrorMsg = []
          this.addBreakPoint(data, editor)
          this.bindBreakClickEvent(editor)
          this.checkSqlLoadBtn = false
          editor && editor.on('change', this.editerChangeHandle)
        })
      }, (res) => {
        handleError(res)
      })
    },
    // 添加错误标志
    addBreakPoint (data, editor) {
      this.errorMsg = ''
      this.successMsg = ''
      if (!editor) {
        return
      }
      if (data && data.length) {
        var hasFailValid = false
        data.forEach((r, index) => {
          if (r.capable === false) {
            hasFailValid = true
            editor.session.setBreakpoint(index)
          } else {
            editor.session.clearBreakpoint(index)
          }
        })
        if (hasFailValid) {
          this.errorMsg = this.$t('validFail')
        } else {
          this.successMsg = this.$t('validSuccess')
        }
      }
    },
    // 绑定错误标记事件
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
        var data = this.checkSqlResult
        row = row - 1
        if (data && data.length) {
          if (data[row].capable === false) {
            if (data[row].sqladvices) {
              this.errorMsg = ''
              this.currentSqlErrorMsg = data[row].sqladvices
            }
          } else {
            this.errorMsg = ''
            this.successMsg = ''
            this.currentSqlErrorMsg = []
          }
        }
        e.stop()
      })
    },
    editerChangeHandle () {
      this.hasCheck = false
    },
    initCube () {
      this.cubeMeta = {
        cubeName: '',
        modelName: '',
        projectName: ''
      }
    },
    filterChange () {
      clearTimeout(this.lockST)
      this.lockST = setTimeout(() => {
        this.loadCubesList(this.currentPage - 1)
      }, 1000)
    },
    segmentsSize (segments) {
      let totalSize = 0
      if (segments) {
        segments.forEach(function (segment) {
          totalSize += segment.storageSize
          if (segment.rawTableStorageSize) {
            totalSize += segment.rawTableStorageSize
          }
        })
      }
      return totalSize
    },
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    },
    addCube () {
      if (!this.selected_project) {
        this.$message(this.$t('kylinLang.project.mustSelectProject'))
        return
      }
      this.initCube()
      this.loadAllModels()
      this.createCubeVisible = true
    },
    createCube () {
      this.$refs['addCubeForm'].validate((valid) => {
        if (valid) {
          this.btnLoading = true
          this.checkCubeName({cubeName: this.cubeMeta.cubeName}).then((res) => {
            this.btnLoading = false
            handleSuccess(res, (data) => {
              if (!data) {
                this.$message({
                  duration: 0,  // 不自动关掉提示
                  showClose: true,    // 给提示框增加一个关闭按钮
                  message: this.$t('kylinLang.cube.sameCubeName'),
                  type: 'warning'
                })
              } else {
                this.createCubeVisible = false
                this.$emit('addtabs', 'cube', this.cubeMeta.cubeName, 'cubeEdit', {
                  project: localStorage.getItem('selected_project'),
                  cubeName: this.cubeMeta.cubeName,
                  modelName: this.cubeMeta.modelName,
                  isEdit: false
                })
              }
            })
          }, (res) => {
            this.btnLoading = false
            handleError(res)
          })
        }
      })
    },
    reloadCubeList () {
      this.loadCubesList(this.currentPage - 1)
    },
    showRowClass (o) {
      return o.is_draft ? 'is_draft' : ''
    },
    loadCubesList: function (curPage) {
      let cubesNameList = []
      let param = {pageSize: pageCount, pageOffset: curPage}
      if (localStorage.getItem('selected_project')) {
        param.projectName = localStorage.getItem('selected_project')
      }
      if (this.currentModel && this.currentModel !== 'ALL') {
        param.modelName = this.currentModel
      }
      if (this.filterCube) {
        param.cubeName = this.filterCube
        param.exactMatch = false
      }
      if (this.extraoption && this.extraoption.cubeName) {
        param.cubeName = this.extraoption.cubeName
      }
      let timeZone = localStorage.getItem('GlobalSeverTimeZone') ? localStorage.getItem('GlobalSeverTimeZone') : ''
      this.getCubesList(param).then((res) => {
        handleSuccess(res, (data) => {
          this.cubesList = data.cubes.map((p) => {
            if (!p.is_draft) {
              cubesNameList.push(p.name)
            }
            p.lastModifiedGMT = p.last_modified === 0 ? '' : transToGmtTimeAfterAjax(p.last_modified, timeZone, this)
            if (p.segments.length > 0) {
              p.buildGMTTime = p.segments[p.segments.length - 1].last_build_time === 0 ? '' : transToGmtTimeAfterAjax(p.segments[p.segments.length - 1].last_build_time, timeZone, this)
            }
            return p
          })
          this.totalCubes = data.size
          if (cubesNameList.length > 0) {
            cubesNameList.forEach((cubename) => {
              // this.totalSizeList[cubename] = 0
              this.loadCubeDesc({cubeName: cubename, project: this.selected_project}).then((res) => {
                var innerCubeName = cubename
                handleSuccess(res, (data) => {
                  if (data.cube && (data.cube.storage_type === 100 || data.cube.storage_type === 99)) {
                    this.getColumnarInfo(innerCubeName).then((res) => {
                      handleSuccess(res, (data) => {
                        let totalSize = 0
                        if (data[0]) {
                          data[0].forEach(function (segment) {
                            totalSize += segment.storageSize
                            if (segment.rawTableStorageSize) {
                              totalSize += segment.rawTableStorageSize
                            }
                          })
                        }
                        this.$set(this.totalSizeList, innerCubeName, totalSize)
                      })
                    })
                  } else {
                    this.getHbaseInfo(innerCubeName).then((res) => {
                      handleSuccess(res, (data) => {
                        let totalSize = 0
                        data.forEach(function (segment) {
                          totalSize += segment.tableSize
                          if (segment.rawTableStorageSize) {
                            totalSize += segment.rawTableStorageSize
                          }
                        })
                        this.$set(this.totalSizeList, innerCubeName, totalSize)
                      })
                    })
                  }
                })
              })
            })
          }
        })
      }, (res) => {
        handleError(res)
      })
    },
    drop: function (cube) {
      if (!(cube.segments && cube.segments.length >= 0)) {
        this.$message(this.$t('kylinLang.cube.cubeHasJob'))
        return
      }
      kapConfirm(this.$t('deleteCube')).then(() => {
        this.deleteCube({cubeName: cube.name, project: cube.project}).then((res) => {
          handleSuccess(res, (data) => {
            this.$message({
              type: 'success',
              message: this.$t('deleteSuccessful'),
              duration: 3000
            })
          })
          if (this.isViewCubeMode) {
            this.$emit('removetabs', 'viewcube[view] ' + this.extraoption.cubeName, 'Overview')
            this.$emit('reload', 'cubelList')
          } else {
            this.loadCubesList(this.currentPage - 1)
          }
        }, (res) => {
          handleError(res)
        })
      })
    },
    edit: function (cube) {
      if (cube.status === 'READY') {
        this.$message(this.$t('kylinLang.cube.readyCubeTip'))
        return
      }
      this.$emit('addtabs', 'cube', cube.name, 'cubeEdit', {
        project: cube.project,
        cubeName: cube.name,
        modelName: cube.model,
        isEdit: true,
        cubeStatus: cube.status,
        cubeInstance: cube
      })
    },
    build: function (cube) {
      // if (!(cube.segments && cube.segments.length >= 0)) {
      //   this.$message(this.$t('kylinLang.cube.cubeHasJob'))
      //   return
      // }
      this.selected_cube = cube
      if (cube.is_streaming) {
        kapConfirm(this.$t('buildCube')).then(() => {
          this.rebuildStreamingCube(cube.name).then((res) => {
            handleSuccess(res, (data) => {
              this.$message({
                type: 'success',
                message: this.$t('kylinLang.common.submitSuccess'),
                duration: 3000
              })
              this.loadCubesList(this.currentPage - 1)
            })
          }, (res) => {
            handleError(res)
          })
        })
      } else {
        if (cube.partitionDateColumn) {
          this.buildCubeFormVisible = true
        } else {
          kapConfirm(this.$t('buildCube')).then(() => {
            let time = {buildType: 'BUILD', startTime: 0, endTime: 0}
            this.rebuildCube({cubeName: cube.name, timeZone: time}).then((res) => {
              handleSuccess(res, (data) => {
                this.$message({
                  type: 'success',
                  message: this.$t('kylinLang.common.submitSuccess'),
                  duration: 3000
                })
                this.loadCubesList(this.currentPage - 1)
              })
            }, (res) => {
              handleError(res)
            })
          })
        }
      }
    },
    resetCubeBuildField: function () {
      this.$refs['buildCubeForm'].$emit('resetBuildCubeForm')
    },
    checkBuildCubeForm: function () {
      this.$refs['buildCubeForm'].$emit('buildCubeFormValid')
    },
    buildCubeValidSuccess: function (data, isFullBuild) {
      let time = {buildType: 'BUILD', startTime: data.start, endTime: data.end}
      this.rebuildCube({cubeName: this.selected_cube.name, timeZone: time}).then((res) => {
        handleSuccess(res, (data) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.submitSuccess'),
            duration: 3000
          })
          this.loadCubesList(this.currentPage - 1)
        })
      }, (res) => {
        handleError(res)
      })
      this.buildCubeFormVisible = false
    },
    refresh: function (cube) {
      if (!(cube.segments && cube.segments.length >= 0)) {
        this.$message(this.$t('kylinLang.cube.cubeHasJob'))
        return
      }
      this.selected_cube = cube
      this.refreshCubeFormVisible = true
    },
    checkRefreshCubeForm: function () {
      this.$refs['refreshCubeForm'].$emit('refreshCubeFormValid')
    },
    refreshCubeValidSuccess: function (data, noFullBuild) {
      if (!noFullBuild) {
        data.date_range_end = 0
      }
      let time = {buildType: 'REFRESH', startTime: data.date_range_start, endTime: data.date_range_end}
      this.rebuildCube({cubeName: this.selected_cube.name, timeZone: time}).then((res) => {
        handleSuccess(res, (data) => {
          this.$message({
            type: 'success',
            message: this.$t('refreshSuccessful'),
            duration: 3000
          })
          this.loadCubesList(this.currentPage - 1)
        })
      }, (res) => {
        handleError(res)
      })
      this.refreshCubeFormVisible = false
    },
    merge: function (cube) {
      if (!(cube.segments && cube.segments.length >= 0)) {
        this.$message(this.$t('kylinLang.cube.cubeHasJob'))
        return
      }
      this.selected_cube = cube
      this.mergeCubeFormVisible = true
    },
    checkMergeCubeForm: function () {
      this.$refs['mergeCubeForm'].$emit('mergeCubeFormValid')
    },
    mergeCubeValidSuccess: function (data) {
      let time = {buildType: 'MERGE', startTime: data.date_range_start, endTime: data.date_range_end}
      this.rebuildCube({cubeName: this.selected_cube.name, timeZone: time}).then((res) => {
        handleSuccess(res, (data) => {
          this.$message({
            type: 'success',
            message: this.$t('mergeSuccessful'),
            duration: 3000
          })
          this.loadCubesList(this.currentPage - 1)
        })
      }, (res) => {
        handleError(res)
      })
      this.mergeCubeFormVisible = false
    },
    enable: function (cubeName) {
      kapConfirm(this.$t('enableCube')).then(() => {
        this.enableCube(cubeName).then((res) => {
          handleSuccess(res, (data) => {
            this.$message({
              type: 'success',
              message: this.$t('enableSuccessful'),
              duration: 3000
            })
            this.loadCubesList(this.currentPage - 1)
          })
        }, (res) => {
          handleError(res)
        })
      })
    },
    disable: function (cubeName) {
      kapConfirm(this.$t('disableCube')).then(() => {
        this.disableCube(cubeName).then((res) => {
          handleSuccess(res, (data) => {
            this.$message({
              type: 'success',
              message: this.$t('disableSuccessful'),
              duration: 3000
            })
            this.loadCubesList(this.currentPage - 1)
          })
        }, (res) => {
          handleError(res)
        })
      })
    },
    purge: function (cubeName) {
      kapConfirm(this.$t('purgeCube')).then(() => {
        this.purgeCube(cubeName).then((res) => {
          handleSuccess(res, (data) => {
            this.$message({
              type: 'success',
              message: this.$t('purgeSuccessful'),
              duration: 3000
            })
            this.loadCubesList(this.currentPage - 1)
          })
        }, (res) => {
          handleError(res)
        })
      })
    },
    clone: function (cube) {
      this.selected_cube = cube
      this.cloneCubeFormVisible = true
    },
    checkCloneCubeForm: function () {
      this.$refs['cloneCubeForm'].$emit('cloneCubeFormValid')
    },
    cloneCubeValidSuccess: function (data) {
      this.cloneCube(data).then((res) => {
        handleSuccess(res, (data) => {
          this.$message({
            type: 'success',
            message: this.$t('cloneSuccessful'),
            duration: 3000
          })
          this.loadCubesList(this.currentPage - 1)
        })
      }, (res) => {
        handleError(res)
      })
      this.cloneCubeFormVisible = false
    },
    editCubeDesc: function (cube) {
      this.$emit('addtabs', 'edit', cube.name, 'cubeMetadata', {
        project: cube.project,
        cubeName: cube.name,
        type: 'edit'
      })
    },
    view: function (cube) {
      this.$emit('addtabs', 'view', cube.name, 'cubeMetadata', {
        project: cube.project,
        cubeName: cube.name,
        cubeDesc: cube,
        type: 'view'
      })
    },
    backup: function (cubeName) {
      kapConfirm(this.$t('backupCube')).then(() => {
        this.backupCube(cubeName).then((result) => {
          this.$message({
            type: 'success',
            message: this.$t('backupSuccessful')
          })
          this.loadCubesList(this.currentPage - 1)
        }, (res) => {
          handleError(res)
        })
      })
    },
    currentChange: function (value) {
      this.currentPage = value
      this.loadCubesList(value - 1)
    },
    changeTab: function (tab) {
      if (tab.$data.index === '1') {
        tab.$children[0].loadCubeSql()
      }
      if (tab.$data.index === '3') {
        tab.$children[0].loadSegments()
      }
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
    hasSomePermissionOfProject (project) {
      var projectId = this.getProjectIdByName(project)
      return hasPermission(this, projectId, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
    },
    hasQueryPermissionOfProject (project) {
      var projectId = this.getProjectIdByName(project)
      return hasPermission(this, projectId, permissions.READ.mask)
    },
    hasOperationPermissionOfProject (project) {
      var projectId = this.getProjectIdByName(project)
      return hasPermission(this, projectId, permissions.OPERATION.mask)
    },
    loadAllModels () {
      this.loadModels({pageSize: 10000, pageOffset: 0, projectName: this.selected_project || null}).then((res) => {
        handleSuccess(res, (data) => {
          if (data && data.models) {
            this.allModels = data.models.filter((mo) => {
              return mo.is_draft === false
            })
          }
        })
      })
    }
  },
  created () {
    this.loadCubesList(0)
    this.loadAllModels()
  },
  computed: {
    modelsList () {
      var allModels = objectClone(this.allModels)
      allModels.push({name: 'ALL'})
      return allModels
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    },
    isViewCubeMode () {
      return !!(this.extraoption && this.extraoption.cubeName)
    }
  },
  locales: {
    'en': {name: 'Name', model: 'Model', status: 'Status', cubeSize: 'Cube Size', sourceTableSize: 'Source Table Size: ', expansionRate: 'Expansion Rate: ', sourceRecords: 'Source Records', lastBuildTime: 'Last Build Time', owner: 'Owner', createTime: 'Create Time', actions: 'Actions', drop: 'Drop', edit: 'Edit', build: 'Build', merge: 'Merge', refresh: 'Refresh', enable: 'Enable', purge: 'Purge', clone: 'Clone', disable: 'Disable', editCubeDesc: 'Edit CubeDesc', viewCube: 'View Cube', backup: 'Backup', storage: 'Storage', updateTime: 'Update Time', cancel: 'Cancel', yes: 'Yes', tip: 'Tip', deleteSuccessful: 'Delete the cube successful!', deleteCube: 'Once it\'s deleted, your cube\'s metadata and data will be cleaned up and can\'t be restored back. ', enableCube: 'Are you sure to enable the cube? Please note: if cube schema is changed in the disabled period, all segments of the cube will be discarded due to data and schema mismatch.', enableSuccessful: 'Enable the cube successful!', disableCube: 'Are you sure to disable the cube?', disableSuccessful: 'Disable the cube successful!', purgeCube: 'Are you sure to purge the cube? ', purgeSuccessful: 'Purge the cube successful!', backupCube: 'Are you sure to backup ?', backupSuccessful: 'Backup the cube successful!', buildCube: 'Are you sure to start the build?', buildSuccessful: 'Build the cube successful!', cubeBuildConfirm: 'CUBE BUILD CONFIRM', cubeRefreshConfirm: 'CUBE Refresh Confirm', refreshSuccessful: 'Refresh the cube successful!', cubeMergeConfirm: 'CUBE Merge Confirm', mergeSuccessful: 'Merge the cube successful!', cubeCloneConfirm: 'CUBE Clone Confirm', cloneSuccessful: 'Clone the cube successful!', chooseModel: 'choose model to filter', verifyModelTip1: '1. This function will help you to verify if the cube can answer following SQL statements.', verifyModelTip2: '2. Multiple SQL statements will be separated by ";".', validFail: 'Uh oh, some SQL went wrong. Click the failed SQL to learn why it didn\'t work and how to refine it.', validSuccess: 'Great! All SQL can perfectly work on this cube.'},
    'zh-cn': {name: '名称', model: '模型', status: '状态', cubeSize: '存储空间', sourceTableSize: '源表大小：', expansionRate: '膨胀率：', sourceRecords: '源数据条目', lastBuildTime: '最后构建时间', owner: '所有者', createTime: '创建时间', actions: '操作', drop: '删除', edit: '编辑', build: '构建', merge: '合并', refresh: '刷新', enable: '启用', purge: '清理', clone: '克隆', disable: '禁用', editCubeDesc: '编辑 Cube详细信息', viewCube: '查看 Cube', backup: '备份', storage: '存储', updateTime: '更新时间', tip: '提示', cancel: '取消', yes: '确定', deleteSuccessful: '删除cube成功!', deleteCube: '删除后, Cube定义及数据会被清除, 且不能恢复.', enableCube: '请注意, 如果在禁用期间, Cube的元数据发生改变, 所有的Segment会被丢弃. 确定要启用Cube?', enableSuccessful: '启用cube成功!', disableCube: '确定要禁用此Cube? ', disableSuccessful: '禁用cube成功!', purgeCube: '确定要清空此Cube?', purgeSuccessful: '清理cube成功!', backupCube: '确定要备份此Cube? ', backupSuccessful: '备份cube成功!', buildCube: '确定要构建此Cube?', buildSuccessful: '构建cube成功!', cubeBuildConfirm: 'Cube构建确认', cubeRefreshConfirm: 'Cube刷新确认', refreshSuccessful: '刷新Cube成功!', cubeMergeConfirm: 'Cube合并确认', mergeSuccessful: '合并Cube成功!', cubeCloneConfirm: 'Cube克隆确认', cloneSuccessful: '克隆Cube成功!', chooseModel: '选择model过滤', verifyModelTip1: '1. 系统将帮助您检验以下SQL是否能被本Cube回答。', verifyModelTip2: '2. 输入多条SQL语句时将以“；”作为分隔。', validFail: '有无法运行的SQL查询。请点击未验证成功的SQL，获得具体原因与修改建议。', validSuccess: '所有SQL都能被本Cube验证。'}
  }
}
</script>
<style lang="less">
  @import '../../less/config.less';
  .cube-list {
    .null_pic_box{
      text-align:center;
      padding-top:100px;
      .null_pic_2{
        width:150px;
      }
    }
    .line{
      background: #292b38;
      margin-left: -20px;
      margin-right: -20px;
    }
    .suggestBox{
      border-radius: 4px;
      background-color: #20222e;
      padding: 10px 10px;
      font-size: 12px;
      color:#9DA3B3;
      max-height: 200px;
      overflow-y: auto;
      h3{
        margin-bottom:10px;
        // margin-top: 10px;
        color:#F44236;
        font-size: 12px;
      }
      p{
        margin-bottom: 10px;
      }
    }
    margin-left: 30px;
    margin-right: 30px;
    .el-form-item__label{
      float: none;
    }
    .el-icon-arrow-right{
      color: #d4d7e3;
    }
    .el-table {
      .is_draft {
        td {
          background: #515770!important;
        }
        &>td:nth-child(2) {
         &>div{
          height: 100%;
          line-height: 40px;
          background-image: url('../../assets/img/draft.png');
          background-repeat: no-repeat;
          background-size: 20px;
          background-position: 90% 80%;
         }
        }
      }
    }
    .cubeSearch {
      margin-bottom: 5px;
      .el-input {
        width:200px;
        input{
          border-color:#393e53;
        }
      }
    }
    .el-table {
      font-size: 12px;
      tr th:first-child,
      tr td:first-child {
        border-right: 0;
      }
      .cell {
        padding: 0 8px;
      }
      .el-table .caret-wrapper {
        width: 14px;
      }
    }
    .demo-table-expand {
      font-size: 0;
    }
    .demo-table-expand label {
      width: 90px;
      color: #99a9bf;
    }
    .demo-table-expand .el-form-item {
      margin-right: 0;
      margin-bottom: 0;
      width: 50%;
    }
    .el-tag--success {
      background-color: #13ce66;
    }
    .el-tag {
      color: #fff;
    }
    .el-tag--danger {
        background-color: #ff4949;
    }
  }
  .el-tag--gray{
    // background: yellow;
    color: #000!important;
  }
  #cube-list-table{
    .el-tabs--border-card{
      background: @tableBC;
    }
  }
</style>

