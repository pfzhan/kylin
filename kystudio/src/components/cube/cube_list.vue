<template>
<div class="ksd-border-tab cube-list ksd-mrl-20">
  <el-row class="ksd-mt-20" v-if="!isViewCubeMode && cubesList.length > 0 || (!cubesList.length && showSearchResult)">
    <el-button size="medium" type="primary" plain icon="el-icon-plus" class="ksd-fleft" v-if="isAdmin || hasSomePermissionOfProject(selected_project)" @click.native="addCube">{{$t('kylinLang.common.cube')}}</el-button>
     <el-input size="medium" v-model="filterCube" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'" class="ksd-mb-10 ksd-ml-10 ksd-fright show-search-btn" @input="filterChange" :placeholder="$t('kylinLang.common.pleaseFilterByCubeName')" style="width:200px;"></el-input>
    <el-select size="medium" v-model="currentModel" class="ksd-ml-20 ksd-fright" :placeholder="$t('chooseModel')">
      <el-option value="ALL" :label="$t('kylinLang.common.all')"></el-option>
      <el-option
        v-for="item in modelsList"
        :key="item.name"
        :label="item.name"
        :value="item.name">
      </el-option>
    </el-select>
  </el-row>

  <el-table class="cube-list-table" v-if="cubesList&&cubesList.length || (!cubesList.length && showSearchResult)"
    :data="cubesList"
    :default-expand-all="isViewCubeMode"
    :row-class-name="showRowClass"
    @sort-change="sortCubesList"
    border
    style="width: 100%">
    <el-table-column type="expand" min-width="30">
      <template slot-scope="props">
        <el-tabs activeName="first" class="el-tabs--default" @tab-click="changeTab">
          <el-tab-pane label="Overview" name="first" v-if="!props.row.is_draft">
            <cube_desc_view :cube="props.row" :index="props.$index"></cube_desc_view>
          </el-tab-pane>
          <el-tab-pane label="Segments" name="second" v-if="!props.row.is_draft">
            <segments :cube="props.row" v-on:addSegTabs="addSegTabs"></segments>
          </el-tab-pane>
          <el-tab-pane label="SQL Patterns" name="third" v-if="!props.row.is_draft">
            <show_sql :cube="props.row"></show_sql>
          </el-tab-pane>
          <el-tab-pane label="JSON" name="forth" v-if="!props.row.is_draft">
            <show_json :json="props.row.desc"></show_json>
          </el-tab-pane>
        </el-tabs>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('name')"
      sortable
      show-overflow-tooltip
      min-width="130"
      prop="name">
      <template slot-scope="scope">
        <span v-if="scope.row.is_draft" class="draft-cube ksd-mr-4">[draft]</span> {{scope.row.name}}
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('model')"
      show-overflow-tooltip
      prop="model">
    </el-table-column>
    <el-table-column
      :label="$t('status')"
      min-width="110"
      prop="status">
      <template slot-scope="scope">
        <el-tag size="small" :type="scope.row.status === 'DISABLED' ? 'danger' : scope.row.status === 'DESCBROKEN'? 'info' : 'success'">{{scope.row.status}}</el-tag>
      </template>
    </el-table-column>
    <el-table-column
      min-width="100"
      :label="$t('cubeSize')"
      prop="total_storage_size_kb">
      <template slot-scope="scope">
        <el-tooltip class="item" effect="dark" placement="top">
          <div slot="content">
            {{$t('sourceTableSize')}}{{scope.row.input_records_size|dataSize}}<br/>
            {{$t('expansionRate')}}{{(scope.row.input_records_size>0? scope.row.total_storage_size_kb*1024/scope.row.input_records_size : 0) * 100 | number(2)}}%
          </div>
          <span class="ksd-mr-4 ksd-fright">{{scope.row.total_storage_size_kb * 1024 | dataSize}}</span>
        </el-tooltip>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('sourceRecords')"
      show-overflow-tooltip
      min-width="100"
      prop="input_records_count">
      <template slot-scope="scope">
        <span class="ksd-mr-4 ksd-fright">{{scope.row.input_records_count | readableNumber}} </span>
      </template>
    </el-table-column>
    <el-table-column
      show-overflow-tooltip
      min-width="150"
      :label="$t('lastBuildTime')">
      <template slot-scope="scope">
        <span v-if="scope.row.segments[scope.row.segments.length-1]">{{scope.row.buildGMTTime}}</span>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('owner')"
      show-overflow-tooltip
      min-width="90"
      prop="owner">
    </el-table-column>
    <el-table-column
      :label="$t('updateTime')"
      sortable
      min-width="150"
      show-overflow-tooltip
      prop="lastModifiedGMT">
    </el-table-column>
    <el-table-column
       min-width="120"
      :label="$t('actions')">
      <template slot-scope="scope">
        <span v-if="!(isAdmin || hasSomePermissionOfProject(scope.row.project) || hasOperationPermissionOfProject(selected_project))"> N/A</span>
        <common-tip :content="$t('kylinLang.common.edit')">
          <i class="el-icon-ksd-table_edit ksd-fs-16" v-show="isAdmin || hasSomePermissionOfProject(selected_project)" @click="edit(scope.row)"></i>
        </common-tip>
        <el-dropdown trigger="click" v-show="isAdmin || hasSomePermissionOfProject(scope.row.project) || hasOperationPermissionOfProject(selected_project) ">
          <common-tip :content="$t('kylinLang.common.moreActions')">
            <i class="el-icon-ksd-table_others ksd-ml-10 ksd-fs-16"></i>
          </common-tip>
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item @click.native="openValidateSql(scope.row)" v-show="!scope.row.is_draft">{{$t('kylinLang.common.verifySql')}}</el-dropdown-item>

            <el-dropdown-item v-show="scope.row.status !=='READY' && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="drop(scope.row)">{{$t('drop')}}</el-dropdown-item>

<!--             <el-dropdown-item @click.native="edit(scope.row)" v-show="isAdmin || hasSomePermissionOfProject(selected_project)">{{$t('edit')}}</el-dropdown-item> -->
<!--             <el-dropdown-item v-show="scope.row.status !== 'DESCBROKEN' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="manage(scope.row)">{{$t('manage')}}</el-dropdown-item> -->
            <el-dropdown-item v-show="scope.row.status !== 'DESCBROKEN' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project)) " @click.native="build(scope.row)">{{$t('build')}}</el-dropdown-item>

            <el-dropdown-item v-show="scope.row.status=='DISABLED' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="enable(scope.row.name)">{{$t('enable')}}</el-dropdown-item>

            <el-dropdown-item v-show="scope.row.status ==='READY' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="disable(scope.row.name)">{{$t('disable')}}</el-dropdown-item>


            <el-dropdown-item v-show="scope.row.status==='DISABLED' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="purge(scope.row)">{{$t('purge')}}</el-dropdown-item>

            <el-dropdown-item v-show="scope.row.status!=='DESCBROKEN' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="clone(scope.row)">{{$t('clone')}}</el-dropdown-item>

<!--             <el-dropdown-item @click.native="view(scope.row)" v-show="isAdmin||hasSomePermissionOfProject(selected_project)" divided>{{$t('viewCube')}}</el-dropdown-item> -->
            <el-dropdown-item divided @click.native="backup(scope.row.name)" v-show="!scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project))  ">{{$t('backup')}}</el-dropdown-item>

            <el-dropdown-item @click.native="editCubeDesc(scope.row)" v-show="scope.row.status==='DISABLED' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))">{{$t('editCubeDesc')}}</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </template>
    </el-table-column>
  </el-table>
   <pager ref="pager" :perPageSize="10"  :totalSize="totalCubes"  v-on:handleCurrentChange='currentChange' ></pager>


  <div class="ksd-null-pic-text" v-if="!(cubesList && cubesList.length) && !showSearchResult">
      <img src="../../assets/img/no_cube.png">
      <p>{{$t('kylinLang.cube.noCube')}}</p>
      <div>
      <el-button size="medium" type="primary" icon="el-icon-plus" v-if="isAdmin || hasPermissionOfProject()" @click="addCube">{{$t('kylinLang.common.cube')}}</el-button>
       </div>
    </div>

  <el-dialog :title="'Cube [' + (selected_cube.name.length > 18? selected_cube.name.substring(0,16) + '...' :  selected_cube.name) + '] ' + $t('cubeBuildConfirm')" :visible.sync="buildCubeFormVisible" :close-on-press-escape="false" :close-on-click-modal="false" @close="resetCubeBuildField" width="440px" :append-to-body="true">
    <build_cube :cubeDesc="selected_cube" :formVisible="buildCubeFormVisible" ref="buildCubeForm" v-on:validSuccess="buildCubeValidSuccess"></build_cube>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="buildCubeFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button size="medium" type="primary" plain @click="checkBuildCubeForm">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>

  <el-dialog :title="'Cube [' + (selected_cube.name && selected_cube.name.length > 18? selected_cube.name.substring(0,16) + '...' :  selected_cube.name) + '] ' + $t('cubeBuildConfirm')" :visible.sync="buildFullCubeVisible" :close-on-press-escape="false" :close-on-click-modal="false" width="440px">
    {{selected_cube.name}}{{$t('cubeFullBuild')}}
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="buildFullCubeVisible = false">{{$t('cancel')}}</el-button>
      <el-button size="medium" type="primary" plain @click="buildFullCube">{{$t('kylinLang.common.continue')}}</el-button>
    </div>
  </el-dialog>

  <el-dialog :title="'Cube [' + (selected_cube.name.length > 18? selected_cube.name.substring(0,16) + '...' :  selected_cube.name) + '] ' + $t('cubePurgeConfirm')" :visible.sync="purgeCubeFormVisible" @close="resetCubePurgeField" :close-on-press-escape="false" :close-on-click-modal="false" width="440px" :append-to-body="true">
    <purge_cube :cubeDesc="selected_cube" ref="purgeCubeForm" v-on:validSuccess="purgeCubeValidSuccess"></purge_cube>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="purgeCubeFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button size="medium" type="primary" plain @click="checkPurgeCubeForm">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>

  <el-dialog width="440px" :title="$t('cubeCloneConfirm')" :visible.sync="cloneCubeFormVisible">
    <clone_cube :cubeDesc="selected_cube" ref="cloneCubeForm" v-on:validSuccess="cloneCubeValidSuccess"></clone_cube>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="cloneCubeFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button size="medium" type="primary" plain @click="checkCloneCubeForm">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>

   <!-- 添加cube -->

    <el-dialog width="440px" :title="$t('kylinLang.cube.addCube')" :visible.sync="createCubeVisible" @close="resetCubeForm" class="add-cube" :close-on-click-modal="false" :append-to-body="true">
      <el-form label-position="top" label-width="115px" :model="cubeMeta" :rules="createCubeFormRule" ref="addCubeForm">
        <el-form-item :label="$t('kylinLang.cube.cubeName')" prop="cubeName">
          <span slot="label">{{$t('kylinLang.cube.cubeName')}}
            <common-tip :content="$t('kylinLang.cube.cubeNameTip')" >
              <i class="el-icon-question"></i>
            </common-tip>
          </span>
          <el-input size="medium" v-model="cubeMeta.cubeName" auto-complete="off"></el-input>
        </el-form-item>
        <el-form-item :label="$t('kylinLang.model.modelNameGrid')" prop="modelName">
           <el-select size="medium" v-model="cubeMeta.modelName" :placeholder="$t('kylinLang.common.pleaseSelect')" style="width:100%;">
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
        <el-button size="medium" @click="createCubeVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button size="medium" type="primary" plain @click="createCube" :loading="btnLoading">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-dialog>

    <el-dialog width="780px" :title="$t('kylinLang.common.verifySql')" class="cube-sql" :visible.sync="checkSQLFormVisible" :before-close="sqlClose" :close-on-press-escape="false" :close-on-click-modal="false">
      <p>{{$t('verifyModelTip1')}}</p>
      <p>{{$t('verifyModelTip2')}}</p>
      <div :class="{hasCheck: hasCheck}">
        <kap_editor ref="sqlbox" class="ksd-mt-20" height="200" width="100%" lang="sql" theme="chrome" v-model="sqlString" dragbar="#393e53">
        </kap_editor>
      </div>
      <div class="ksd-mt-16">
        <el-button size="small" type="primary" :disabled="sqlString === ''" :loading="checkSqlLoadBtn" @click="validateSql" >{{$t('kylinLang.common.verify')}}</el-button>
        <el-button size="small" plain v-show="checkSqlLoadBtn" @click="cancelCheckSql">{{$t('kylinLang.common.cancel')}}</el-button>
      </div>
      <div v-if="currentSqlErrorMsg && currentSqlErrorMsg.length || successMsg || errorMsg" class="suggest-box">
        <div v-if="successMsg">
          <el-alert
            :title="successMsg"
            show-icon
            :closable="false"
            type="success">
          </el-alert>
        </div>
        <div v-if="errorMsg">
         <el-alert
            title=""
            show-icon
            :closable="false"
            type="error">
            <div v-html="errorMsg.replace(/\n/g,'<br/>')"></div>
          </el-alert>
        </div>
        <div class="ksd-mt-30 sql-suggest" v-if="currentSqlErrorMsg.length > 0">
          <div class="ksd-fs-14" v-for="(sug, index) in currentSqlErrorMsg">
            <span class="ksd-pb-10">{{$t('kylinLang.common.errorDetail')}}</span>
            <p class="ksd-mb-20">{{sug.incapableReason}}</p>
            <span class="ksd-pb-10">{{$t('kylinLang.common.suggest')}}</span>
            <p v-html="sug.suggestion"></p>
            <div class="ky-dashed ksd-mtb-20" v-if="index < currentSqlErrorMsg.length - 1"></div>
          </div>
        </div>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" type="primary" plain :loading="sqlBtnLoading" @click="sqlClose()">{{$t('kylinLang.common.close')}}</el-button>
      </span>
    </el-dialog>

    <el-dialog width="780px" :title="$t('kylinLang.common.cubeDesc')" :visible.sync="checkMetadataFormVisible" :close-on-press-escape="false" :close-on-click-modal="false">
      <cube_meta :cubeDesc="selected_cube" ref="cubeDescForm" v-if="checkMetadataFormVisible" v-on:validSuccess="cubeDescValidSuccess"></cube_meta>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" plain :loading="sqlBtnLoading" @click="checkMetadataFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button size="medium" type="primary" plain :loading="sqlBtnLoading" @click="descEditSub()">{{$t('kylinLang.common.submit')}}</el-button>
      </span>
    </el-dialog>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import { permissions, NamedRegex } from '../../config'
import showJson from './json'
import showSql from './sql'
import segments from './segments'
import cubeDescView from './view/cube_desc_view'
import buildCube from './dialog/build_cube'
import cloneCube from './dialog/clone_cube'
import purgeCube from './dialog/purge_cube'
import cubeMeta from './cube_metadata'
import accessEdit from '../project/access_edit'
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
      buildFullCubeVisible: false,
      cloneCubeFormVisible: false,
      purgeCubeFormVisible: false,
      checkMetadataFormVisible: false,
      selected_cube: {
        name: ''
      },
      searchLoading: false,
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
      },
      filter: {
        pageOffset: 0,
        pageSize: 10,
        projectName: localStorage.getItem('selected_project'),
        cubeName: null,
        modelName: 'ALL',
        sortBy: 'update_time',
        reverse: true,
        exactMatch: false
      },
      showSearchResult: false
    }
  },
  components: {
    'show_json': showJson,
    'show_sql': showSql,
    'segments': segments,
    'cube_desc_view': cubeDescView,
    'build_cube': buildCube,
    'clone_cube': cloneCube,
    'purge_cube': purgeCube,
    'access_edit': accessEdit,
    'cube_meta': cubeMeta
  },
  watch: {
    currentModel (val) {
      this.loadCubesList(0)
      if (val !== 'ALL') {
        this.showSearchResult = true
      } else {
        this.showSearchResult = false
      }
    }
  },
  methods: {
    ...mapActions({
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
      verifyCubeSql: 'VERIFY_CUBE_SQL',
      updateCube: 'UPDATE_CUBE',
      draftCube: 'DRAFT_CUBE'
    }),
    sortCubesList (column, prop, order) {
      let _column = column.column
      if (order === 'ascending') {
        this.filter.reverse = false
      } else {
        this.filter.reverse = true
      }
      if (_column.label === this.$t('name')) {
        this.filter.sortBy = 'cube_name'
      } else if (_column.label === this.$t('updateTime')) {
        this.filter.sortBy = 'update_time'
      }
      this.loadCubesList(this.currentPage - 1)
    },
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
      var editor = this.$refs.sqlbox && this.$refs.sqlbox.$refs.kapEditor.editor || ''
      editor && editor.removeListener('change', this.editerChangeHandle)
      this.renderEditerRender(editor)
      this.errorMsg = false
      this.checkSqlLoadBtn = true
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
              // this.errorMsg = ''
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
        this.searchLoading = true
        this.loadCubesList(this.currentPage - 1).then(() => {
          this.searchLoading = false
          if (this.filterCube) {
            this.showSearchResult = true
          } else {
            this.showSearchResult = false
          }
        }, () => {
          this.searchLoading = false
        })
      }, 1000)
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
      return o.row.is_draft ? 'is-draft' : ''
    },
    loadCubesList: function (curPage) {
      let cubesNameList = []
      this.filter.pageOffset = curPage
      if (localStorage.getItem('selected_project')) {
        this.filter.projectName = localStorage.getItem('selected_project')
      }
      if (this.currentModel && this.currentModel !== 'ALL') {
        this.$set(this.filter, 'modelName', this.currentModel)
      } else {
        delete this.filter.modelName
      }
      if (this.filterCube) {
        this.filter.cubeName = this.filterCube
      } else {
        this.filter.cubeName = null
      }
      if (this.extraoption && this.extraoption.cubeName) {
        this.filter.cubeName = this.extraoption.cubeName
      }
      let timeZone = localStorage.getItem('GlobalSeverTimeZone') ? localStorage.getItem('GlobalSeverTimeZone') : ''
      return this.getCubesList(this.filter).then((res) => {
        handleSuccess(res, (data) => {
          this.totalCubes = data.size
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
      if (cube.status === 'DISABLED' && cube.segments.length > 0) {
        this.$message({
          showClose: true,
          duration: 0,
          message: this.$t('kylinLang.cube.disableCubeEditTip', {cubename: cube.name})
        })
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
        this.$confirm(this.$t('buildCube'), 'Cube [' + (this.selected_cube.name.length > 18 ? this.selected_cube.name.substring(0, 16) + '...' : this.selected_cube.name) + '] ' + this.$t('cubeBuildConfirm')).then(() => {
          this.rebuildStreamingCube(cube.name).then((res) => {
            handleSuccess(res, (data) => {
              this.$message({
                type: 'success',
                message: this.$t('kylinLang.common.submitSuccess'),
                showClose: true
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
          // this.buildFullCubeVisible = true
          this.$confirm(this.selected_cube.name + this.$t('cubeFullBuild'), 'Cube [' + (this.selected_cube.name.length > 18 ? this.selected_cube.name.substring(0, 16) + '...' : this.selected_cube.name) + '] ' + this.$t('cubeBuildConfirm'), {confirmButtonText: this.$t('kylinLang.common.continue')}).then(() => {
            let time = {buildType: 'BUILD', startTime: 0, endTime: 0}
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
          })
        }
      }
    },
    buildFullCube: function () {
      let time = {buildType: 'BUILD', startTime: 0, endTime: 0}
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
      this.buildFullCubeVisible = false
    },
    resetCubeBuildField: function () {
      this.$refs['buildCubeForm'].$emit('resetBuildCubeForm')
    },
    checkBuildCubeForm: function () {
      this.$refs['buildCubeForm'].$emit('buildCubeFormValid')
    },
    buildCubeValidSuccess: function (data, isFullBuild) {
      let time = {buildType: 'BUILD', startTime: data.start, endTime: data.end, mpValues: data.mpValues}
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
    manage: function (cube) {
      this.$emit('addtabs', 'manage', cube.name, 'cubeManage', {
        project: cube.project,
        cubeName: cube.name,
        cubeDesc: cube,
        type: 'manage'
      })
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
    purge: function (cube) {
      this.selected_cube = cube
      this.purgeCubeFormVisible = true
    },
    checkPurgeCubeForm: function () {
      this.$refs['purgeCubeForm'].$emit('purgeCubeFormValid')
    },
    purgeCubeValidSuccess: function (data) {
      this.purgeCube({name: this.selected_cube.name, values: data.mpValues}).then((res) => {
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
      this.purgeCubeFormVisible = false
    },
    resetCubePurgeField: function () {
      this.$refs['purgeCubeForm'].$emit('resetPurgeCubeForm')
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
      this.selected_cube = cube
      this.checkMetadataFormVisible = true
    },
    descEditSub: function () {
      this.$refs['cubeDescForm'].$emit('descFormValid')
    },
    cubeDescValidSuccess: function (data) {
      this[data.type](data.json).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.saveSuccess')
          })
          this.checkMetadataFormVisible = false
        })
      }, (res) => {
        handleError(res)
      })
    },
    resetCubeMetaField: function () {
      this.checkMetadataFormVisible = false
    },
    backup: function (cubeName) {
      kapConfirm(this.$t('backupCube')).then(() => {
        this.backupCube(cubeName).then((result) => {
          handleSuccess(result, (data, code, status, msg) => {
            this.$message({
              type: 'success',
              message: this.$t('kylinLang.common.backupSuccessTip') + data,
              showClose: true,
              duration: 0
            })
          })
          this.loadCubesList(this.currentPage - 1)
        }, (res) => {
          handleError(res)
        })
      })
    },
    addSegTabs: function (data) {
      this.$emit('addtabs', 'Segment', 'Segment[' + data.name.substr(0, 14) + '...]', 'cubeSegment', data)
    },
    currentChange: function (value) {
      this.currentPage = value
      this.loadCubesList(value - 1)
    },
    changeTab: function (tab) {
      if (tab.label === 'SQL Patterns') {
        tab.$children[0].loadCubeSql()
      }
      if (tab.label === 'Segments') {
        tab.$children[0].loadSegments()
      }
      if (tab.label === 'JSON') {
        tab.$children[0].createFlower()
      }
    },
    hasSomePermissionOfProject () {
      return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
    },
    hasQueryPermissionOfProject () {
      return hasPermission(this, permissions.READ.mask)
    },
    hasOperationPermissionOfProject () {
      return hasPermission(this, permissions.OPERATION.mask)
    },
    hasPermissionOfProject () {
      return hasPermission(this, permissions.ADMINISTRATION.mask, permissions.MANAGEMENT.mask)
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
      var models = objectClone(this.allModels)
      return models
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    },
    isViewCubeMode () {
      return !!(this.extraoption && this.extraoption.cubeName)
    }
  },
  locales: {
    'en': {name: 'Name', model: 'Model', status: 'Status', cubeSize: 'Cube Size', sourceTableSize: 'Source Table Size: ', expansionRate: 'Expansion Rate: ', sourceRecords: 'Source Records', lastBuildTime: 'Last Build Time', owner: 'Owner', createTime: 'Create Time', actions: 'Actions', drop: 'Drop', edit: 'Edit', build: 'Build', merge: 'Merge', refresh: 'Refresh', enable: 'Enable', purge: 'Purge', clone: 'Clone', disable: 'Disable', editCubeDesc: 'Edit CubeDesc', viewCube: 'View Cube', backup: 'Backup', segments: 'Segments', updateTime: 'Update Time', cancel: 'Cancel', yes: 'Ok', tip: 'Tip', deleteSuccessful: 'Delete the cube successfully.', deleteCube: 'Once it\'s deleted, your cube\'s metadata and data will be cleaned up and can\'t be restored back. ', enableCube: 'Are you sure to enable the cube? Please note: if cube schema is changed in the disabled period, all segments of the cube will be discarded due to data and schema mismatch.', enableSuccessful: 'Enable the cube successfully.', disableCube: 'Are you sure to disable the cube?', disableSuccessful: 'Disable the cube successfully.', purgeSuccessful: 'Purge the cube successfully.', backupCube: 'Are you sure to backup this cube ?', buildCube: 'Are you sure to start the build?', buildSuccessful: 'Build the cube successfully.', cubeBuildConfirm: 'Build Confirm', cubeCloneConfirm: 'Cube Clone Confirm', cloneSuccessful: 'Clone the cube successfully.', chooseModel: 'choose model to filter', verifyModelTip1: '1. This function will help you to verify if the cube can answer following SQL statements.', verifyModelTip2: '2. Multiple SQL statements will be separated by ";".', validFail: 'Uh oh, some SQL went wrong. Click the failed SQL to learn why it didn\'t work and how to refine it.', validSuccess: 'Great! All SQL can perfectly work on this cube.', cubeFullBuild: ' will be full build, are you sure to continue?', cubePurgeConfirm: 'Purge Confirm'},
    'zh-cn': {name: '名称', model: '模型', status: '状态', cubeSize: '存储空间', sourceTableSize: '源表大小：', expansionRate: '膨胀率：', sourceRecords: '源数据条目', lastBuildTime: '最后构建时间', owner: '所有者', createTime: '创建时间', actions: '操作', drop: '删除', edit: '编辑', build: '构建', merge: '合并', refresh: '刷新', enable: '启用', purge: '清理', clone: '克隆', disable: '禁用', editCubeDesc: '编辑 Cube详细信息', viewCube: '查看 Cube', backup: '备份', segments: 'Segments', updateTime: '更新时间', tip: '提示', cancel: '取消', yes: '确定', deleteSuccessful: '删除cube成功!', deleteCube: '删除后, Cube定义及数据会被清除, 且不能恢复.', enableCube: '请注意, 如果在禁用期间, Cube的元数据发生改变, 所有的Segment会被丢弃. 确定要启用Cube?', enableSuccessful: '启用cube成功。', disableCube: '确定要禁用此Cube? ', disableSuccessful: '禁用cube成功。', purgeSuccessful: '清理cube成功。', backupCube: '确定要备份此Cube? ', buildCube: '确定要构建此Cube?', buildSuccessful: '构建cube成功。', cubeBuildConfirm: '构建确认', cubeRefreshConfirm: 'Cube刷新确认', refreshSuccessful: '刷新Cube成功!', cubeMergeConfirm: 'Cube合并确认', mergeSuccessful: '合并Cube成功。', cubeCloneConfirm: 'Cube克隆确认', cloneSuccessful: '克隆Cube成功。', chooseModel: '选择model过滤', verifyModelTip1: '1. 系统将帮助您检验以下SQL是否能被本Cube回答。', verifyModelTip2: '2. 输入多条SQL语句时将以“;”作为分隔。', validFail: '有无法运行的SQL查询。请点击未验证成功的SQL，获得具体原因与修改建议。', validSuccess: '所有SQL都能被本Cube验证。', cubeFullBuild: '将被全量构建，您确定要继续吗？', cubePurgeConfirm: '清理确认'}
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .cube-list {
    .null_pic_box{
      text-align: center;
      padding-top: 200px;
      .null_pic_2{
        width:120px;
      }
      >div {
        position: relative;
        left: 50%;
        .el-button {
           position:relative;
           left: -38px;
        }
      }
    }
    .cube-sql {
      >p {
        font-size: 12px;
      }
      .suggest-box {
        border-radius: 4px;
        padding: 20px 0px;
        i {
          font-size: 16px;
        }
        .sql-suggest {
          background: @table-stripe-color;
          border: 1px solid @text-secondary-color;
          border-radius: 6px;
          padding: 20px 16px;
          span {
            color: @error-color-1;
          }
          .ky-dashed {
            margin:0 -16px;
            width: 720px;
          }
        }
      }
    }
    .cube-list-table {
      .el-table__expand-column {
        border-right: 0;
        .cell {
          padding-right: 0px;
        }
      }
      tr td i {
        cursor: pointer;
      }
      .draft-cube {
        color: @warning-color-1;
      }
      .is-draft {
        .el-table__expand-icon {
          display: none;
        }
        td:nth-of-type(2) {
          .cell {
            margin-left: -26px;
          }
        }
      }
      .el-table__expanded-cell {
        padding: 20px 30px;
        background-color: @breadcrumbs-bg-color; 
      }
      .el-table__expanded-cell:hover {
        padding: 20px 30px;
        background-color: @breadcrumbs-bg-color!important; 
      }
    }
  }
</style>

