<template>
<div class="paddingbox ksd-border-tab cube-list" style="min-height:800px" id="cube-list">
<img src="../../assets/img/no_cube.png" class="null_pic" v-if="!(cubesList && cubesList.length)" >
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
      tooltip-effect="dark"
      border
      style="width:100%">
      <el-table-column type="expand" width="30">
        <template scope="props">
          <el-tabs activeName="first" class="el-tabs--default" @tab-click="changeTab">
            <el-tab-pane label="Grid" name="first" v-if="!props.row.is_draft">
              <cube_desc_view :extraoption="props.row"></cube_desc_view>
            </el-tab-pane>
            <el-tab-pane label="Segments" name="second" v-if="!props.row.is_draft">
              <segments :cube="props.row" v-on:addVersionTabs="addVersionTabs" v-on:addSegTabs="addSegTabs"></segments>
            </el-tab-pane>
            <el-tab-pane label="SQL" name="third" v-if="!props.row.is_draft">
              <show_sql :cube="props.row"></show_sql>
            </el-tab-pane>
            <el-tab-pane label="JSON" name="forth" v-if="!props.row.is_draft">
              <show_json :json="props.row.desc" ></show_json>
            </el-tab-pane>
          </el-tabs>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('name')"
        sortable
        width="130"
        show-overflow-tooltip
        prop="name">
         <template scope="scope" >
            {{scope.row.name}}
<!--             <el-tooltip class="item" effect="dark" :content="scope.row&&scope.row.name" placement="top">
                <span >{{scope.row.name|omit(24, '...')}}</span>
            </el-tooltip> -->
         </template>
      </el-table-column>
      <el-table-column
        :label="$t('model')"
        show-overflow-tooltip
        sortable
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
              {{$t('expansionRate')}}{{(scope.row.input_records_size>0? scope.row.cube_size*1024/scope.row.input_records_size : 0) * 100 | number(2)}}%
            </div>
            <span>{{scope.row.cube_size*1024 | dataSize}}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('sourceRecords')"
        sortable
        prop="input_records_count">
      </el-table-column>
      <el-table-column
        sortable
        show-overflow-tooltip
        :label="$t('lastBuildTime')">
        <template scope="scope">
          <span>{{scope.row.buildGMTTime}}</span>
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
        :label="$t('createTime')"
        sortable
        show-overflow-tooltip
        prop="createGMTTime">
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
              <el-dropdown-item v-show="scope.row.status !=='READY' && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="drop(scope.row)">{{$t('drop')}}</el-dropdown-item>
              <el-dropdown-item @click.native="edit(scope.row)" v-show="isAdmin || hasSomePermissionOfProject(selected_project)">{{$t('edit')}}</el-dropdown-item>
              <el-dropdown-item v-show="scope.row.status !== 'DESCBROKEN' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project)) " @click.native="build(scope.row)">{{$t('build')}}</el-dropdown-item>
              <el-dropdown-item v-show="scope.row.status=='DISABLED' && !scope.row.is_draft" @click.native="enable(scope.row.name)">{{$t('enable')}}</el-dropdown-item>
              <el-dropdown-item v-show="scope.row.status ==='READY' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="disable(scope.row.name)">{{$t('disable')}}</el-dropdown-item>
              <el-dropdown-item v-show="scope.row.status !== 'DESCBROKEN' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="manage(scope.row)">{{$t('manage')}}</el-dropdown-item>
              <el-dropdown-item v-show="scope.row.status!=='DESCBROKEN' && !scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project))" @click.native="clone(scope.row)">{{$t('clone')}}</el-dropdown-item>
              <el-dropdown-item @click.native="view(scope.row)" v-show="isAdmin" style="border-top:solid 1px rgb(68, 75, 103)">{{$t('viewCube')}}</el-dropdown-item>
              <el-dropdown-item @click.native="backup(scope.row.name)" v-show="!scope.row.is_draft && (isAdmin || hasSomePermissionOfProject(selected_project) || hasOperationPermissionOfProject(selected_project))  ">{{$t('backup')}}</el-dropdown-item>
              <el-dropdown-item v-show="scope.row.status==='DISABLED'&&!scope.row.is_draft" @click.native="editCubeDesc(scope.row)">{{$t('editCubeDesc')}}</el-dropdown-item>
              </el-dropdown-menu>
          </el-dropdown>
        </template>
      </el-table-column>
    </el-table>
    <pager ref="pager"  :totalSize="totalCubes"  v-on:handleCurrentChange='currentChange' ></pager>

    <el-dialog :title="$t('cubeBuildConfirm')" v-model="buildCubeFormVisible">
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

   <!-- 添加cube -->

    <el-dialog class="add-m" title="Add Cube" v-model="createCubeVisible" size="tiny">
      <el-form :model="cubeMeta" :rules="createCubeFormRule" ref="addCubeForm">
        <el-form-item :label="$t('kylinLang.cube.cubeName')" prop="cubeName" style="margin-top: 10px;">
          <span slot="label">{{$t('kylinLang.cube.cubeName')}}
            <common-tip :content="$t('kylinLang.cube.cubeNameTip')" ><icon name="exclamation-circle"></icon></common-tip>
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
import accessEdit from '../project/access_edit'
import { handleSuccess, handleError, transToGmtTime, hasRole, hasPermission, kapConfirm } from '../../util/business'
export default {
  name: 'cubeslist',
  props: ['extraoption'],
  data () {
    return {
      lockST: null,
      btnLoading: false,
      cubesList: [],
      currentPage: 1,
      totalCubes: 0,
      createCubeVisible: false,
      buildCubeFormVisible: false,
      cloneCubeFormVisible: false,
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
          {required: true, message: this.$t('kylinLang.cube.selectModelName'), trigger: 'blur'}
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
    'access_edit': accessEdit
  },
  watch: {
    currentModel (val) {
      this.loadCubesList(0)
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
      cloneCube: 'CLONE_CUBE',
      checkCubeName: 'CHECK_CUBE_NAME_AVAILABILITY',
      backupCube: 'BACKUP_CUBE',
      getCubeSql: 'GET_CUBE_SQL',
      deleteRawTable: 'DELETE_RAW_TABLE',
      deleteScheduler: 'DELETE_SCHEDULER',
      loadModels: 'LOAD_ALL_MODEL',
      loadCubeDesc: 'LOAD_CUBE_DESC',
      getHbaseInfo: 'GET_HBASE_INFO',
      getColumnarInfo: 'GET_COLUMNAR_INFO'
    }),
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
          this.checkCubeName({cubeName: this.cubeMeta.cubeName, project: this.selected_project}).then((res) => {
            this.btnLoading = false
            handleSuccess(res, (data) => {
              if (data && data.size > 0) {
                this.$message({
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
      this.getCubesList(param).then((res) => {
        handleSuccess(res, (data) => {
          this.cubesList = data.cubes.map((p) => {
            p.createGMTTime = p.create_time_utc === 0 ? '' : transToGmtTime(p.create_time_utc, this)
            p.buildGMTTime = p.last_build_time === 0 ? '' : transToGmtTime(p.last_build_time, this)
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
            this.rebuildCube({cubeName: cube.name, para: time}).then((res) => {
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
    checkBuildCubeForm: function () {
      this.$refs['buildCubeForm'].$emit('buildCubeFormValid')
    },
    buildCubeValidSuccess: function (data, isFullBuild) {
      let time = {buildType: 'BUILD', startTime: data.start, endTime: data.end}
      this.rebuildCube({cubeName: this.selected_cube.name, para: time}).then((res) => {
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
    addVersionTabs: function (data) {
      this.$emit('addtabs', 'cubeVersion', data.name + '-' + data.versionDescription, 'cubeView', data)
    },
    addSegTabs: function (data) {
      this.$emit('addtabs', 'Segment', 'Segment[' + data.segmentName.substr(0, 14) + '...]', 'cubeSegment', data)
    },
    currentChange: function (value) {
      this.currentPage = value
      this.loadCubesList(value - 1)
    },
    changeTab: function (tab) {
      if (tab.label === 'SQL') {
        tab.$children[0].loadCubeSql()
      }
      if (tab.label === 'Segments') {
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
          this.allModels = data.models.filter((mo) => {
            return mo.is_draft === false
          })
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
      var models = this.$store.state.model.modelsList.filter((mo) => {
        return mo.is_draft === false
      })
      models.push({name: 'ALL'})
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
    'en': {name: 'Name', model: 'Model', status: 'Status', cubeSize: 'Cube Size', sourceTableSize: 'Source Table Size: ', expansionRate: 'Expansion Rate: ', sourceRecords: 'Source Records', lastBuildTime: 'Last Build Time', owner: 'Owner', createTime: 'Create Time', actions: 'Action', drop: 'Drop', edit: 'Edit', build: 'Build', manage: 'Manage', enable: 'Enable', clone: 'Clone', disable: 'Disable', editCubeDesc: 'Edit CubeDesc', viewCube: 'View Cube', backup: 'Backup', storage: 'Storage', cancel: 'Cancel', yes: 'Yes', tip: 'Tip', deleteSuccessful: 'Delete the cube successful!', deleteCube: 'Once it\'s deleted, your cube\'s metadata and data will be cleaned up and can\'t be restored back. ', enableCube: 'Are you sure to enable the cube? Please note: if cube schema is changed in the disabled period, all segments of the cube will be discarded due to data and schema mismatch.', enableSuccessful: 'Enable the cube successful!', disableCube: 'Are you sure to disable the cube?', disableSuccessful: 'Disable the cube successful!', purgeCube: 'Are you sure to purge the cube? ', purgeSuccessful: 'Purge the cube successful!', backupCube: 'Are you sure to backup ?', backupSuccessful: 'Backup the cube successful!', buildCube: 'Are you sure to start the build?', buildSuccessful: 'Build the cube successful!', cubeBuildConfirm: 'CUBE BUILD CONFIRM', cubeRefreshConfirm: 'CUBE Refresh Confirm', refreshSuccessful: 'Refresh the cube successful!', cubeMergeConfirm: 'CUBE Merge Confirm', mergeSuccessful: 'Merge the cube successful!', cubeCloneConfirm: 'CUBE Clone Confirm', cloneSuccessful: 'Clone the cube successful!', chooseModel: 'choose model to filter'},
    'zh-cn': {name: '名称', model: '模型', status: '状态', cubeSize: '存储空间', sourceTableSize: '源表大小：', expansionRate: '膨胀率：', sourceRecords: '源数据条目', lastBuildTime: '最后构建时间', owner: '所有者', createTime: '创建时间', actions: '操作', drop: '删除', edit: '编辑', build: '构建', manage: '管理', enable: '启用', clone: '克隆', disable: '禁用', editCubeDesc: '编辑 Cube详细信息', viewCube: '查看 Cube', backup: '备份', storage: '存储', tip: '提示', cancel: '取消', yes: '确定', deleteSuccessful: '删除cube成功!', deleteCube: '删除后, Cube定义及数据会被清除, 且不能恢复.', enableCube: '请注意, 如果在禁用期间, Cube的元数据发生改变, 所有的Segment会被丢弃. 确定要启用Cube?', enableSuccessful: '启用cube成功!', disableCube: '确定要禁用此Cube? ', disableSuccessful: '禁用cube成功!', purgeCube: '确定要清空此Cube?', purgeSuccessful: '清理cube成功!', backupCube: '确定要备份此Cube? ', backupSuccessful: '备份cube成功!', buildCube: '确定要构建此Cube?', buildSuccessful: '构建cube成功!', cubeBuildConfirm: 'Cube构建确认', cubeRefreshConfirm: 'Cube刷新确认', refreshSuccessful: '刷新Cube成功!', cubeMergeConfirm: 'Cube合并确认', mergeSuccessful: '合并Cube成功!', cubeCloneConfirm: 'Cube克隆确认', cloneSuccessful: '克隆Cube成功!', chooseModel: '选择model过滤'}
  }
}
</script>
<style lang="less">
  @import '../../less/config.less';
  .cube-list {
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
      .el-tabs--default {
        padding-bottom: 0px;
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
      .el-table__expanded-cell {
        padding: 5px 5px 5px 5px;
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

