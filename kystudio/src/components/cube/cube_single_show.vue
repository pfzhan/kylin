<template>
<div class="paddingbox ksd-border-tab">
  <el-table
    :data="cubesList"
    :default-expand-all="true"
    style="width: 100%!important">
    <el-table-column type="expand">
      <template slot-scope="props" >
        <el-tabs activeName="first" type="border-card" @tab-click="changeTab">
          <el-tab-pane label="Grid" name="first">
            <cube_desc_view :cube="props.row" :index="props.$index"></cube_desc_view>
          </el-tab-pane>
          <el-tab-pane label="SQL" name="second">
            <show_sql :cube="props.row"></show_sql>
          </el-tab-pane>
          <el-tab-pane label="JSON" name="third" >
            <show_json :json="props.row.desc" ></show_json>
          </el-tab-pane>
          <el-tab-pane :label="$t('storage')" name="fourth">
            <segments :cube="props.row"></segments>
          </el-tab-pane>
        </el-tabs>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('name')"
      prop="name">
    </el-table-column>
    <el-table-column
      :label="$t('model')"
      prop="model">
    </el-table-column>
    <el-table-column
      :label="$t('status')"
      prop="status">
      <template slot-scope="scope">
        <el-tag  :type="scope.row.status === 'DISABLED' ? 'danger' : 'success'">{{scope.row.status}}</el-tag>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('cubeSize')">
      <template slot-scope="scope">
        <span>{{scope.row.size_kb*1024}}KB</span>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('sourceRecords')"
      prop="input_records_count">
    </el-table-column>
    <el-table-column
      :label="$t('lastBuildTime')">
      <template slot-scope="scope">
        <span v-if="scope.row.segments[scope.row.segments.length-1]">{{scope.row.buildGMTTime}}</span>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('owner')"
      prop="owner">
    </el-table-column>
    <el-table-column
      :label="$t('createTime')"
      prop="createGMTTime">
    </el-table-column>
    <el-table-column
      :label="$t('actions')">
      <template slot-scope="scope">
        <el-dropdown trigger="click">
          <el-button class="el-dropdown-link">
            <i class="el-icon-more"></i>
          </el-button >
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item v-if="scope.row.status==='DISABLED' " @click.native="drop(scope.row.name)">{{$t('drop')}}</el-dropdown-item>
            <el-dropdown-item @click.native="edit(scope.row)">{{$t('edit')}}</el-dropdown-item>
            <el-dropdown-item v-if="scope.row.status !== 'DESCBROKEN' " @click.native="build(scope.row)">{{$t('build')}}</el-dropdown-item>
            <el-dropdown-item v-if="scope.row.status==='READY' " @click.native="refresh(scope.row)">{{$t('refresh')}}</el-dropdown-item>
            <el-dropdown-item v-if="scope.row.status!== 'DESCBROKEN'" @click.native="merge(scope.row)">{{$t('merge')}}</el-dropdown-item>
            <el-dropdown-item v-if="scope.row.status=='DISABLED' " @click.native="enable(scope.row.name)">{{$t('enable')}}</el-dropdown-item>
            <el-dropdown-item v-if="scope.row.status!=='DISABLED' " @click.native="disable(scope.row.name)">{{$t('disable')}}</el-dropdown-item>
            <el-dropdown-item v-if="scope.row.status==='DISABLED' " @click.native="purge(scope.row.name)">{{$t('purge')}}</el-dropdown-item>
            <el-dropdown-item v-if="scope.row.status!=='DESCBROKEN' " @click.native="clone(scope.row)">{{$t('clone')}}</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </template>
    </el-table-column>
    <el-table-column
      label="Admin">
      <template slot-scope="scope">
        <el-dropdown trigger="click">
          <el-button class="el-dropdown-link">
            <i class="el-icon-more"></i>
          </el-button >
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item v-if="scope.row.status==='DISABLED' " @click.native="editCubeDesc(scope.row)">{{$t('editCubeDesc')}}</el-dropdown-item>
            <el-dropdown-item @click.native="view(scope.row)">{{$t('viewCube')}}</el-dropdown-item>
            <el-dropdown-item @click.native="backup(scope.row.name)">{{$t('backup')}}</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </template>
    </el-table-column>
  </el-table>

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

</div>
</template>

<script>
import { mapActions } from 'vuex'
import showJson from './json'
import showSql from './sql'
import segments from './segments'
import cubeDescView from './view/cube_desc_view'
import buildCube from './dialog/build_cube'
import cloneCube from './dialog/clone_cube'
import mergeCube from './dialog/merge_cube'
import refreshCube from './dialog/refresh_cube'
import { handleSuccess, handleError, transToGmtTime } from '../../util/business'
export default {
  name: 'cubeslist',
  props: ['extraoption'],
  data () {
    return {
      cubesList: [],
      buildCubeFormVisible: false,
      cloneCubeFormVisible: false,
      mergeCubeFormVisible: false,
      refreshCubeFormVisible: false,
      selected_cube: {},
      selected_project: this.$store.state.project.selected_project
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
    'refresh_cube': refreshCube
  },
  methods: {
    ...mapActions({
      getCubesList: 'GET_CUBES_LIST',
      deleteCube: 'DELETE_CUBE',
      rebuildCube: 'REBUILD_CUBE',
      rebuildStreamingCube: 'REBUILD_STREAMING_CUBE',
      enableCube: 'ENABLE_CUBE',
      disableCube: 'DISABLE_CUBE',
      purgeCube: 'PURGE_CUBE',
      cloneCube: 'CLONE_CUBE',
      backupCube: 'BACKUP_CUBE',
      getCubeSql: 'GET_CUBE_SQL',
      deleteRawTable: 'DELETE_RAW_TABLE',
      deleteScheduler: 'DELETE_SCHEDULER'
    }),
    transToGmtTime,
    loadCubesList: function () {
      let _this = this
      this.getCubesList({pageSize: 65535, pageOffset: 0, projectName: localStorage.getItem('selected_project')}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          data.cubes.map((p) => {
            if (p.name === _this.extraoption.cubeName) {
              p.createGMTTime = transToGmtTime(p.create_time_utc, _this)
              if (p.segments.length > 0) {
                p.buildGMTTime = transToGmtTime(p.segments[p.segments.length - 1].last_build_time, _this)
              }
              this.cubesList.push(p)
            }
          })
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
        })
      })
    },
    drop: function (cubeName) {
      this.$confirm(this.$t('deleteCube'), this.$t('tip'), {
        confirmButtonText: this.$t('yes'),
        cancelButtonText: this.$t('cancel'),
        type: 'warning'
      }).then(() => {
        this.deleteCube({cubeName: cubeName, project: this.selected_project}).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            this.$message({
              type: 'success',
              message: this.$t('deleteSuccessful'),
              duration: 3000
            })
          })
          this.deleteRawTable(cubeName).then((res) => {
            handleSuccess(res, (data, code, status, msg) => {
            })
          }).catch((res) => {
            handleError(res, (data, code, status, msg) => {
            })
          })
          this.deleteScheduler(cubeName).then((res) => {
            handleSuccess(res, (data, code, status, msg) => {
            })
          }).catch((res) => {
            handleError(res, (data, code, status, msg) => {
            })
          })
          this.loadCubesList()
        }).catch((res) => {
          handleError(res, (data, code, status, msg) => {
            this.$message({
              type: 'error',
              message: msg,
              duration: 0,  // 不自动关掉提示
              showClose: true    // 给提示框增加一个关闭按钮
            })
            if (status === 404) {
              this.$router.replace('access/login')
            }
          })
        })
      }).catch(() => {
      })
    },
    edit: function (cube) {
      this.$emit('addtabs', 'cube', cube.name, 'cubeEdit', {
        project: localStorage.getItem('selected_project'),
        cubeName: cube.name,
        modelName: cube.model,
        isEdit: true
      })
    },
    build: function (cube) {
      let _this = this
      _this.selected_cube = cube
      if (cube.is_streaming) {
        this.$confirm(this.$t('buildCube'), this.$t('tip'), {
          confirmButtonText: this.$t('yes'),
          cancelButtonText: this.$t('cancel'),
          type: 'warning'
        }).then(() => {
          _this.rebuildStreamingCube(cube.name).then((res) => {
            handleSuccess(res, (data, code, status, msg) => {
              this.$message({
                type: 'success',
                message: this.$t('buildSuccessful'),
                duration: 3000
              })
              _this.loadCubesList()
            })
          }).catch((res) => {
            handleError(res, (data, code, status, msg) => {
              this.$message({
                type: 'error',
                message: msg,
                duration: 0,  // 不自动关掉提示
                showClose: true    // 给提示框增加一个关闭按钮
              })
              if (status === 404) {
                _this.$router.replace('access/login')
              }
            })
          })
        }).catch(() => {
        })
      } else {
        if (cube.partitionDateColumn) {
          this.buildCubeFormVisible = true
        } else {
          this.$confirm(this.$t('buildCube'), this.$t('tip'), {
            confirmButtonText: this.$t('yes'),
            cancelButtonText: this.$t('cancel'),
            type: 'warning'
          }).then(() => {
            let time = {buildType: 'BUILD', startTime: 0, endTime: 0}
            _this.rebuildCube({cubeName: cube.name, timeZone: time}).then((res) => {
              handleSuccess(res, (data, code, status, msg) => {
                this.$message({
                  type: 'success',
                  message: this.$t('buildSuccessful'),
                  duration: 3000
                })
                _this.loadCubesList()
              })
            }).catch((res) => {
              handleError(res, (data, code, status, msg) => {
                this.$message({
                  type: 'error',
                  message: msg,
                  duration: 0,  // 不自动关掉提示
                  showClose: true    // 给提示框增加一个关闭按钮
                })
                if (status === 404) {
                  _this.$router.replace('access/login')
                }
              })
            })
          }).catch(() => {
          })
        }
      }
    },
    checkBuildCubeForm: function () {
      this.$refs['buildCubeForm'].$emit('buildCubeFormValid')
    },
    buildCubeValidSuccess: function (data) {
      let _this = this
      let time = {buildType: 'BUILD', startTime: data.start, endTime: data.end}
      this.rebuildCube({cubeName: _this.selected_cube.name, timeZone: time}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$message({
            type: 'success',
            message: this.$t('buildSuccessful'),
            duration: 3000
          })
          _this.loadCubesList()
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
          this.$message({
            type: 'error',
            message: msg,
            duration: 0,  // 不自动关掉提示
            showClose: true    // 给提示框增加一个关闭按钮
          })
          if (status === 404) {
            _this.$router.replace('access/login')
          }
        })
      })
      this.buildCubeFormVisible = false
    },
    refresh: function (cube) {
      this.selected_cube = cube
      this.refreshCubeFormVisible = true
    },
    checkRefreshCubeForm: function () {
      this.$refs['refreshCubeForm'].$emit('refreshCubeFormValid')
    },
    refreshCubeValidSuccess: function (data) {
      let _this = this
      let time = {buildType: 'REFRESH', startTime: data.date_range_start, endTime: data.date_range_end}
      this.rebuildCube({cubeName: _this.selected_cube.name, timeZone: time}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$message({
            type: 'success',
            message: this.$t('refreshSuccessful'),
            duration: 3000
          })
          _this.loadCubesList()
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
          this.$message({
            type: 'error',
            message: msg,
            duration: 0,  // 不自动关掉提示
            showClose: true    // 给提示框增加一个关闭按钮
          })
          if (status === 404) {
            _this.$router.replace('access/login')
          }
        })
      })
      _this.refreshCubeFormVisible = false
    },
    merge: function (cube) {
      this.selected_cube = cube
      this.mergeCubeFormVisible = true
    },
    checkMergeCubeForm: function () {
      this.$refs['mergeCubeForm'].$emit('mergeCubeFormValid')
    },
    mergeCubeValidSuccess: function (data) {
      let _this = this
      let time = {buildType: 'MERGE', startTime: data.date_range_start, endTime: data.date_range_end}
      this.rebuildCube({cubeName: _this.selected_cube.name, timeZone: time}).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$message({
            type: 'success',
            message: this.$t('mergeSuccessful'),
            duration: 3000
          })
          _this.loadCubesList()
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
          this.$message({
            type: 'error',
            message: msg,
            duration: 0,  // 不自动关掉提示
            showClose: true    // 给提示框增加一个关闭按钮
          })
          if (status === 404) {
            _this.$router.replace('access/login')
          }
        })
      })
      _this.mergeCubeFormVisible = false
    },
    enable: function (cubeName) {
      let _this = this
      this.$confirm(this.$t('enableCube'), this.$t('tip'), {
        confirmButtonText: this.$t('yes'),
        cancelButtonText: this.$t('cancel'),
        type: 'warning'
      }).then(() => {
        this.enableCube(cubeName).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            this.$message({
              type: 'success',
              message: this.$t('enableSuccessful'),
              duration: 3000
            })
            _this.loadCubesList()
          })
        }).catch((res) => {
          handleError(res, (data, code, status, msg) => {
            this.$message({
              type: 'error',
              message: msg,
              duration: 0,  // 不自动关掉提示
              showClose: true    // 给提示框增加一个关闭按钮
            })
            if (status === 404) {
              _this.$router.replace('access/login')
            }
          })
        })
      }).catch(() => {
      })
    },
    disable: function (cubeName) {
      let _this = this
      this.$confirm(this.$t('disableCube'), this.$t('tip'), {
        confirmButtonText: this.$t('yes'),
        cancelButtonText: this.$t('cancel'),
        type: 'warning'
      }).then(() => {
        this.disableCube(cubeName).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            this.$message({
              type: 'success',
              message: this.$t('disableSuccessful'),
              duration: 3000
            })
            _this.loadCubesList()
          })
        }).catch((res) => {
          handleError(res, (data, code, status, msg) => {
            this.$message({
              type: 'error',
              message: msg,
              duration: 0,  // 不自动关掉提示
              showClose: true    // 给提示框增加一个关闭按钮
            })
            if (status === 404) {
              _this.$router.replace('access/login')
            }
          })
        })
      }).catch(() => {
      })
    },
    purge: function (cubeName) {
      let _this = this
      this.$confirm(this.$t('purgeCube'), this.$t('tip'), {
        confirmButtonText: this.$t('yes'),
        cancelButtonText: this.$t('cancel'),
        type: 'warning'
      }).then(() => {
        this.purgeCube(cubeName).then((res) => {
          handleSuccess(res, (data, code, status, msg) => {
            this.$message({
              type: 'success',
              message: this.$t('purgeSuccessful'),
              duration: 3000
            })
            _this.loadCubesList()
          })
        }).catch((res) => {
          handleError(res, (data, code, status, msg) => {
            this.$message({
              type: 'error',
              message: msg,
              duration: 0,  // 不自动关掉提示
              showClose: true    // 给提示框增加一个关闭按钮
            })
            if (status === 404) {
              _this.$router.replace('access/login')
            }
          })
        })
      }).catch(() => {
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
      let _this = this
      this.cloneCube(data).then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$message({
            type: 'success',
            message: this.$t('cloneSuccessful'),
            duration: 3000
          })
          _this.loadCubesList()
        })
      }).catch((res) => {
        handleError(res, (data, code, status, msg) => {
          this.$message({
            type: 'error',
            message: msg,
            duration: 0,  // 不自动关掉提示
            showClose: true    // 给提示框增加一个关闭按钮
          })
          if (status === 404) {
            _this.$router.replace('access/login')
          }
        })
      })
      _this.cloneCubeFormVisible = false
    },
    editCubeDesc: function (cube) {
      this.$emit('addtabs', 'edit', cube.name, 'cubeMetadata', {
        project: localStorage.getItem('selected_project'),
        cubeName: cube.name,
        type: 'edit'
      })
    },
    view: function (cube) {
      this.$emit('addtabs', 'view', cube.name, 'cubeMetadata', {
        project: localStorage.getItem('selected_project'),
        cubeName: cube.name,
        cubeDesc: cube,
        type: 'view'
      })
    },
    backup: function (cubeName) {
      this.$confirm(this.$t('backupCube'), this.$t('tip'), {
        confirmButtonText: this.$t('yes'),
        cancelButtonText: this.$t('cancel'),
        type: 'warning'
      }).then(() => {
        this.backupCube(cubeName).then((result) => {
          this.$message({
            type: 'success',
            message: this.$t('backupSuccessful')
          })
          this.loadCubesList()
        }).catch((result) => {
          this.$message({
            type: 'error',
            message: result.statusText,
            duration: 0,  // 不自动关掉提示
            showClose: true    // 给提示框增加一个关闭按钮
          })
        })
      }).catch(() => {
      })
    },
    changeTab: function (tab) {
      if (tab.$data.index === '1') {
        tab.$children[0].loadCubeSql()
      }
      if (tab.$data.index === '3') {
        tab.$children[0].loadSegments()
      }
    }
  },
  created () {
    this.loadCubesList()
  },
  locales: {
    'en': {name: 'Name', model: 'Model', status: 'Status', cubeSize: 'Cube Size', sourceRecords: 'Source Records', lastBuildTime: 'Last Build Time', owner: 'Owner', createTime: 'Create Time', actions: 'Action', drop: 'Drop', edit: 'Edit', build: 'Build', merge: 'Merge', refresh: 'Refresh', enable: 'Enable', purge: 'Purge', clone: 'Clone', disable: 'Disable', editCubeDesc: 'Edit CubeDesc', viewCube: 'View Cube', backup: 'Backup Cube', storage: 'Storage', cancel: 'Cancel', yes: 'Yes', tip: 'Tip', deleteSuccessful: 'Delete the cube successful!', deleteCube: 'Once it\'s deleted, your cube\'s metadata and data will be cleaned up and can\'t be restored back. ', enableCube: 'Are you sure to enable the cube? Please note: if cube schema is changed in the disabled period, all segments of the cube will be discarded due to data and schema mismatch.', enableSuccessful: 'Enable the cube successful!', disableCube: 'Are you sure to disable the cube?', disableSuccessful: 'Disable the cube successful!', purgeCube: 'Are you sure to purge the cube? ', purgeSuccessful: 'Purge the cube successful!', backupCube: 'Are you sure to backup ?', backupSuccessful: 'Backup the cube successful!', buildCube: 'Are you sure to start the build?', buildSuccessful: 'Build the cube successful!', cubeBuildConfirm: 'CUBE BUILD CONFIRM', cubeRefreshConfirm: 'CUBE Refresh Confirm', refreshSuccessful: 'Refresh the cube successful!', cubeMergeConfirm: 'CUBE Merge Confirm', mergeSuccessful: 'Merge the cube successful!', cubeCloneConfirm: 'CUBE Clone Confirm', cloneSuccessful: 'Clone the cube successful!'},
    'zh-cn': {name: '名称', model: '模型', status: '状态', cubeSize: 'Cube大小', sourceRecords: '源数据条目', lastBuildTime: '最后构建时间', owner: '所有者', createTime: '创建时间', actions: '操作', drop: '删除', edit: '编辑', build: '构建', merge: '合并', refresh: '刷新', enable: '启用', purge: '清理', clone: '克隆', disable: '禁用', editCubeDesc: '编辑 Cube详细信息', viewCube: '查看 Cube', backup: '备份cube', storage: '存储', tip: '提示', cancel: '取消', yes: '确定', deleteSuccessful: '删除cube成功!', deleteCube: '删除后, Cube定义及数据会被清除, 且不能恢复.', enableCube: '请注意, 如果在禁用期间, Cube的元数据发生改变, 所有的Segment会被丢弃. 确定要启用Cube?', enableSuccessful: '启用cube成功!', disableCube: '确定要禁用此Cube? ', disableSuccessful: '禁用cube成功!', purgeCube: '确定要清空此Cube?', purgeSuccessful: '清理cube成功!', backupCube: '确定要备份此Cube? ', backupSuccessful: '备份cube成功!', buildCube: '确定要构建此Cube?', buildSuccessful: '构建cube成功!', cubeBuildConfirm: 'Cube构建确认', cubeRefreshConfirm: 'Cube刷新确认', refreshSuccessful: '刷新Cube成功!', cubeMergeConfirm: 'Cube合并确认', mergeSuccessful: '合并Cube成功!', cubeCloneConfirm: 'Cube克隆确认', cloneSuccessful: '克隆Cube成功!'}
  }
}
</script>
<style scoped>
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
</style>

