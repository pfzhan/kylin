<template>
  <div class="paddingbox" id="project-list">
 <el-button style="border-radius: 20px;" type="blue" class="ksd-mb-10" v-if="isAdmin" @click="addProject">+{{$t('kylinLang.common.project')}}</el-button>
 <img src="../../assets/img/no_project.png" class="null_pic" v-if="!(projectList && projectList.length)">
  <el-table v-if="projectList && projectList.length"
    :data="projectList"
    tooltip-effect="dark"
    style="width: 100%">
    <el-table-column type="expand">
      <template slot-scope="props">
         <el-tabs activeName="first" class="el-tabs--default">
          <el-tab-pane label="Models" name="first">
            <model_list :modelList="props.row.models"></model_list>
          </el-tab-pane>
          <el-tab-pane label="Cubes" name="second">
            <cube_list :cubeList="props.row.realizations"></cube_list>
          </el-tab-pane>
          <el-tab-pane :label="$t('access')" name="third">
            <access_edit :accessId="props.row.uuid" :projectName="props.row.name" own='project'></access_edit>
          </el-tab-pane>
          <el-tab-pane :label="$t('projectConfig')" name="fourth">
            <project_config :override="props.row.override_kylin_properties"></project_config>
          </el-tab-pane>
           <!-- <el-tab-pane :label="$t('externalFilters')" name="fourth">
             <filter_edit :project="props.row.name" :projectId="props.row.uuid"></filter_edit>
           </el-tab-pane> -->
        </el-tabs>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('name')"
      show-overflow-tooltip
      :width="320"
      prop="name">
    </el-table-column>
    <el-table-column
      :label="$t('owner')"
      :width="220"
      show-overflow-tooltip
      prop="owner">
    </el-table-column>
    <el-table-column
      :label="$t('description')"
      show-overflow-tooltip
      prop="description">
    </el-table-column>
    <el-table-column
      show-overflow-tooltip
      :width="196"
      :label="$t('createTime')"
      prop="gmtTime">
    </el-table-column>
    <el-table-column
      :width="100"
      :label="$t('actions')">
      <template slot-scope="scope">
      <!--<span v-if="!(isAdmin || hasAdminProjectPermission(scope.row.uuid))">N/A</span> v-if="isAdmin || hasAdminProjectPermission(scope.row.uuid)"-->
      <el-dropdown trigger="click">
      <el-button class="el-dropdown-link">
        <i class="el-icon-more"></i>
      </el-button >
      <el-dropdown-menu slot="dropdown" >
        <el-dropdown-item @click.native="editProject(scope.row)" v-if="isAdmin || hasAdminProjectPermission(scope.row.uuid)">{{$t('edit')}}</el-dropdown-item>
        <el-dropdown-item @click.native="backup(scope.row)">{{$t('backup')}}</el-dropdown-item>
        <el-dropdown-item @click.native="removeProject(scope.row)" v-if="isAdmin">{{$t('delete')}}</el-dropdown-item>
      </el-dropdown-menu>
      </el-dropdown>
      </template>
    </el-table-column>
    </el-table>
    <pager class="ksd-center" :totalSize="projectsTotal" v-on:handleCurrentChange='pageCurrentChange' ></pager>
    <el-dialog  class="add-project" :title="$t('project')" v-model="FormVisible" @close="resetProjectForm">
      <project_edit ref="projectForm" :project="project"  :visible="FormVisible" v-on:validSuccess="validSuccess" v-on:validFailed='validFailed' :isEdit="isEdit"></project_edit>
      <div slot="footer" class="dialog-footer">
        <el-button @click="FormVisible = false">{{$t('cancel')}}</el-button>
        <el-button type="primary" @click="checkProjectForm">{{$t('yes')}}</el-button>
      </div>
    </el-dialog>
 </div>
</template>
<script>
import { mapActions } from 'vuex'
import cubeList from './cube_list'
import modeList from './model_list'
import accessEdit from './access_edit'
import filterEdit from './filter_edit'
import projectEdit from './project_edit'
import projectConfig from './project_config'
import { permissions, pageCount } from '../../config/index'
import { handleSuccess, handleError, transToGmtTime, hasPermission, hasRole, kapConfirm } from '../../util/business'
export default {
  name: 'projectlist',
  methods: {
    ...mapActions({
      loadProjects: 'LOAD_PROJECT_LIST',
      loadAllProjects: 'LOAD_ALL_PROJECT',
      deleteProject: 'DELETE_PROJECT',
      updateProject: 'UPDATE_PROJECT',
      saveProject: 'SAVE_PROJECT',
      backupProject: 'BACKUP_PROJECT'
    }),
    editProject (project) {
      this.isEdit = true
      this.FormVisible = true
      this.project = project
    },
    checkProjectForm () {
      this.$refs.projectForm.$emit('projectFormValid')
    },
    pageCurrentChange (currentPage) {
      this.currentPage = currentPage
      this.loadProjects({pageOffset: currentPage - 1, pageSize: this.pageCount})
    },
    addProject () {
      this.isEdit = false
      this.FormVisible = true
      this.project = {name: '', description: '', override_kylin_properties: {}}
    },
    validSuccess (data) {
      if (this.project.uuid) {
        this.updateProject({name: this.project.name, desc: JSON.stringify(data)}).then((result) => {
          this.$message({
            type: 'success',
            message: this.$t('saveSuccessful')
          })
          this.loadProjects()
          this.loadAllProjects()
        }, (res) => {
          handleError(res)
        })
      } else {
        this.saveProject(JSON.stringify(data)).then((result) => {
          this.$message({
            type: 'success',
            message: this.$t('saveSuccessful')
          })
          this.loadProjects()
          // this.loadAllProjects()
        }, (res) => {
          handleError(res)
        })
      }
      this.FormVisible = false
    },
    validFailed (data) {
      // this.FormVisible = false
    },
    removeProject (project) {
      this.$confirm(this.$t('deleteProject'), this.$t('tip'), {
        confirmButtonText: this.$t('yes'),
        cancelButtonText: this.$t('cancel'),
        type: 'warning'
      }).then(() => {
        this.deleteProject(project.name).then((result) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.delSuccess')
          })
          this.loadProjects()
          this.loadAllProjects()
        }, (res) => {
          handleError(res)
        })
      })
    },
    backup (project) {
      kapConfirm(this.$t('backupProject')).then(() => {
        this.backupProject(project).then((result) => {
          handleSuccess(result, (data, code, status, msg) => {
            this.$message({
              type: 'success',
              message: this.$t('kylinLang.common.backupSuccessTip') + data,
              showClose: true,
              duration: 0
            })
          })
        }, (res) => {
          handleError(res)
        })
      })
    },
    initAccessMeta () {
      return {
        permission: '',
        principal: true,
        sid: ''
      }
    },
    hasAdminProjectPermission () {
      return hasPermission(this, permissions.ADMINISTRATION.mask)
    },
    resetProjectForm () {
      this.$refs['projectForm'].$refs['projectForm'].resetFields()
    }
  },
  data () {
    return {
      pageCount: pageCount,
      currentPage: 1,
      project: {},
      isEdit: false,
      FormVisible: false,
      deleteTip: false,
      editAccessVisible: false,
      editFilterVisible: false,
      accessMetaList: {},
      accessList: [{
        name: 'admin',
        type: 'user',
        access: 'Cube Admin'
      }],
      accessMeta: {
        permission: 'READ',
        principal: true,
        sid: '',
        editAccessVisible: false
      },
      selected_project: localStorage.getItem('selected_project')
    }
  },
  components: {
    'cube_list': cubeList,
    'project_edit': projectEdit,
    'access_edit': accessEdit,
    'filter_edit': filterEdit,
    'model_list': modeList,
    'project_config': projectConfig
  },
  computed: {
    projectList () {
      return this.$store.state.project.projectList.map((p) => {
        p.gmtTime = transToGmtTime(p.create_time_utc, this)
        return p
      })
    },
    projectsTotal () {
      return this.$store.state.project.projectTotalSize
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  created () {
    this.loadProjects({pageOffset: this.currentPage - 1, pageSize: this.pageCount})
  },
  locales: {
    'en': {project: 'Project', name: 'Name', owner: 'Owner', description: 'Description', createTime: 'Create Time', actions: 'Actions', access: 'Access', externalFilters: 'External Filters', edit: 'Configure', backup: 'Backup', delete: 'Delete', tip: 'Tip', cancel: 'Cancel', yes: 'Yes', saveSuccessful: 'Saved the project successful!', saveFailed: 'Save Failed!', deleteProject: 'Once it\'s deleted, your project\'s metadata and data will be cleaned up and can\'t be restored back.  ', projectConfig: 'Configuration', backupProject: 'Are you sure to backup this project ?'},
    'zh-cn': {project: '项目', name: '名称', owner: '所有者', description: '描述', createTime: '创建时间', actions: '操作', access: '权限', externalFilters: '其他过滤', edit: '配置', backup: '备份', delete: '删除', tip: '提示', cancel: '取消', yes: '确定', saveSuccessful: '保存项目成功!', saveFailed: '保存失败!', deleteProject: '删除后, 项目定义及数据会被清除, 且不能恢复.', projectConfig: '项目配置', backupProject: '确认要备份此项目？'}
  }
}
</script>
<style lang="less">
  @import url(../../less/config.less);
  #project-list{
    margin-left: 30px;
    margin-right: 30px;
    .el-tabs__header{
      border-color: @grey-color;
    }
    .el-tabs__item{
      transition: none;
    }
    .add-project {
      .el-dialog {
        width: 390px;
        .el-dialog__header {
          font-family:Montserrat-Regular;
          font-size:14px;
          color:#ffffff;
          letter-spacing:0;
          line-height:16px;
          text-align:left;
          span {
            line-height: 29px;
          }
          .el-dialog__headerbtn{
            line-height: 29px;
            margin-top: 0px;
          }
        }
        .el-dialog__body {
          padding: 15px 20px 0px 20px;
          min-height: 90px;
        }
      }
    }
  }
</style>
