<template>
  <div class="paddingbox" id="project-list">
 <!-- <el-button type="primary" plain class="ksd-mb-20 ksd-mt-10" v-if="isAdmin && (projectList && projectList.length)" @click="addProject">+{{$t('kylinLang.common.project')}}</el-button> -->
 <el-button type="primary" plain size="medium" class="ksd-mb-20 ksd-mt-10" icon="el-icon-ksd-add_2" v-if="isAdmin && (projectList && projectList.length)" @click="newProject">{{$t('kylinLang.common.project')}}</el-button>
 <div v-if="!(projectList && projectList.length)" class="nodata">
    <div class="ksd-mb-10"><img src="../../assets/img/default_project.png"></div>
    <div class="ksd-mb-20">{{$t('noProject')}}</div>
    <el-button type="primary" size="medium" class="ksd-mb-20 ksd-mt-20" v-if="isAdmin" @click="addProject">+{{$t('kylinLang.common.project')}}</el-button>
 </div>
  <el-table v-if="projectList && projectList.length"
    :data="projectList"
    tooltip-effect="dark"
    border
    style="width: 100%">
    <el-table-column type="expand" :width="34">
      <template slot-scope="props">
         <el-tabs activeName="first" class="el-tabs--default">
          <!-- <el-tab-pane label="Models" name="first">
            <model_list :modelList="props.row.models"></model_list>
          </el-tab-pane>
          <el-tab-pane label="Cubes" name="second">
            <cube_list :cubeList="props.row.realizations"></cube_list>
          </el-tab-pane> -->
          <el-tab-pane :label="$t('access')" name="first">
            <access_edit :accessId="props.row.uuid" :projectName="props.row.name" own='project'></access_edit>
          </el-tab-pane>
          <el-tab-pane :label="$t('projectConfig')" name="second">
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
      header-align="center"
      prop="name">
    </el-table-column>
    <el-table-column
      :label="$t('owner')"
      :width="220"
      show-overflow-tooltip
      header-align="center"
      prop="owner">
    </el-table-column>
    <el-table-column
      :label="$t('description')"
      show-overflow-tooltip
      header-align="center"
      prop="description">
    </el-table-column>
    <el-table-column
      show-overflow-tooltip
      :width="208"
      :label="$t('createTime')"
      align="center"
      prop="gmtTime">
    </el-table-column>
    <el-table-column
      :width="87"
      header-align="center"
      :label="$t('actions')">
      <template slot-scope="scope">
      <!--<span v-if="!(isAdmin || hasAdminProjectPermission(scope.row.uuid))">N/A</span> v-if="isAdmin || hasAdminProjectPermission(scope.row.uuid)"-->
      <!-- <i class="el-icon-ksd-setting ksd-fs-16" @click="editProject(scope.row)" v-if="isAdmin || hasAdminProjectPermission(scope.row.uuid)"></i> -->
        <el-tooltip :content="$t('setting')" effect="dark" placement="top">
          <i class="el-icon-ksd-setting ksd-mr-10 ksd-fs-14" @click="changeProject(scope.row)" v-if="isAdmin || hasAdminProjectPermission(scope.row.uuid)"></i>
        </el-tooltip><span>
        </span><el-tooltip :content="$t('backup')" effect="dark" placement="top">
          <i class="el-icon-ksd-backup ksd-mr-10 ksd-fs-14" @click="backup(scope.row)"></i>
        </el-tooltip><span>
        </span><el-tooltip :content="$t('delete')" effect="dark" placement="top">
          <i class="el-icon-ksd-table_delete ksd-fs-14" @click="removeProject(scope.row)" v-if="isAdmin"></i>
        </el-tooltip>
      </template>
    </el-table-column>
    </el-table>
    <kap-pager
      class="ksd-center ksd-mt-20 ksd-mb-20" ref="pager"
      :totalSize="projectsTotal"
      @handleCurrentChange="handleCurrentChange">
    </kap-pager>
 </div>
</template>
<script>
import { mapActions } from 'vuex'
import cubeList from './cube_list'
import modeList from './model_list'
import accessEdit from './access_edit'
import filterEdit from './filter_edit'
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
    ...mapActions('ProjectEditModal', {
      callProjectEditModal: 'CALL_MODAL'
    }),
    editProject (project) {
      this.isEdit = true
      this.projectWidth = '660px'
      this.FormVisible = true
      this.project = project
    },
    checkProjectForm () {
      this.$refs.projectForm.$emit('projectFormValid')
    },
    handleCurrentChange (currentPage) {
      this.currentPage = currentPage
      this.loadProjects({pageOffset: currentPage, pageSize: this.pageCount})
    },
    addProject () {
      this.isEdit = false
      this.projectWidth = '440px'
      this.FormVisible = true
      this.project = {name: '', description: '', override_kylin_properties: {}}
    },
    async newProject () {
      const isSubmit = await this.callProjectEditModal({ editType: 'new' })
      isSubmit && this.loadProjects({pageOffset: this.currentPage, pageSize: this.pageCount})
      this.loadAllProjects()
    },
    async changeProject (project) {
      const isSubmit = await this.callProjectEditModal({ editType: 'edit', project })
      if (isSubmit) {
        this.loadProjects({pageOffset: this.currentPage, pageSize: this.pageCount})
        this.loadAllProjects()
      }
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
          this.loadProjects({pageOffset: this.currentPage, pageSize: this.pageCount})
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
    }
  },
  data () {
    return {
      pageCount: pageCount,
      currentPage: 0,
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
      selected_project: localStorage.getItem('selected_project'),
      projectWidth: '440px'
    }
  },
  components: {
    'cube_list': cubeList,
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
    this.loadProjects({pageOffset: this.currentPage, pageSize: this.pageCount})
  },
  locales: {
    'en': {project: 'Project', name: 'Name', owner: 'Owner', description: 'Description', createTime: 'Create Time', actions: 'Actions', setting: 'Setting', access: 'Access', externalFilters: 'External Filters', edit: 'Configure', backup: 'Backup', delete: 'Delete', tip: 'Tip', cancel: 'Cancel', yes: 'Ok', saveSuccessful: 'Saved the project successful!', saveFailed: 'Save Failed!', deleteProject: 'Once it\'s deleted, your project\'s metadata and data will be cleaned up and can\'t be restored back.  ', projectConfig: 'Configuration', backupProject: 'Are you sure to backup this project ?', noProject: 'There is no Project.  You can click below button to add Project.'},
    'zh-cn': {project: '项目', name: '名称', owner: '所有者', description: '描述', createTime: '创建时间', actions: '操作', setting: '设置', access: '权限', externalFilters: '其他过滤', edit: '配置', backup: '备份', delete: '删除', tip: '提示', cancel: '取消', yes: '确定', saveSuccessful: '保存项目成功!', saveFailed: '保存失败!', deleteProject: '删除后, 项目定义及数据会被清除, 且不能恢复.', projectConfig: '项目配置', backupProject: '确认要备份此项目？', noProject: '您可以点击下面的按钮来添加项目。'}
  }
}
</script>
<style lang="less">
  @import "../../assets/styles/variables.less";
  #project-list{
    margin-left: 20px;
    margin-right: 20px;
    .nodata {
      text-align: center;
      margin-top: 220px;
      img {
        width: 80px;
        height: 80px;
      }
    }
    .el-table__expanded-cell[class*=cell] {
      padding: 20px 30px;
      background-color: @table-stripe-color;
      .el-pagination.is-background {
        button,
        .el-pager li {
          margin: 0;
        }
      }
    }
    .el-tabs__nav {
      margin-left: 0;
    }
    .el-tabs__header{
      border-color: @grey-color;
    }
    .el-tabs__item{
      transition: none;
    }
    .add-project {
      .el-dialog {
        .el-dialog__body {
          min-height: 90px;
        }
      }
    }
  }
</style>
