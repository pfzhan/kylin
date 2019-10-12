<template>
  <div class="paddingbox" id="project-list">
 <!-- <el-button type="primary" plain class="ksd-mb-20 ksd-mt-10" v-if="isAdmin && (projectList && projectList.length)" @click="addProject">+{{$t('kylinLang.common.project')}}</el-button> -->
 <div class="ksd-title-label ksd-mt-20">{{$t('projectsList')}}</div>
  <div>
    <el-button type="primary" plain size="medium" class="ksd-mb-10 ksd-mt-10" icon="el-icon-ksd-add_2" v-if="projectActions.includes('addProject')" @click="newProject">{{$t('kylinLang.common.project')}}</el-button>
    <div style="width:200px;" class="ksd-fright ksd-mtb-10">
      <el-input class="show-search-btn"
        size="medium"
        prefix-icon="el-icon-search"
        :placeholder="$t('projectFilter')"
        @input="inputFilter">
      </el-input>
    </div>
    <el-table
      :data="projectList"
      tooltip-effect="dark"
      border
      class="project-table"
      style="width: 100%">
      <el-table-column type="expand" :width="34" align="center">
        <template slot-scope="props">
          <el-tabs activeName="first" type="card" class="el-tabs--default">
            <!-- <el-tab-pane label="Models" name="first">
              <model_list :modelList="props.row.models"></model_list>
            </el-tab-pane>
            <el-tab-pane label="Cubes" name="second">
              <cube_list :cubeList="props.row.realizations"></cube_list>
            </el-tab-pane> -->
            <!-- <el-tab-pane :label="$t('access')" name="first">
              <access_edit :accessId="props.row.uuid" :projectName="props.row.name" own='project'></access_edit>
            </el-tab-pane> -->
            <el-tab-pane :label="$t('projectConfig')" name="first">
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
        :label="$t('type')"
        show-overflow-tooltip
        :width="120"
        prop="maintain_model_type">
        <template slot-scope="scope">
          {{scope.row.maintain_model_type === projectType.auto ? $t('autoType') : $t('manualType')}}
        </template>
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
        :width="218"
        :label="$t('createTime')"
        prop="gmtTime">
        <template slot-scope="scope">
          {{transToGmtTime(scope.row.create_time_utc)}}
        </template>
      </el-table-column>
      <el-table-column
        :width="83"
        :label="$t('actions')">
        <template slot-scope="scope">
        <!--<span v-if="!(isAdmin || hasAdminProjectPermission(scope.row.uuid))">N/A</span> v-if="isAdmin || hasAdminProjectPermission(scope.row.uuid)"-->
        <!-- <i class="el-icon-ksd-setting ksd-fs-16" @click="editProject(scope.row)" v-if="isAdmin || hasAdminProjectPermission(scope.row.uuid)"></i> -->
          <!-- <el-tooltip :content="$t('setting')" effect="dark" placement="top">
            <i class="el-icon-ksd-setting ksd-mr-10 ksd-fs-14" @click="changeProject(scope.row)" v-if="isAdmin || hasAdminProjectPermission(scope.row.uuid)"></i>
          </el-tooltip><span>
          </span> -->
          <el-tooltip :content="$t('backup')" effect="dark" placement="top">
            <i class="el-icon-ksd-backup ksd-mr-10 ksd-fs-14" v-if="projectActions.includes('backUpProject')" @click="backup(scope.row)"></i>
          </el-tooltip><span>
          </span><el-tooltip :content="$t('author')" effect="dark" placement="top">
            <router-link :to="{path: '/admin/project/' + scope.row.name, query: {projectId: scope.row.uuid}}">
              <i class="el-icon-ksd-security ksd-mr-10 ksd-fs-14" v-if="projectActions.includes('accessActions')"></i>
            </router-link>
          </el-tooltip><span>
          </span><el-tooltip :content="$t('delete')" effect="dark" placement="top">
            <i class="el-icon-ksd-table_delete ksd-fs-14" @click="removeProject(scope.row)" v-if="projectActions.includes('deleteProject')"></i>
          </el-tooltip>
        </template>
      </el-table-column>
      </el-table>
      <kap-pager
        class="ksd-center ksd-mtb-10" ref="pager"
        :totalSize="projectsTotal"
        v-if="projectsTotal"
        @handleCurrentChange="handleCurrentChange">
      </kap-pager>
    </div>
 </div>
</template>
<script>
import { mapActions, mapGetters } from 'vuex'
import cubeList from './cube_list'
import modeList from './model_list'
import accessEdit from './access_edit'
import filterEdit from './filter_edit'
import projectConfig from './project_config'
import { permissions, pageCount, projectCfgs } from '../../config/index'
import { handleSuccess, handleError, transToGmtTime, hasPermission, hasRole, kapConfirm } from '../../util/business'
export default {
  name: 'projectlist',
  methods: {
    transToGmtTime: transToGmtTime,
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
    inputFilter (value) {
      clearInterval(this.filterTimer)
      this.filterTimer = setTimeout(() => {
        this.filterData.project = value
        this.loadProjects(this.filterData)
      }, 1500)
    },
    checkProjectForm () {
      this.$refs.projectForm.$emit('projectFormValid')
    },
    handleCurrentChange (currentPage, pageSize) {
      this.filterData.pageOffset = currentPage
      this.filterData.pageSize = pageSize
      this.loadProjects(this.filterData)
    },
    async newProject () {
      const isSubmit = await this.callProjectEditModal({ editType: 'new' })
      isSubmit && this.loadProjects(this.filterData)
      this.loadAllProjects()
    },
    async changeProject (project) {
      const isSubmit = await this.callProjectEditModal({ editType: 'edit', project })
      if (isSubmit) {
        this.loadProjects(this.filterData)
        this.loadAllProjects()
      }
    },
    removeProject (project) {
      kapConfirm(this.$t('deleteProjectTip', {projectName: project.name}), null, this.$t('delProjectTitle')).then(() => {
        this.deleteProject(project.name).then((result) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.delSuccess')
          })
          this.loadProjects(this.filterData)
          this.loadAllProjects()
        }, (res) => {
          handleError(res)
        })
      })
    },
    backup (project) {
      kapConfirm(this.$t('backupProject'), {type: 'info'}, this.$t('backupPro')).then(() => {
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
      projectType: projectCfgs.projectType,
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
      projectWidth: '440px',
      filterTimer: null,
      filterData: {pageOffset: 0, pageSize: pageCount, exact: false, project: ''}
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
    ...mapGetters([
      'projectActions'
    ]),
    projectList () {
      return this.$store.state.project.projectList
    },
    projectsTotal () {
      return this.$store.state.project.projectTotalSize
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  created () {
    this.loadProjects(this.filterData)
  },
  locales: {
    'en': {
      autoType: 'Smart Mode',
      manualType: 'Expert Mode',
      project: 'Project',
      name: 'Name',
      type: 'Type',
      owner: 'Owner',
      description: 'Description',
      createTime: 'Create Time',
      actions: 'Actions',
      setting: 'Setting',
      access: 'Access',
      externalFilters: 'External Filters',
      edit: 'Configure',
      backup: 'Backup',
      delete: 'Delete',
      delProjectTitle: 'Delete Project',
      cancel: 'Cancel',
      yes: 'Ok',
      saveSuccessful: 'Saved the project successful!',
      saveFailed: 'Save Failed!',
      deleteProjectTip: 'Once it\'s deleted, the project {projectName}\'s metadata and data will be cleaned up and can\'t be restored back.  ',
      projectConfig: 'Configuration',
      backupProject: 'Are you sure to backup this project ?',
      noProject: 'There is no Project.  You can click below button to add Project.',
      projectsList: 'Project List',
      projectFilter: 'Search Project',
      backupPro: 'Backup Project',
      author: 'Authorization'
    },
    'zh-cn': {
      autoType: '智能模式',
      manualType: '专家模式',
      project: '项目',
      name: '名称',
      type: '类型',
      owner: '所有者',
      description: '描述',
      createTime: '创建时间',
      actions: '操作',
      setting: '设置',
      access: '权限',
      externalFilters: '其他过滤',
      edit: '配置',
      backup: '备份',
      delete: '删除',
      delProjectTitle: '删除项目',
      cancel: '取消',
      yes: '确定',
      saveSuccessful: '保存项目成功!',
      saveFailed: '保存失败!',
      deleteProjectTip: '删除后, 项目 {projectName} 的定义及数据会被清除, 且不能恢复.',
      projectConfig: '项目配置',
      backupProject: '确认要备份此项目？',
      noProject: '您可以点击下面的按钮来添加项目。',
      projectsList: '项目列表',
      projectFilter: '搜索项目',
      backupPro: '备份项目',
      author: '授权'
    }
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
    .project-table {
      .el-icon-ksd-security {
        color: @text-title-color;
        &:hover {
          color: @base-color;
        }
      }
      .el-icon-ksd-backup:hover,
      .el-icon-ksd-table_delete:hover {
        color: @base-color;
      }
    }
    .el-table__expanded-cell[class*=cell] {
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
