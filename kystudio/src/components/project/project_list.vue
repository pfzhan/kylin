<template>
  <div class="paddingbox">
  <el-table
    :data="projectList"
    style="width: 100%">
    <el-table-column type="expand">
      <template scope="props">
         <el-tabs activeName="first" type="card" >
          <el-tab-pane label="Models" name="first">
            <model_list :modelList="props.row.models"></model_list>
          </el-tab-pane>
          <el-tab-pane label="Cubes" name="second">
            <cube_list :cubeList="props.row.realizations"></cube_list>
          </el-tab-pane>
          <el-tab-pane :label="$t('access')" name="third">
            <access_edit :accessId="props.row.uuid" own='project'></access_edit>
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
      prop="name">
    </el-table-column>
    <el-table-column
      :label="$t('owner')"
      prop="owner">
    </el-table-column>
    <el-table-column
      :label="$t('description')"
      prop="description">
    </el-table-column> 
    <el-table-column
      :label="$t('createTime')"
      prop="gmtTime">
    </el-table-column>   
    <el-table-column
      :label="$t('action')">
      <template scope="scope">
      <el-dropdown trigger="click">
      <el-button class="el-dropdown-link">
        <i class="el-icon-more"></i>
      </el-button >
      <el-dropdown-menu slot="dropdown">
        <el-dropdown-item @click.native="editProject(scope.row)">{{$t('edit')}}</el-dropdown-item> 
        <el-dropdown-item @click.native="backup(scope.row)">{{$t('backup')}}</el-dropdown-item>
        <el-dropdown-item @click.native="removeProject(scope.row)">{{$t('delete')}}</el-dropdown-item>
      </el-dropdown-menu>
      </el-dropdown>
      </template>      
    </el-table-column>     
    </el-table>
    <pager class="ksd-center" :pageSize="pageSize" :totalSize="projectsTotal" :currentPage='currentPage' v-on:handleCurrentChange='pageCurrentChange' ></pager>

    <el-dialog :title="$t('project')" v-model="FormVisible" @close="resetProjectForm">
      <project_edit ref="projectForm" :project="project"  v-on:validSuccess="validSuccess" v-on:validFailed='validFailed'></project_edit>
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
import { handleSuccess, handleError, transToGmtTime } from '../../util/business'
export default {
  name: 'projectlist',
  methods: {
    ...mapActions({
      loadProjects: 'LOAD_PROJECT_LIST',
      deleteProject: 'DELETE_PROJECT',
      updateProject: 'UPDATE_PROJECT',
      saveProject: 'SAVE_PROJECT',
      saveAccess: 'SAVE_PROJECT_ACCESS',
      editAccess: 'EDIT_PROJECT_ACCESS',
      getAccess: 'GET_PROJECT_ACCESS',
      delAccess: 'DEL_PROJECT_ACCESS',
      backupProject: 'BACKUP_PROJECT'
    }),
    editProject (project) {
      this.FormVisible = true
      this.project = project
    },
    checkProjectForm () {
      this.$refs.projectForm.$emit('projectFormValid')
    },
    saveAccess () {
      this.$notify({
        title: '保存成功',
        message: 'Access保存成功',
        type: 'success'
      })
      // this.editAccessVisible = false
    },
    saveFilter () {
      this.$notify({
        title: '保存成功',
        message: 'Filter保存成功',
        type: 'success'
      })
      // this.editFilterVisible = false
    },
    pageCurrentChange (currentPage) {
      this.loadProjects({pageOffset: currentPage - 1, pageSize: this.pageSize})
    },
    validSuccess (data) {
      let _this = this
      console.log(9990)
      if (this.project.uuid) {
        this.updateProject({name: this.project.name, desc: JSON.stringify(data)}).then((result) => {
          this.$message({
            type: 'success',
            message: this.$t('saveSuccessful')
          })
          _this.loadProjects()
        }, (result) => {
          this.$message({
            type: 'info',
            message: this.$t('saveFailed')
          })
        })
      } else {
        this.saveProject(JSON.stringify(data)).then((result) => {
          this.$message({
            type: 'success',
            message: this.$t('saveSuccessful')
          })
          console.log(result)
          this.loadProjects()
        }, (res) => {
          handleError(res, (data, code, status, msg) => {
            console.log(data, code, status, msg)
            if (status === 400) {
              this.$message({
                type: 'success',
                message: msg
              })
            }
          })
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
            message: this.$t('saveSuccessful')
          })
          this.loadProjects()
        }, (result) => {
          this.$message({
            type: 'info',
            message: this.$t('saveFailed')
          })
        })
      })
    },
    backup (project) {
      // console.log('1')
      this.backupProject(project).then((result) => {
        handleSuccess(result, (data, code, status, msg) => {
          this.$message({
            type: 'success',
            message: this.$t('backupSuccessful: ' + data)
          })
        })
      }, (res) => {
        handleError(res, (data, code, status, msg) => {
          console.log(data, code, status, msg)
          if (status === 400) {
            this.$message({
              type: 'error',
              message: msg
            })
          }
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
    resetProjectForm () {
      this.$refs['projectForm'].$refs['projectForm'].resetFields()
    }
  },
  data () {
    return {
      pageSize: 6,
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
      filterList: [{
        tableName: 'HDFS',
        resourcePath: '../../xxx',
        description: 'xxx'
      }],
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
    }
  },
  created () {
    this.loadProjects({pageOffset: this.currentPage - 1, pageSize: this.pageSize})
  },
  locales: {
    'en': {project: 'Project', name: 'Name', owner: 'Owner', description: 'Description', createTime: 'Create Time', action: 'Action', access: 'Access', externalFilters: 'External Filters', edit: 'Edit', backup: 'Backup', delete: 'Delete', tip: 'Tip', cancel: 'Cancel', yes: 'Yes', saveSuccessful: 'Saved the project successful!', saveFailed: 'Save Failed!', deleteProject: 'Once it\'s deleted, your project\'s metadata and data will be cleaned up and can\'t be restored back.  ', backupSuccessful: 'backup successful!', projectConfig: 'project config'},
    'zh-cn': {project: '项目', name: '名称', owner: '所有者', description: '描述', createTime: '创建时间', action: '操作', access: '权限', externalFilters: '其他过滤', edit: '编辑', backup: '备份', delete: '删除', tip: '提示', cancel: '取消', yes: '确定', saveSuccessful: '保存项目成功!', saveFailed: '保存失败!', deleteProject: '删除后, 项目定义及数据会被清除, 且不能恢复.', backupSuccessful: '备份成功!', projectConfig: '项目配置'}
  }
}
</script>
<style scoped="">
</style>
