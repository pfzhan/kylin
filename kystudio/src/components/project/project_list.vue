<template>
  <div>
  <el-table
    :data="projectList"
    style="width: 100%">
    <el-table-column type="expand">
      <template scope="props">
         <el-tabs activeName="first" type="card" >
           <el-tab-pane label="Cubes" name="first">
             <cube_list :cubeList="props.row.realizations"></cube_list>
           </el-tab-pane>
           <el-tab-pane :label="$t('access')" name="second">access
           </el-tab-pane>
           <el-tab-pane :label="$t('externalFilters')" name="third">
            External Filters
           </el-tab-pane>
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
      prop="create_time_utc">
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
        <el-dropdown-item @click.native="backup">{{$t('backup')}}</el-dropdown-item>
        <el-dropdown-item @click.native="removeProject(scope.row)">{{$t('delete')}}</el-dropdown-item>
      </el-dropdown-menu>
      </el-dropdown>
      </template>      
    </el-table-column>     
    </el-table>
    <el-dialog :title="$t('project')" v-model="FormVisible">
      <project_edit :project="project" ref="projectForm" v-on:validSuccess="validSuccess" v-on:validFailed='validFailed'></project_edit>
      <span slot="footer" class="dialog-footer">
         <el-button @click="FormVisible = false">{{$t('cancel')}}</el-button>
         <el-button type="primary" @click.native="updateProject">{{$t('yes')}}</el-button>
      </span>     
    </el-dialog>  
 </div>
</template>
<script>
import { mapActions } from 'vuex'
import cubeList from './cube_list'
import projectEdit from './project_edit'
export default {
  name: 'projectlist',
  methods: {
    ...mapActions({
      loadProjects: 'LOAD_PROJECT_LIST',
      deleteProject: 'DELETE_PROJECT',
      updateProject: 'UPDATE_PROJECT',
      saveProject: 'SAVE_PROJECT'
    }),
    editProject (project) {
      this.FormVisible = true
      this.project = project
    },
    updateProject () {
      this.$refs.projectForm.$emit('projectFormValid')
    },
    validSuccess (data) {
      let _this = this
      this.updateProject({name: this.project.name, desc: data}).then((result) => {
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
      this.FormVisible = false
    },
    validFailed (data) {
      // this.FormVisible = false
    },
    removeProject (project) {
      this.$confirm('此操作将永久删除, 是否继续?', '提示', {
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
    backup () {
      console.log('1')
    }
  },
  data () {
    return {
      project: {},
      isEdit: false,
      FormVisible: false,
      deleteTip: false,
      selected_project: localStorage.getItem('selected_project')
    }
  },
  components: {
    'cube_list': cubeList,
    'project_edit': projectEdit
  },
  computed: {
    projectList () {
      return this.$store.state.project.projectList
    }
  },
  created () {
    this.loadProjects()
  },
  locales: {
    'en': {project: '项目', name: 'Name', owner: 'Owner', description: 'Description', createTime: 'Create Time', action: 'Action', access: 'Access', externalFilters: 'External Filters', edit: 'Edit', backup: 'Backup', delete: 'Delete', cancel: 'Cancel', yes: 'Yes', saveSuccessful: 'Saved the project successful!', saveFailed: 'Save Failed!', deleteProject: 'Once it\'s deleted, your project\'s metadata and data will be cleaned up and can\'t be restored back.  '},
    'zh-cn': {project: 'Project', name: '名称', owner: '所有者', description: '描述', createTime: '创建时间', action: '操作', access: '权限', externalFilters: '其他过滤', edit: '编辑', backup: '备份', delete: '删除', cancel: '取消', yes: '确定', saveSuccessful: '保存项目成功!', saveFailed: '保存失败!', deleteProject: '删除后, 项目定义及数据会被清除, 且不能恢复.'}
  }
}
</script>
<style scoped="">
</style>
