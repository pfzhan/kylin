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
           <el-tab-pane label="Access" name="second">Access
           </el-tab-pane>
           <el-tab-pane label="External Filters" name="third">
            External Filters
           </el-tab-pane>
        </el-tabs>
      </template>
    </el-table-column>
    <el-table-column
      label="Name"
      prop="name">
    </el-table-column>
    <el-table-column
      label="Owner"
      prop="owner">
    </el-table-column>
    <el-table-column
      label="Owner"
      prop="owner">
    </el-table-column>
    <el-table-column
      label="Description"
      prop="description">
    </el-table-column>  
    <el-table-column
      label="Action">
      <template scope="scope">
      <el-dropdown trigger="click">
      <el-button class="el-dropdown-link">
        <i class="el-icon-more"></i>
      </el-button >
      <el-dropdown-menu slot="dropdown">
        <el-dropdown-item @click.native="editProject(scope.row)">Edit</el-dropdown-item>      
        <el-dropdown-item @click.native="addProject">Backup</el-dropdown-item>
        <el-dropdown-item @click.native="deleteProject">Delete</el-dropdown-item>
      </el-dropdown-menu>
      </el-dropdown>
      </template>      
    </el-table-column>     
    </el-table>
    <el-dialog title="Project" v-model="FormVisible">
      <project_edit :project="project"></project_edit>
      <span slot="footer" class="dialog-footer">
         <el-button @click="FormVisible = false">取 消</el-button>
         <el-button type="primary" @click="updateOrSave(project)">确 定</el-button>
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
      this.isEdit = true
      this.project = project
    },
    addProject () {
      this.FormVisible = true
      this.isEdit = false
      this.project = {}
    },
    updateOrSave (project) {
      if (this.isEdit) {
        this.updateProject(project)
      } else {
        this.saveProject(project)
      }
    },
    showDeleteTip (project) {
      this.deleteTip = true
      this.project = project
    },
    deleteProject () {
      this.$confirm('此操作将永久删除, 是否继续?', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        this.$message({
          type: 'success',
          message: '删除成功!'
        })
      }).catch(() => {
        this.$message({
          type: 'info',
          message: '已取消删除'
        })
      })
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
  }
}
</script>
<style scoped="">
</style>
