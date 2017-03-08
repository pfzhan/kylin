<template>
  <div>
  <el-table
    :data="projectList"
    style="width: 100%">
    <el-table-column type="expand">
      <template scope="props">
         <el-tabs v-model="activeName" type="card" >
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
        <el-dropdown-item @click.native="selectProject(scope.row)">Edit</el-dropdown-item>      
        <el-dropdown-item>Backup</el-dropdown-item>
        <el-dropdown-item @click.native="dialogVisible=true">Delete</el-dropdown-item>
      </el-dropdown-menu>
      </el-dropdown>
      </template>      
    </el-table-column>     
    </el-table>

    <el-dialog title="Project" v-model="dialogFormVisible">
      <project_edit :project="project"></project_edit>
      <div slot="footer" class="dialog-footer">
        <el-button @click="dialogFormVisible = false">取 消</el-button>
        <el-button type="primary" @click="dialogFormVisible = false">确 定</el-button>
      </div>
    </el-dialog>  
    <el-dialog title="提示" v-model="dialogVisible" size="tiny">
      <span>Are you sure to delete this project.</span>
      <span slot="footer" class="dialog-footer">
       <el-button @click="dialogVisible = false">取 消</el-button>
       <el-button type="primary" @click="dialogVisible = false">确 定</el-button>
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
      loadProjects: 'LOAD_PROJECT_LIST'
    }),
    selectProject (project) {
      this.dialogFormVisible = true
      this.project = project
      console.log(project)
    }
  },
  data () {
    return {
      project: {},
      dialogFormVisible: false,
      dialogVisible: false,
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
