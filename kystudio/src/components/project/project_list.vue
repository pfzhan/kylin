<template>
  <el-table
    :data="projectList"
    style="width: 100%">
    <el-table-column type="expand">
      <template scope="props">
         <el-tabs v-model="activeName" type="card" >
           <el-tab-pane label="Cubes" name="first">
             {{props.row}}
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
        <el-dropdown-item>Edit</el-dropdown-item>
        <el-dropdown-item>Backup</el-dropdown-item>
        <el-dropdown-item>Drop</el-dropdown-item>
      </el-dropdown-menu>
      </el-dropdown>
      </template>      
    </el-table-column>     
  </el-table>
</template>

<script>
import { mapActions } from 'vuex'
export default {
  name: 'projectlist',
  methods: {
    ...mapActions({
      loadProjects: 'LOAD_PROJECT_LIST'
    })
  },
  data () {
    return {
      activeName: 'first',
      selected_project: localStorage.getItem('selected_project')
    }
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
