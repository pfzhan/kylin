<template>
  <el-card class="box-card" :body-style="{ padding: '0px'}">
    <div slot="header" class="clearfix">
      <span style="line-height: 36px;">Project</span>
    </div>
    <el-table
      :data="projectList" height='250' >
      <el-table-column
        prop="name"
        label="+ Add Project">
      </el-table-column>
    </el-table>
  </el-card>    
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

.box-card {
	width:30%;
      height: 50%;
      overflow:visible
}
</style>
