<template>
   <el-select  placeholder="" v-model="selected_project">
    <el-option
      v-for="item in projectList"
      :label="item.name"
      :value="item.name"
      >
    </el-option>
  </el-select>
</template>
<script>
import { mapActions } from 'vuex'
export default {
  name: 'projectselect',
  watch: {
    selected_project (val) {
      localStorage.setItem('selected_project', val)
      this.$store.state.project.selected_project = val
    }
  },
  data () {
    return {
      selected_project: this.$store.state.project.selected_project
    }
  },
  methods: {
    ...mapActions({
      loadProjects: 'LOAD_PROJECT_LIST'
    })
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
