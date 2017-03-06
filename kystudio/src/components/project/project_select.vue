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
      if (localStorage.getItem('selected_project') !== this.selected_project) {

      }
      localStorage.setItem('selected_project', this.selected_project)
    }
  },
  data () {
    return {
      selected_project: localStorage.getItem('selected_project')
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
  },
  mounted () {
    console.log(2)
  },
  updated () {
    console.log(3)
  }
}
</script>
<style scoped="">

</style>
