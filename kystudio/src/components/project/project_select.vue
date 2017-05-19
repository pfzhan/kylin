<template>
   <el-select  placeholder="请选择project" v-model="selected_project">
    <el-option
      v-for="item in projectList" :key="item.name"
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
      this.$emit('changePro', val)
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
    }),
    clearProject () {
      localStorage.removeItem('selected_project')
      this.$store.state.project.selected_project = ''
    }
  },
  computed: {
    projectList () {
      return this.$store.state.project.projectList || ''
    }
  },
  created () {
    console.log(this.$store.state.project.selected_project, '0101')
    this.loadProjects()
  }
}
</script>
<style scoped="">

</style>
