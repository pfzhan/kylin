<template>
   <el-select  placeholder="请选择project" v-model="selected_project" @change="changeProject">
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
  data () {
    return {}
  },
  methods: {
    ...mapActions({
      loadAllProjects: 'LOAD_ALL_PROJECT'
    }),
    clearProject () {
      localStorage.removeItem('selected_project')
      this.$store.state.project.selected_project = ''
    },
    changeProject (val) {
      localStorage.setItem('selected_project', val)
      this.$store.state.project.selected_project = val
      this.$emit('changePro', val)
    }
  },
  computed: {
    projectList () {
      return this.$store.state.project.allProject || ''
    },
    selected_project () {
      return this.$store.state.project.selected_project
    }
  },
  created () {
    // console.log(this.$store.state.project.selected_project, '0101')
    this.loadAllProjects()
  }
}
</script>
<style scoped="">

</style>
