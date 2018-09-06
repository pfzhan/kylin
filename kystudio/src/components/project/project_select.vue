<template>
   <el-select class="project_select"  :placeholder="$t('kylinLang.project.selectProject')" v-model="selected_project" @change="changeProject" size="medium">
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
import { cacheSessionStorage, cacheLocalStorage } from 'util/index'
export default {
  name: 'projectselect',
  data () {
    return {
      selected_project: ''
    }
  },
  methods: {
    ...mapActions({
      loadAllProjects: 'LOAD_ALL_PROJECT'
    }),
    changeProject (val) {
      cacheSessionStorage('projectName', val)
      cacheLocalStorage('projectName', val)
      this.$store.state.project.selected_project = val
      this.$emit('changePro', val)
    }
  },
  watch: {
    '$store.state.project.selected_project' () {
      this.selected_project = this.$store.state.project.selected_project
    }
  },
  computed: {
    projectList () {
      return this.$store.state.project.allProject || ''
    }
  },
  created () {
    // console.log(this.$store.state.project.selected_project, '0101')
    this.selected_project = this.$store.state.project.selected_project
    // this.loadAllProjects()
  }
}
</script>
<style lang="less">
  .project_select{
    margin: 14px 0 0 20px;
    float: left;
    width: 220px;
  }
</style>
