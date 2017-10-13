<template>
   <el-select  :placeholder="$t('kylinLang.project.selectProject')" v-model="selected_project" @change="changeProject">
    <el-option
      v-for="item in projectList" :key="item.name"
      :label="item.name"
      :value="item.name"
      >
      </el-option>
    </el-select>
  </div>
</template>
<script>
import { mapActions } from 'vuex'
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
  .project_select {
    input {
      width: 220px;
      background: #393e53;
      border: 0;
      color: #fff;
    }
  }
</style>
