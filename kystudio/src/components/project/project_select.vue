<template>
   <el-select
    size="medium"
    class="project_select"
    filterable
    :title="selected_project"
    :placeholder="$t('kylinLang.project.selectProject')"
    v-model="selected_project"
    @change="changeProject">
    <span slot="prefix" class="el-input__icon" :class="isAutoProject ? 'el-icon-ksd-smart_mode_small' : 'el-icon-ksd-expert_mode_small'"></span>
    <el-option
      v-for="item in projectList" :key="item.name"
      :label="item.name"
      :value="item.name">
      </el-option>
    </el-select>
</template>
<script>
import { mapActions, mapMutations, mapGetters } from 'vuex'
export default {
  name: 'projectselect',
  data () {
    return {
      selected_project: ''
    }
  },
  methods: {
    ...mapActions({
      loadAllProjects: 'LOAD_ALL_PROJECT',
      getUserAccess: 'USER_ACCESS'
    }),
    ...mapMutations({
      setProject: 'SET_PROJECT'
    }),
    changeProject (val) {
      this.$emit('changePro', val)
    }
  },
  watch: {
    '$store.state.project.selected_project' () {
      this.selected_project = this.$store.state.project.selected_project
    }
  },
  computed: {
    ...mapGetters([
      'isAutoProject'
    ]),
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
@import "../../assets/styles/variables.less";

.project_select{
  margin: 14px 0 0 20px;
  float: left;
  width: 220px;
  .el-input__icon {
    font-size: 18px;
    color: @text-disabled-color;
    transform: translate(-1px, 0px);
  }
}
</style>
