<template>
   <el-select
    size="small"
    class="project_select"
    filterable
    :title="selected_project"
    :placeholder="$t('pleaseSearchOrSelectProject')"
    v-model="selected_project"
    @change="changeProject">
    <el-option v-if="isAdmin && needAllProjectView " value="**" :label="$t('selectAll')"></el-option>
    <span slot="prefix" v-if="projectList.length" class="el-input__icon" :class="isAutoProject ? 'el-icon-ksd-smart_mode_small' : 'el-icon-ksd-expert_mode_small'"></span>
    <el-option
      v-for="item in projectList" :key="item.name"
      class="project_option"
      :label="item.name"
      :value="item.name">
      <i class="el-icon-ksd-smart_mode_small" v-if="item.maintain_model_type === 'AUTO_MAINTAIN'"></i>
      <i class="el-icon-ksd-expert_mode_small" v-if="item.maintain_model_type === 'MANUAL_MAINTAIN'"></i>
      <span>{{item.name}}</span>
    </el-option>
    </el-select>
</template>
<script>
import { mapGetters } from 'vuex'
import { hasRole } from 'util/business'
export default {
  name: 'projectselect',
  data () {
    return {
      selected_project: '',
      needAllProjectView: false
    }
  },
  methods: {
    changeProject (val) {
      if (val === '**') {
        this.$store.state.project.isAllProject = true
      } else {
        this.$store.state.project.isAllProject = false
        this.$emit('changePro', val)
      }
    },
    checkAllProjectView () {
      if (this.$route.name === 'Job') { // 支持在monitor页面让用户选择全局视角
        this.needAllProjectView = true
      } else {
        this.$store.state.project.isAllProject = false
        this.selected_project = this.$store.state.project.selected_project
        this.needAllProjectView = false
      }
    }
  },
  watch: {
    '$store.state.project.selected_project' () {
      this.selected_project = this.$store.state.project.selected_project
    },
    '$route' (route) {
      this.checkAllProjectView()
    }
  },
  computed: {
    ...mapGetters([
      'isAutoProject'
    ]),
    projectList () {
      return this.$store.state.project.allProject || ''
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  created () {
    this.checkAllProjectView()
    this.selected_project = this.$store.state.project.selected_project
  },
  locales: {
    'en': {selectAll: '-- Select All --', pleaseSearchOrSelectProject: 'Please search or select project'},
    'zh-cn': {selectAll: '-- 选择全部 --', pleaseSearchOrSelectProject: '请搜索或选择项目'}
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
.project_option {
  .el-icon-ksd-smart_mode_small,
  .el-icon-ksd-expert_mode_small {
    color: @text-disabled-color;
  }
}
</style>
