<template>
  <div class="basic-setting">
    <el-popover
      trigger="hover"
      ref="description"
      placement="bottom"
      popper-class="project-edit-description"
      v-model="popover['description']"
      :title="$t('description')"
      @after-leave="handleHidePopover('description')">
      <el-input class="popover-input" size="small" v-model="form.description" type="textarea"></el-input>
      <div style="text-align: right;">
        <el-button type="info" size="mini" text @click="handleHidePopover('description')" :disabled="isLoading">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="mini" text @click="handleSubmit('description')" :loading="isLoading">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
    </el-popover>

    <div class="project-setting project-basic">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('projectName')}}</span>
        <span class="setting-value">{{projectInfo.name}}</span>
      </div>
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('projectType')}}</span>
        <span class="setting-value">
          <i :class="projectInfo.icon"></i>
          {{projectInfo.type}}
        </span>
      </div>
      <div class="setting-item clearfix">
        <span class="setting-label font-medium">{{$t('description')}}</span>
        <span class="setting-value">
          {{projectInfo.description}}
          <i class="value-action el-icon-ksd-table_edit" v-popover:description></i>
        </span>
      </div>
    </div>

    <div class="project-setting project-switch">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('fileBased')}}</span>
        <span class="setting-value">
          <el-switch
            v-model="form.isFileBased"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
      </div>
      <div class="setting-desc">{{$t('fileBasedDesc')}}</div>
      <div class="hr"></div>
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('sourceSampling')}}</span>
        <span class="setting-value">
          <el-switch
            v-model="form.isSourceSampling"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
      </div>
      <div class="setting-desc">{{$t('sourceSamplingDesc')}}</div>
    </div>

    <div class="project-setting project-switch">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownEngin')}}</span>
        <span class="setting-value">
          <el-switch
            v-model="form.isPushdownEngine"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
      </div>
      <div class="hr"></div>
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownRange')}}</span>
        <span class="setting-value">
          <el-switch
            v-model="form.isPushdownRange"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
      </div>
      <div class="setting-desc">{{$t('pushdownRangeDesc')}}</div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { handleError } from '../../../util'
import { projectTypeIcons } from './handler'

@Component({
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  methods: {
    ...mapActions({
      updateProject: 'UPDATE_PROJECT'
    })
  },
  locales
})
export default class SettingBasic extends Vue {
  isLoading = false
  form = {
    isFileBased: true,
    isSourceSampling: true,
    isPushdownEngine: true,
    isPushdownRange: true,
    name: '',
    description: ''
  }
  popover = {
    name: false,
    description: false
  }
  get projectInfo () {
    const name = this.project.name
    const type = this.$t(this.project.maintain_model_type)
    const description = this.project.description
    const icon = projectTypeIcons[this.project.maintain_model_type]
    return { name, type, description, icon }
  }
  get submitData () {
    const { form } = this
    const projectString = JSON.stringify(this.project)
    const newProject = JSON.parse(projectString)

    form.name && (newProject.name = form.name)
    form.description && (newProject.description = form.description)

    return {
      name: newProject.name,
      desc: JSON.stringify(newProject)
    }
  }
  initForm () {
    this.form.name = this.project.name
    this.form.description = this.project.description
  }
  handleHidePopover (type) {
    this.popover[type] = false
    this.form[type] = this.project[type]
  }
  async handleSubmit (type) {
    try {
      this.isLoading = true
      await this.updateProject(this.submitData)
      this.handleHidePopover(type)
      this.isLoading = false
    } catch (e) {
      handleError(e)
    }
  }
  mounted () {
    this.initForm()
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.basic-setting {
  padding: 5px 0;
  .project-basic .setting-label {
    width: 93px;
  }
  .project-switch .setting-label {
    width: 123px;
  }
  .clearfix .setting-value {
    margin-left: 103px;
  }
}

.project-edit-description {
  width: auto !important;
  .popover-input {
    margin-bottom: 13px;
    textarea {
      min-height: 80px !important;
      width: 300px;
    }
  }
  .el-popover__title {
    font-weight: 500;
  }
}
</style>
