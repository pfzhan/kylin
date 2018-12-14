<template>
  <div class="basic-setting">
    <EditableBlock
      :headerContent="$t('basicInfo')"
      @submit="handleSubmitBasic"
      @cancel="handleCancelBasic">
      <div class="setting-item">
        <div class="setting-label font-medium">{{$t('projectName')}}</div>
        <div class="setting-value">{{form.name}}</div>
        <el-input class="setting-input" size="small" style="width: 250px;" v-model="form.name"></el-input>
      </div>
      <div class="setting-item">
        <div class="setting-label font-medium">{{$t('projectType')}}</div>
        <div class="setting-value fixed"><i :class="projectInfo.icon"></i>{{projectInfo.type}}</div>
      </div>
      <div class="setting-item clearfix">
        <div class="setting-label font-medium">{{$t('description')}}</div>
        <div class="setting-value">{{form.description}}</div>
        <el-input class="setting-input" type="textarea" size="small" v-model="form.description"></el-input>
      </div>
    </EditableBlock>

    <!-- <EditableBlock
      :headerContent="$t('basicInfo')"
      @submit="handleSubmitBasic"
      @cancel="handleCancelBasic">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('fileBased')}}</span>
        <span class="setting-value fixed">
          <el-switch
            v-model="form.isFileBased"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('fileBasedDesc')}}</div>
      </div>
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('sourceSampling')}}</span>
        <span class="setting-value fixed">
          <el-switch
            v-model="form.isSourceSampling"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('sourceSamplingDesc')}}</div>
      </div>
    </EditableBlock> -->

    <EditableBlock
      :headerContent="$t('pushdownSettings')"
      @submit="handleSubmitBasic"
      @cancel="handleCancelBasic">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownEngin')}}</span>
        <span class="setting-value fixed">
          <el-switch
            v-model="form.isPushdownEngine"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
      </div>
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownRange')}}</span>
        <span class="setting-value fixed">
          <el-switch
            v-model="form.isPushdownRange"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('pushdownRangeDesc')}}</div>
      </div>
    </EditableBlock>

    <EditableBlock
      :headerContent="$t('segmentSettings')"
      @submit="handleSubmitBasic"
      @cancel="handleCancelBasic">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('segmentMerge')}}</span>
        <span class="setting-value fixed">
          <el-switch
            v-model="form.isSegmentMerge"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('segmentMergeDesc')}}</div>
        <SegmentMerge v-model="form"></SegmentMerge>
      </div>
    </EditableBlock>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { handleError } from '../../../util'
import { projectTypeIcons } from './handler'
import EditableBlock from '../../common/EditableBlock/EditableBlock.vue'
import SegmentMerge from '../SegmentMerge/SegmentMerge.vue'

@Component({
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  components: {
    EditableBlock,
    SegmentMerge
  },
  methods: {
    ...mapActions({
      updateProject: 'UPDATE_PROJECT'
    })
  },
  locales
})
export default class SettingBasic extends Vue {
  form = {
    isFileBased: true,
    isSourceSampling: true,
    isPushdownEngine: true,
    isPushdownRange: true,
    isSegmentMerge: true,
    autoMergeConfigs: [ 'WEEK', 'MONTH' ],
    volatileConfig: {
      value: 0,
      type: 'DAY'
    },
    name: '',
    description: ''
  }
  get projectInfo () {
    const name = this.project.name
    const type = this.$t(this.project.maintain_model_type)
    const description = this.project.description
    const icon = projectTypeIcons[this.project.maintain_model_type]
    return { name, type, description, icon }
  }

  handleSubmitBasic (successCallback) {
    setTimeout(() => {
      successCallback()
    }, 5000)
  }
  handleCancelBasic () {
    this.initForm()
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
  .clearfix .setting-value,
  .clearfix .setting-input {
    width: calc(~'100% - 92px');
  }
}
</style>
