<template>
  <div class="basic-setting">
    <EditableBlock
      :headerContent="$t('basicInfo')"
      @submit="handleSubmitBasicInfo"
      @cancel="handleCancelBasicInfo">
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

    <EditableBlock
      :headerContent="$t('storageSettings')"
      :isEditable="false">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('storageQuota')}}</span>
        <span class="setting-value fixed">{{form.storageQuotaSize | dataSize}}</span>
        <div class="setting-desc">{{$t('storageQuotaDesc')}}</div>
      </div>
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('storageGarbage')}}</span>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
            v-model="form.isCheckStorageGarbage"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc large"
          :class="{ disabled: !form.isCheckStorageGarbage }">
          {{$t('storageGarbageDesc1')}}
          <b>5</b>
          {{$t('storageGarbageDesc1')}}
        </div>
      </div>
    </EditableBlock>

    <EditableBlock
      :headerContent="$t('pushdownSettings')"
      :isEditable="false">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownEngin')}}</span>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
            v-model="form.isPushdownEngine"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('pushdownEnginDesc')}}</div>
      </div>
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownRange')}}</span>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
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
      @submit="handleSubmitSegmentSettings"
      @cancel="handleCancelSegmentSettings">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('segmentMerge')}}</span>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
            v-model="form.isSegmentMerge"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('segmentMergeDesc')}}</div>
        <div class="field-item" :class="{ disabled: !form.isSegmentMerge }">
          <span class="setting-label font-medium">{{$t('autoMerge')}}</span>
          <span class="setting-value">
            {{form.autoMergeConfigs.map(autoMergeConfig => $t(autoMergeConfig)).join(', ')}}
          </span>
          <el-checkbox-group class="setting-input" v-model="form.autoMergeConfigs">
            <el-checkbox
              v-for="autoMergeType in autoMergeTypes"
              :key="autoMergeType"
              :label="autoMergeType">
              {{$t(autoMergeType)}}
            </el-checkbox>
          </el-checkbox-group>
        </div>
        <div class="field-item" :class="{ disabled: !form.isSegmentMerge }">
          <span class="setting-label font-medium">{{$t('volatile')}}</span>
          <span class="setting-value">
            {{form.volatileConfig.value}} {{$t(form.volatileConfig.type.toLowerCase())}}
          </span>
          <el-input class="setting-input" size="small" style="width: 100px;" v-model="form.volatileConfig.value"></el-input>
          <el-select
            class="setting-input"
            size="small"
            style="width: 100px;"
            v-model="form.volatileConfig.type"
            :placeholder="$t('kylinLang.common.pleaseChoose')">
            <el-option
              v-for="volatileType in volatileTypes"
              :key="volatileType"
              :label="$t(volatileType.toLowerCase())"
              :value="volatileType">
            </el-option>
          </el-select>
        </div>
      </div>
    </EditableBlock>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { handleError, handleSuccessAsync } from '../../../util'
import { projectTypeIcons, autoMergeTypes, volatileTypes } from './handler'
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
      updateProject: 'UPDATE_PROJECT',
      fetchQuotaInfo: 'GET_QUOTA_INFO'
    })
  },
  locales
})
export default class SettingBasic extends Vue {
  autoMergeTypes = autoMergeTypes
  volatileTypes = volatileTypes
  form = {
    isFileBased: true,
    isSourceSampling: true,
    isPushdownEngine: true,
    isPushdownRange: true,
    isSegmentMerge: true,
    isCheckStorageGarbage: true,
    storageQuotaSize: 0,
    autoMergeConfigs: [ 'WEEK', 'MONTH' ],
    volatileConfig: {
      value: 0,
      type: 'DAY'
    },
    name: '',
    description: ''
  }
  storageQuotaSize = 0
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
    this.form.storageQuotaSize = this.storageQuotaSize
  }
  async mounted () {
    await this.getQuotaInfo()
    this.initForm()
  }
  async getQuotaInfo () {
    try {
      const res = await this.fetchQuotaInfo({project: this.project.name})
      const resData = await handleSuccessAsync(res)
      this.storageQuotaSize = resData.storage_quota_size
    } catch (e) {
      handleError(e)
    }
  }
  async handleSubmitBasicInfo (successCallback) {
    try {
      await this.updateProject(this.submitData)
      successCallback()
    } catch (e) {
      handleError(e)
    }
  }
  handleCancelBasicInfo () {
    this.form.name = this.project.name
    this.form.description = this.project.description
  }
  handleSubmitSegmentSettings (successCallback) {
    // API
    setTimeout(() => {
      successCallback()
    }, 1000)
  }
  handleCancelSegmentSettings () {
    // API
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
  .ksd-switch {
    transform: scale(0.8);
    transform-origin: left;
  }
}
</style>
