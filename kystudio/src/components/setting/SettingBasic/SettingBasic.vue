<template>
  <div class="basic-setting">
    <!-- 项目基本设置 -->
    <EditableBlock
      :header-content="$t('basicInfo')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'basic-info')"
      @submit="(scb, ecb) => handleSubmit('basic-info', scb, ecb)"
      @cancel="() => handleReset('basic-info')">
      <div class="setting-item">
        <div class="setting-label font-medium" style="width: 92px;">{{$t('projectName')}}</div>
        <div class="setting-value fixed">{{project.alias || project.project}}</div>
        <!-- <el-input class="setting-input" size="small" style="width: 250px;" v-model="form.alias"></el-input> -->
      </div>
      <div class="setting-item">
        <div class="setting-label font-medium" style="width: 92px;">{{$t('projectType')}}</div>
        <div class="setting-value fixed"><i :class="projectIcon"></i>{{$t(project.maintain_model_type)}}</div>
      </div>
      <div class="setting-item clearfix">
        <div class="setting-label font-medium" style="width: 92px;">{{$t('description')}}</div>
        <div class="setting-value">{{project.description}}</div>
        <el-input class="setting-input" type="textarea" size="small" v-model="form.description"></el-input>
      </div>
    </EditableBlock>
    <!-- 项目存储设置 -->
    <EditableBlock
      :header-content="$t('storageSettings')"
      :isEditable="false">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('storageQuota')}}</span>
        <span class="setting-value fixed">{{form.storage_quota_size | dataSize}}</span>
        <div class="setting-desc">{{$t('storageQuotaDesc')}}</div>
      </div>
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('storageGarbage')}}</span>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
            v-model="form.storage_garbage"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @input="value => handleSwitch('pushdown-engine', value)">
          </el-switch>
        </span>
        <div class="setting-desc large"
          :class="{ disabled: !form.storage_garbage }">
          {{$t('storageGarbageDesc1')}}
          <b>5</b>
          {{$t('storageGarbageDesc1')}}
        </div>
      </div>
    </EditableBlock>
    <!-- 下压查询设置 -->
    <EditableBlock
      :header-content="$t('pushdownSettings')"
      :isEditable="false">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownEngine')}}</span>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
            v-model="form.push_down_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @input="value => handleSwitch('pushdown-engine', value)">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('pushdownEngineDesc')}}</div>
      </div>
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownRange')}}</span>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
            v-model="form.push_down_range_limited"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @input="value => handleSwitch('pushdown-range', value)">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('pushdownRangeDesc')}}</div>
      </div>
    </EditableBlock>
    <!-- Segment设置 -->
    <EditableBlock
      :header-content="$t('segmentSettings')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'segment-settings')"
      @submit="(scb, ecb) => handleSubmit('segment-settings', scb, ecb)"
      @cancel="() => handleReset('segment-settings')">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('segmentMerge')}}</span>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
            v-model="form.auto_merge_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('segmentMergeDesc')}}</div>
        <div class="field-item" :class="{ disabled: !form.auto_merge_enabled }">
          <span class="setting-label font-medium">{{$t('autoMerge')}}</span>
          <span class="setting-value">
            {{form.auto_merge_time_ranges.map(autoMergeConfig => $t(autoMergeConfig)).join(', ')}}
          </span>
          <el-checkbox-group class="setting-input" :value="form.auto_merge_time_ranges" @input="handleCheckMergeRanges" :disabled="!form.auto_merge_enabled">
            <el-checkbox
              v-for="autoMergeType in autoMergeTypes"
              :key="autoMergeType"
              :label="autoMergeType">
              {{$t(autoMergeType)}}
            </el-checkbox>
          </el-checkbox-group>
        </div>
        <div class="field-item" :class="{ disabled: !form.auto_merge_enabled }">
          <span class="setting-label font-medium">{{$t('volatile')}}</span>
          <span class="setting-value">
            {{form.volatile_range.volatile_range_number}} {{$t(form.volatile_range.volatile_range_type.toLowerCase())}}
          </span>
          <el-input class="setting-input" size="small" style="width: 100px;" v-model.number="form.volatile_range.volatile_range_number" :disabled="!form.auto_merge_enabled"></el-input>
          <el-select
            class="setting-input"
            size="small"
            style="width: 100px;"
            v-model="form.volatile_range.volatile_range_type"
            :disabled="!form.auto_merge_enabled"
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
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('retentionThreshold')}}</span>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
            v-model="form.retention_range.retention_range_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('retentionThresholdDesc')}}</div>
        <div class="field-item" :class="{ disabled: !form.retention_range.retention_range_enabled }">
          <span class="setting-label font-medium">{{$t('retentionThreshold')}}</span>
          <span class="setting-value">
            {{form.retention_range.retention_range_number}} {{$t(form.retention_range.retention_range_type.toLowerCase())}}
          </span>
          <el-input class="setting-input" size="small" style="width: 100px;" v-model.number="form.retention_range.retention_range_number" :disabled="!form.retention_range.retention_range_enabled"></el-input>
          <span class="setting-input">{{$t(retentionRangeScale)}}</span>
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
import { handleError } from '../../../util'
import { projectTypeIcons, autoMergeTypes, volatileTypes, retentionTypes, initialFormValue, _getProjectGeneralInfo, _getSegmentSettings, _getPushdownConfig, _getStorageQuota, _getRetentionRangeScale } from './handler'
import EditableBlock from '../../common/EditableBlock/EditableBlock.vue'

@Component({
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  components: {
    EditableBlock
  },
  methods: {
    ...mapActions({
      updateProjectGeneralInfo: 'UPDATE_PROJECT_GENERAL_INFO',
      updateSegmentConfig: 'UPDATE_SEGMENT_CONFIG',
      updatePushdownConfig: 'UPDATE_PUSHDOWN_CONFIG',
      updateStorageQuota: 'UPDATE_STORAGE_QUOTA'
    })
  },
  locales
})
export default class SettingBasic extends Vue {
  autoMergeTypes = autoMergeTypes
  volatileTypes = volatileTypes
  retentionTypes = retentionTypes
  form = initialFormValue
  storageQuotaSize = 0
  get projectIcon () {
    return projectTypeIcons[this.project.maintain_model_type]
  }
  get retentionRangeScale () {
    return _getRetentionRangeScale(this.form).toLowerCase()
  }
  initForm () {
    this.handleReset('basic-info')
    this.handleReset('segment-settings')
    this.handleReset('pushdown-settings')
    this.handleReset('storage-quota')
  }
  async mounted () {
    this.initForm()
  }
  handleCheckMergeRanges (value) {
    if (value.length > 0) {
      this.form.auto_merge_time_ranges = value
    }
  }
  async handleSwitch (type, value) {
    try {
      switch (type) {
        case 'auto-merge': {
          const submitData = _getSegmentSettings(this.project)
          submitData.auto_merge_enabled = value
          await this.updateSegmentConfig(submitData); break
        }
        case 'auto-retention': {
          const submitData = _getSegmentSettings(this.project)
          submitData.retention_range.retention_range_enabled = value
          await this.updateSegmentConfig(submitData); break
        }
        case 'pushdown-range': {
          const submitData = _getPushdownConfig(this.project)
          submitData.push_down_range_limited = value
          await this.updatePushdownConfig(submitData); break
        }
        case 'pushdown-engine': {
          const submitData = _getPushdownConfig(this.project)
          submitData.push_down_enabled = value
          await this.updatePushdownConfig(submitData); break
        }
        case 'storage-garbage': {
          const submitData = _getStorageQuota(this.project)
          submitData.storage_garbage = value
          await this.updateStorageQuota(submitData); break
        }
      }
      this.$emit('reload-setting')
      this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
    } catch (e) {
      handleError(e)
    }
  }
  async handleSubmit (type, successCallback, errorCallback) {
    try {
      switch (type) {
        case 'basic-info': {
          const submitData = _getProjectGeneralInfo(this.form)
          await this.updateProjectGeneralInfo(submitData); break
        }
        case 'segment-settings': {
          const submitData = _getSegmentSettings(this.form, this.project)
          await this.updateSegmentConfig(submitData); break
        }
      }
      successCallback()
      this.$emit('reload-setting')
      this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
    } catch (e) {
      errorCallback()
      handleError(e)
    }
  }
  handleReset (type) {
    switch (type) {
      case 'basic-info': {
        this.form = { ...this.form, ..._getProjectGeneralInfo(this.project) }; break
      }
      case 'segment-settings': {
        this.form = { ...this.form, ..._getSegmentSettings(this.project) }; break
      }
      case 'pushdown-settings': {
        this.form = { ...this.form, ..._getPushdownConfig(this.project) }; break
      }
      case 'storage-quota': {
        this.form = { ...this.form, ..._getStorageQuota(this.project) }; break
      }
    }
  }
  isFormEdited (form, type) {
    const project = { ...this.project, alias: this.project.alias || this.project.project }
    switch (type) {
      case 'basic-info':
        return JSON.stringify(_getProjectGeneralInfo(form)) !== JSON.stringify(_getProjectGeneralInfo(project))
      case 'segment-settings':
        return JSON.stringify(_getSegmentSettings(form)) !== JSON.stringify(_getSegmentSettings(project))
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.basic-setting {
  .clearfix .setting-value,
  .clearfix .setting-input {
    width: calc(~'100% - 106px');
  }
  .ksd-switch {
    transform: scale(0.8);
    transform-origin: left;
  }
  .el-icon-ksd-expert_mode,
  .el-icon-ksd-smart_mode {
    margin-right: 5px;
  }
}
</style>
