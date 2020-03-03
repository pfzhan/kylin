<template>
  <div class="basic-setting">
    <!-- 项目基本设置 -->
    <EditableBlock
      :header-content="$t('basicInfo')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'basic-info')"
      :is-reset="false"
      @submit="(scb, ecb) => handleSubmit('basic-info', scb, ecb)">
      <div class="setting-item">
        <div class="setting-label font-medium">{{$t('projectName')}}</div>
        <div class="setting-value fixed">{{project.alias || project.project}}</div>
        <!-- <el-input class="setting-input" size="small" style="width: 250px;" v-model="form.alias"></el-input> -->
      </div>
      <div class="setting-item">
        <div class="setting-label font-medium">{{$t('projectType')}}</div>
        <div class="setting-value fixed"><i :class="projectIcon"></i>{{$t(project.maintain_model_type)}}</div>
      </div>
      <div class="setting-item" v-if="project.maintain_model_type==='MANUAL_MAINTAIN'">
        <span class="setting-label font-medium">{{$t('enableSemiAutomatic')}}</span>
        <span class="setting-value fixed">
          <el-switch
            size="small"
            v-model="form.semi_automatic_mode"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('enableSemiAutomaticDesc')}}</div>
      </div>
      <div class="setting-item clearfix">
        <div class="setting-label font-medium">{{$t('description')}}</div>
        <div class="setting-value">{{project.description}}</div>
        <el-input class="setting-input" :rows="3" type="textarea" size="small" v-model="form.description"></el-input>
      </div>
    </EditableBlock>
    <!-- 项目存储设置 -->
    <EditableBlock
      :header-content="$t('storageSettings')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'storage-quota')"
      @submit="(scb, ecb) => handleSubmit('storage-quota', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('storage-quota', scb, ecb)">
      <el-form ref="setting-storage-quota" :model="form" :rules="storageQuota">
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('storageQuota')}}</span>
          <span class="setting-value fixed">{{form.storage_quota_size | dataSize}}</span>
          <div class="setting-desc">{{$t('storageQuotaDesc')}}</div>
        </div>
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('storageGarbage')}}</span>
          <div class="setting-desc large">
            <span>{{$t('storageGarbageDesc1')}}</span>
            <el-select
              class="setting-input"
              size="small"
              style="width: 100px;"
              v-model="form.frequency_time_window"
              :placeholder="$t('kylinLang.common.pleaseChoose')">
              <el-option
                v-for="lowUsageStorageType in lowUsageStorageTypes"
                :key="lowUsageStorageType"
                :label="$t(lowUsageStorageType+'1')"
                :value="lowUsageStorageType">
              </el-option>
            </el-select>
            <span>{{$t('storageGarbageDesc2')}}</span>
            <el-form-item class="setting-input" :show-message="false" prop="low_frequency_threshold">
              <el-input-number size="small" style="width: 100px;" :max="9999" v-number="form.low_frequency_threshold" v-model="form.low_frequency_threshold" :controls="false"></el-input-number>
            </el-form-item>
            <span>{{$store.state.project.isSemiAutomatic ? $t('storageGarbageDesc3ForSemiAutomatic') : $t('storageGarbageDesc3')}}</span>
          </div>
        </div>
      </el-form>
    </EditableBlock>
    <!-- 下压查询设置 -->
    <EditableBlock
      :header-content="$t('pushdownSettings')"
      :isEditable="false">
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownEngine')}}</span><span class="setting-value fixed">
          <el-switch
            size="small"
            v-model="form.push_down_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @input="value => handleSwitch('pushdown-engine', value)">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('pushdownEngineDesc')}}</div>
      </div>
      <!-- <div class="setting-item">
        <span class="setting-label font-medium">{{$t('pushdownRange')}}</span><span class="setting-value fixed">
          <el-switch
            size="small"
            v-model="form.push_down_range_limited"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')"
            @input="value => handleSwitch('pushdown-range', value)">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('pushdownRangeDesc')}}</div>
      </div> -->
    </EditableBlock>
    <!-- Segment设置 -->
    <EditableBlock
      :header-content="$t('segmentSettings')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'segment-settings')"
      @submit="(scb, ecb) => handleSubmit('segment-settings', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('segment-settings', scb, ecb)">
      <el-form ref="segment-setting-form" :model="form" :rules="rules">
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('segmentMerge')}}</span><span class="setting-value fixed">
            <el-switch
              size="small"
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
            <el-form-item class="setting-input" :show-message="false" prop="volatile_range.volatile_range_number">
              <el-input size="small" style="width: 100px;" v-number="form.volatile_range.volatile_range_number" v-model="form.volatile_range.volatile_range_number" :disabled="!form.auto_merge_enabled"></el-input>
            </el-form-item><el-select
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
            <div class="setting-desc">{{$t('volatileTip')}}</div>
          </div>
        </div>
        <div class="setting-item">
          <span class="setting-label font-medium">{{$t('retentionThreshold')}}</span><span class="setting-value fixed">
            <el-switch
              size="small"
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
            <el-form-item class="setting-input" :show-message="false" prop="retention_range.retention_range_number">
              <el-input size="small" style="width: 100px;" v-number="form.retention_range.retention_range_number" v-model="form.retention_range.retention_range_number" :disabled="!form.retention_range.retention_range_enabled"></el-input>
            </el-form-item>
            <span class="setting-input">{{$t(retentionRangeScale)}}</span>
          </div>
        </div>
      </el-form>
    </EditableBlock>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import { handleError, handleSuccessAsync, kapConfirm } from '../../../util'
import { projectTypeIcons, lowUsageStorageTypes, autoMergeTypes, volatileTypes, validate, retentionTypes, initialFormValue, _getProjectGeneralInfo, _getSegmentSettings, _getPushdownConfig, _getStorageQuota, _getRetentionRangeScale } from './handler'
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
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      updateProjectGeneralInfo: 'UPDATE_PROJECT_GENERAL_INFO',
      updateSegmentConfig: 'UPDATE_SEGMENT_CONFIG',
      updatePushdownConfig: 'UPDATE_PUSHDOWN_CONFIG',
      updateStorageQuota: 'UPDATE_STORAGE_QUOTA',
      resetConfig: 'RESET_PROJECT_CONFIG'
    })
  },
  locales
})
export default class SettingBasic extends Vue {
  lowUsageStorageTypes = lowUsageStorageTypes
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
  get rules () {
    return {
      'volatile_range.volatile_range_number': [{ validator: validate['positiveNumber'], trigger: 'change' }],
      'retention_range.retention_range_number': [{ validator: validate['positiveNumber'], trigger: 'change' }]
    }
  }
  get storageQuota () {
    return {
      'low_frequency_threshold': [{ validator: validate['storageQuotaNum'], trigger: 'change' }]
    }
  }
  @Watch('form', { deep: true })
  @Watch('project', { deep: true })
  onFormChange () {
    const basicSetting = this.isFormEdited(this.form, 'basic-info') || this.isFormEdited(this.form, 'segment-settings') || this.isFormEdited(this.form, 'storage-quota')
    this.$emit('form-changed', { basicSetting })
  }
  initForm () {
    this.handleInit('basic-info')
    this.handleInit('segment-settings')
    this.handleInit('pushdown-settings')
    this.handleInit('storage-quota')
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
          if (!submitData.semi_automatic_mode) {
            await kapConfirm(this.$t('turnOffTips'), {confirmButtonText: this.$t('confirmClose'), type: '', dangerouslyUseHTMLString: true})
            await this.updateProjectGeneralInfo(submitData); break
          } else {
            await this.updateProjectGeneralInfo(submitData); break
          }
        }
        case 'segment-settings': {
          if (await this.$refs['segment-setting-form'].validate()) {
            const submitData = _getSegmentSettings(this.form, this.project)
            await this.updateSegmentConfig(submitData); break
          } else {
            return errorCallback()
          }
        }
        case 'storage-quota': {
          if (await this.$refs['setting-storage-quota'].validate()) {
            const submitData = _getStorageQuota(this.form, this.project)
            await this.updateStorageQuota(submitData); break
          } else {
            return errorCallback()
          }
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
  handleInit (type) {
    switch (type) {
      case 'basic-info': {
        this.form = { ...this.form, ..._getProjectGeneralInfo(this.project) }; break
      }
      case 'segment-settings': {
        this.form = { ...this.form, ..._getSegmentSettings(this.project) }
        this.$refs['segment-setting-form'].clearValidate()
        break
      }
      case 'pushdown-settings': {
        this.form = { ...this.form, ..._getPushdownConfig(this.project) }; break
      }
      case 'storage-quota': {
        this.form = { ...this.form, ..._getStorageQuota(this.project) }; break
      }
    }
  }
  async handleResetForm (type, successCallback, errorCallback) {
    try {
      switch (type) {
        case 'segment-settings': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'segment_config'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getSegmentSettings(data) }
          this.$refs['segment-setting-form'].clearValidate()
          break
        }
        case 'storage-quota': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'garbage_cleanup_config'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getStorageQuota(data) }
          this.$refs['setting-storage-quota'].clearValidate()
          break
        }
      }
      successCallback()
      this.$emit('reload-setting')
      this.$message({ type: 'success', message: this.$t('kylinLang.common.resetSuccess') })
    } catch (e) {
      errorCallback()
      handleError(e)
    }
  }
  isFormEdited (form, type) {
    const project = { ...this.project, alias: this.project.alias || this.project.project }
    switch (type) {
      case 'basic-info':
        return JSON.stringify(_getProjectGeneralInfo(form)) !== JSON.stringify(_getProjectGeneralInfo(project))
      case 'segment-settings':
        return JSON.stringify(_getSegmentSettings(form)) !== JSON.stringify(_getSegmentSettings(project))
      case 'storage-quota':
        return JSON.stringify(_getStorageQuota(form)) !== JSON.stringify(_getStorageQuota(project))
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.basic-setting {
  .clearfix .setting-value,
  .clearfix .setting-input {
    width: calc(~'100% - 114px');
  }
  .ksd-switch {
    transform: scale(0.91);
    transform-origin: left;
  }
  .el-icon-ksd-expert_mode_small,
  .el-icon-ksd-smart_mode_small {
    margin-right: 5px;
  }
}

</style>
