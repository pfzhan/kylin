<template>
  <div class="accelerate-setting">
    <!-- 加速设置 -->
    <EditableBlock
      :header-content="$t('accePreference')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'accelerate-settings')"
      @submit="(scb, ecb) => handleSubmit('accelerate-settings', scb, ecb)"
      @cancel="() => handleReset('accelerate-settings')">
      <el-form ref="accelerate-setting-form" :model="form" :rules="accelerateRules">
        <div class="setting-item">
          <div class="setting-label font-medium">{{$t('acceThreshold')}}</div><span class="setting-value fixed">
            <el-switch
              size="small"
              v-model="form.tips_enabled"
              :active-text="$t('kylinLang.common.OFF')"
              :inactive-text="$t('kylinLang.common.ON')">
            </el-switch>
          </span>
          <div class="setting-desc large"
            :class="{'disabled': !form.tips_enabled }">
            {{$t('notifyLeftTips')}}
            <b class="setting-value">{{project.threshold}}</b>
            <el-form-item class="setting-input" :show-message="false" prop="threshold">
              <el-input
                size="small"
                class="acce-input"
                v-number="form.threshold"
                :disabled="!form.tips_enabled"
                v-model="form.threshold">
              </el-input>
            </el-form-item>
            {{$t('notifyRightTips')}}
          </div>
        </div>
      </el-form>
    </EditableBlock>
    <!-- 任务邮件通知设置 -->
    <EditableBlock
      :header-content="$t('jobAlert')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'job-alert')"
      @submit="(scb, ecb) => handleSubmit('job-alert', scb, ecb)"
      @cancel="() => handleReset('job-alert')">
      <!-- 空任务邮件通知 -->
      <div class="setting-item">
        <span class="setting-label font-medium">{{$t('emptyDataLoad')}}</span><span class="setting-value fixed">
          <el-switch
            size="small"
            v-model="form.data_load_empty_notification_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('emptyDataLoadDesc')}}</div>
        <div class="split"></div>
        <span class="setting-label font-medium">{{$t('errorJob')}}</span><span class="setting-value fixed">
          <el-switch
            size="small"
            v-model="form.job_error_notification_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('errorJobDesc')}}</div>
      </div>
      <div class="setting-item">
        <span class="setting-value">
          <span class="setting-label font-medium">{{$t('emails')}}</span>
          <template v-for="(email, index) in form.job_notification_emails">
            <span class="notice-email" v-if="email" :key="index">{{email}}</span>
            <span v-else-if="index === 0 && !email" :key="index">{{$t('noData')}}</span>
          </template>
        </span>
        <div class="setting-input">
          <el-form ref="job-alert" :model="form" size="small">
            <div class="item-value" v-for="(email, index) in form.job_notification_emails" :key="index">
              <span class="setting-label font-medium email-fix-top">{{$t('emails')}}</span>
              <el-form-item :prop="`job_notification_emails.${index}`" :rules="emailRules">
                <el-input v-model="form.job_notification_emails[index]" :placeholder="$t('pleaseInputEmail')"></el-input><el-button
                 icon="el-icon-ksd-add_2" circle size="mini" @click="handleAddItem('job_notification_emails', index)"></el-button><el-button
                  icon="el-icon-minus" class="ksd-ml-5" circle size="mini" @click="handleRemoveItem('job_notification_emails', index)" :disabled="form.job_notification_emails.length < 2"></el-button>
              </el-form-item>
            </div>
          </el-form>
        </div>
      </div>
    </EditableBlock>
  </div>
</template>

<script>
import Vue from 'vue'
import locales from './locales'
import { mapActions, mapGetters } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import { handleError } from '../../../util'
import { validate, _getAccelerationSettings, _getJobAlertSettings } from './handler'
import EditableBlock from '../../common/EditableBlock/EditableBlock.vue'
@Component({
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  methods: {
    ...mapActions({
      updateAccelerationSettings: 'UPDATE_ACCELERATION_SETTINGS',
      updateJobAlertSettings: 'UPDATE_JOB_ALERT_SETTINGS'
    })
  },
  components: {
    EditableBlock
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  locales
})
export default class SettingAdvanced extends Vue {
  form = {
    project: '',
    tips_enabled: true,
    threshold: 20,
    job_error_notification_enabled: true,
    data_load_empty_notification_enabled: true,
    job_notification_emails: []
  }
  get accelerateRules () {
    return {
      'threshold': [{ validator: validate['positiveNumber'], trigger: 'blur' }]
    }
  }
  get emailRules () {
    return [
      { required: true, message: this.$t('pleaseInputEmail'), trigger: 'blur' },
      { type: 'email', message: this.$t('pleaseInputVaildEmail'), trigger: 'blur' }
    ]
  }
  @Watch('form', { deep: true })
  @Watch('project', { deep: true })
  onFormChange () {
    const advanceSetting = this.isFormEdited(this.form, 'accelerate-settings') || this.isFormEdited(this.form, 'job-alert')
    this.$emit('form-changed', { advanceSetting })
  }
  mounted () {
    this.initForm()
  }
  initForm () {
    this.handleReset('accelerate-settings')
    this.handleReset('job-alert')
  }
  handleReset (type) {
    switch (type) {
      case 'accelerate-settings': {
        this.form = { ...this.form, ..._getAccelerationSettings(this.project) }
        this.$refs['accelerate-setting-form'].clearValidate()
        break
      }
      case 'job-alert': {
        this.form = { ...this.form, ..._getJobAlertSettings(this.project, true) }; break
      }
    }
  }
  async handleSubmit (type, successCallback, errorCallback) {
    try {
      switch (type) {
        case 'accelerate-settings': {
          if (await this.$refs['accelerate-setting-form'].validate()) {
            const submitData = _getAccelerationSettings(this.form)
            await this.updateAccelerationSettings(submitData); break
          } else {
            return errorCallback()
          }
        }
        case 'job-alert': {
          if (await this.$refs['job-alert'].validate()) {
            const submitData = _getJobAlertSettings(this.form)
            await this.updateJobAlertSettings(submitData); break
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
  handleAddItem (key, index) {
    this.form[key].splice(index + 1, 0, '')
  }
  handleRemoveItem (key, index) {
    if (this.form[key].length > 1) {
      this.form[key].splice(index, 1)
    }
  }
  isFormEdited (form, type) {
    const project = { ...this.project, alias: this.project.alias || this.project.project }
    switch (type) {
      case 'accelerate-settings':
        return JSON.stringify(_getAccelerationSettings(form)) !== JSON.stringify(_getAccelerationSettings(project))
      case 'job-alert':
        return JSON.stringify(_getJobAlertSettings(form, true)) !== JSON.stringify(_getJobAlertSettings(project, true))
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.accelerate-setting {
  .item-value .el-input {
    width: 200px;
  }
  .notice-email {
    font-size: 14px;
    color: @text-title-color;
    line-height: 18px;
    background-color: @grey-4;
    margin-right: 10px;
  }
  .item-value .el-button {
    margin-left: 10px;
  }
  .item-value .el-button+.el-button {
    margin-left: 2px;
  }
  .item-value:not(:last-child) {
    margin-bottom: 5px;
  }
  .item-value:not(:first-child) .setting-label {
    visibility: hidden;
  }
  .acce-input {
    width: 64px;
  }
  .el-form-item {
    margin-bottom: 0;
    display: inline-block;
  }
  .ksd-switch {
    transform: scale(0.91);
    transform-origin: left;
  }
  .email-fix-top {
    position: relative;
    top: 7px;
    vertical-align: top;
  }
  .split {
    margin-top: 15px;
  }
  .el-button.is-disabled {
    * {
      cursor: not-allowed;
    }
  }
}
</style>
