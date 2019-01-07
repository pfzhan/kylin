<template>
  <div class="accelerate-setting">
    <!-- 加速设置 -->
    <EditableBlock
      :header-content="$t('accePreference')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'accelerate-settings')"
      @submit="(scb, ecb) => handleSubmit('accelerate-settings', scb, ecb)"
      @cancel="() => handleReset('accelerate-settings')">
      <div class="setting-item">
        <div class="setting-label font-medium">{{$t('acceThreshold')}}</div>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
            v-model="form.batch_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc large"
           :class="{'disabled': !form.batch_enabled }">
          {{$t('notifyLeftTips')}}
          <b class="setting-value">{{project.threshold}}</b>
          <el-input
            size="small"
            class="acce-input setting-input"
            :disabled="!form.batch_enabled"
            v-model.number="form.threshold">
          </el-input>
          {{$t('notifyRightTips')}}
        </div>
      </div>
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
        <span class="setting-label font-medium">{{$t('emptyDataLoad')}}</span>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
            v-model="form.data_load_empty_notification_enabled"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
        <div class="setting-desc">{{$t('emptyDataLoadDesc')}}</div>
        <div class="split"></div>
        <span class="setting-label font-medium">{{$t('errorJob')}}</span>
        <span class="setting-value fixed">
          <el-switch
            class="ksd-switch"
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
              <el-form-item :prop="`job_notification_emails.${index}`" :rules="rules">
                <el-input v-model="form.job_notification_emails[index]"></el-input>
                <el-button icon="el-icon-plus" circle size="mini" @click="handleAddItem('job_notification_emails', index)"></el-button>
                <el-button icon="el-icon-minus" circle size="mini" @click="handleRemoveItem('job_notification_emails', index)"></el-button>
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
import { Component } from 'vue-property-decorator'

import { handleError } from '../../../util'
import { _getAccelerationSettings, _getJobAlertSettings } from './handler'
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
    auto_apply: false,
    batch_enabled: true,
    threshold: 20,
    job_error_notification_enabled: true,
    data_load_empty_notification_enabled: true,
    job_notification_emails: []
  }
  get rules () {
    return [
      { required: true, message: this.$t('pleaseInputEmail'), trigger: 'blur' },
      { type: 'email', message: this.$t('pleaseInputVaildEmail'), trigger: 'blur' }
    ]
  }
  mounted () {
    this.initForm()
  }
  initForm () {
    this.handleReset('accelerate-settings')
    this.handleReset('job-alert')
  }
  async handleSwitch (type, value) {
    try {
      switch (type) {
        case 'auto-merge': {
          const submitData = _getAccelerationSettings(this.project)
          submitData.batch_enabled = value
          await this.updateAccelerationSettings(submitData); break
        }
        case 'enable-empty-job-alert': {
          const submitData = _getJobAlertSettings(this.project)
          submitData.data_load_empty_notification_enabled = value
          await this.updateJobAlertSettings(submitData); break
        }
        case 'enable-error-job-alert': {
          const submitData = _getJobAlertSettings(this.project)
          submitData.job_error_notification_enabled = value
          await this.updateJobAlertSettings(submitData); break
        }
      }
      this.$emit('reload-setting')
    } catch (e) {
      handleError(e)
    }
  }
  handleReset (type) {
    switch (type) {
      case 'accelerate-settings': {
        this.form = { ...this.form, ..._getAccelerationSettings(this.project) }; break
      }
      case 'job-alert': {
        this.form = { ...this.form, ..._getJobAlertSettings(this.project, true) }; break
      }
    }
  }
  async handleSubmit (type, successCallback, errorCallback) {
    try {
      let isVaild = true
      switch (type) {
        case 'accelerate-settings': {
          const submitData = _getAccelerationSettings(this.form)
          await this.updateAccelerationSettings(submitData); break
        }
        case 'job-alert': {
          const submitData = _getJobAlertSettings(this.form)
          const isVaild = await this.$refs['job-alert'].validate()
          isVaild && await this.updateJobAlertSettings(submitData); break
        }
      }
      if (isVaild) {
        successCallback(); this.$emit('reload-setting')
      } else {
        errorCallback()
      }
    } catch (e) {
      errorCallback(); handleError(e)
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
  .item-value {
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
    transform: scale(0.8);
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
}
</style>
