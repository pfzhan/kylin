<template>
  <div class="accelerate-setting">
    <!-- 加速设置 -->
    <EditableBlock
      :header-content="$t('accePreference')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'accelerate-settings')"
      @submit="(scb, ecb) => handleSubmit('accelerate-settings', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('accelerate-settings', scb, ecb)">
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
    <!-- 默认数据库设置 -->
    <EditableBlock
      :header-content="$t('defaultDBTitle')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'defaultDB-settings')"
      :is-reset="false"
      @submit="(scb, ecb) => handleSubmit('defaultDB-settings', scb, ecb)">
      <el-form ref="setDefaultDB" :model="form" :rules="setDefaultDBRules">
        <div class="setting-item">
          <div class="setting-label font-medium">{{$t('defaultDB')}}</div>
          <el-form-item prop="default_database">
            <el-select v-model="form.default_database">
              <el-option
                v-for="item in dbList"
                :key="item"
                :label="item"
                :value="item">
              </el-option>
            </el-select>
          </el-form-item>
          <div class="setting-desc">
            <p>{{$t('defaultDBNote1')}}</p>
            <p class="warning"><i class="el-icon-ksd-alert"></i>{{$t('defaultDBNote2')}}</p>
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
      @cancel="(scb, ecb) => handleResetForm('job-alert', scb, ecb)">
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
    <!-- YARN 资源队列 -->
    <EditableBlock
      :header-content="$t('yarnQueue')"
      :is-keep-editing="true"
      :is-edited="isFormEdited(form, 'yarn-name')"
      :is-editable="userType"
      @submit="(scb, ecb) => handleSubmit('yarn-name', scb, ecb)"
      @cancel="(scb, ecb) => handleResetForm('yarn-name', scb, ecb)">
      <el-form ref="yarn-setting-form" :model="form" :rules="yarnQueueRules" :inline-message="true">
        <div class="setting-item">
          <div class="setting-label font-medium">{{$t('yarnQueue')}}</div>
          <el-form-item prop="yarn_queue">
            <el-input
              size="small"
              class="yarn-name-input"
              :disabled="!userType"
              v-model.trim="form.yarn_queue">
            </el-input>
          </el-form-item>
          <div class="setting-desc">
            <p>{{$t('yarnQueueTip')}}</p>
            <p class="warning"><i class="el-icon-ksd-alert"></i>{{$t('yarnQueueWarn')}}</p>
          </div>
        </div>
      </el-form>
    </EditableBlock>
  </div>
</template>

<script>
import Vue from 'vue'
import locales from './locales'
import { mapActions, mapGetters, mapState } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'

import { handleError, handleSuccessAsync } from '../../../util'
import { validate, _getAccelerationSettings, _getJobAlertSettings, _getDefaultDBSettings, _getYarnNameSetting } from './handler'
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
      updateJobAlertSettings: 'UPDATE_JOB_ALERT_SETTINGS',
      resetConfig: 'RESET_PROJECT_CONFIG',
      updateDefaultDBSettings: 'UPDATE_DEFAULT_DB_SETTINGS',
      fetchDatabases: 'FETCH_DATABASES',
      updateYarnQueue: 'UPDATE_YARN_QUEUE'
    })
  },
  components: {
    EditableBlock
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'currentProjectData'
    ]),
    ...mapState({
      currentUser: state => state.user.currentUser
    })
  },
  locales
})
export default class SettingAdvanced extends Vue {
  dbList = []
  form = {
    project: '',
    tips_enabled: true,
    threshold: 20,
    job_error_notification_enabled: true,
    data_load_empty_notification_enabled: true,
    job_notification_emails: [],
    default_database: this.$store.state.project.projectDefaultDB || '',
    yarn_queue: this.$store.state.project.yarn_queue || ''

  }
  get accelerateRules () {
    return {
      'threshold': [{ validator: validate['positiveNumber'], trigger: 'blur' }]
    }
  }
  get setDefaultDBRules () {
    return {
      'default_database': { required: true, message: this.$t('kylinLang.common.pleaseSelect'), trigger: 'change' }
    }
  }
  get emailRules () {
    return [
      { required: true, message: this.$t('pleaseInputEmail'), trigger: 'blur' },
      { type: 'email', message: this.$t('pleaseInputVaildEmail'), trigger: 'blur' }
    ]
  }
  get yarnQueueRules () {
    return {
      'yarn_queue': [{ validator: validate['validateYarnName'], message: [this.$t('yarnIsEmpty'), this.$t('yarnFormat')], trigger: 'blur' }]
    }
  }
  get userType () {
    return this.currentUser.authorities.map(user => user.authority).some((u) => u === 'ROLE_ADMIN')
  }
  @Watch('form', { deep: true })
  @Watch('project', { deep: true })
  onFormChange () {
    const advanceSetting = this.isFormEdited(this.form, 'accelerate-settings') || this.isFormEdited(this.form, 'job-alert') || this.isFormEdited(this.form, 'defaultDB-settings') || this.isFormEdited(this.form, 'yarn-name')
    this.$emit('form-changed', { advanceSetting })
  }
  async loadDataBases () {
    // 按单数据源来处理
    const { override_kylin_properties: overrideKylinProperties } = this.currentProjectData || {}
    let currentSourceTypes = overrideKylinProperties && overrideKylinProperties['kylin.source.default']
      ? [+overrideKylinProperties['kylin.source.default']]
      : []
    const responses = await this.fetchDatabases({ projectName: this.currentSelectedProject, sourceType: currentSourceTypes[0] })
    this.dbList = await handleSuccessAsync(responses)
    if (this.dbList.indexOf('DEFAULT') === -1) {
      this.dbList.push('DEFAULT')
    }
    this.form.default_database = this.$store.state.project.projectDefaultDB
  }
  mounted () {
    this.loadDataBases()
    this.initForm()
  }
  initForm () {
    this.handleInit('accelerate-settings')
    this.handleInit('job-alert')
    this.handleInit('defaultDB-settings')
    this.handleInit('yarn-name')
  }
  handleInit (type) {
    switch (type) {
      case 'accelerate-settings': {
        this.form = { ...this.form, ..._getAccelerationSettings(this.project) }
        this.$refs['accelerate-setting-form'].clearValidate()
        break
      }
      case 'job-alert': {
        this.form = { ...this.form, ..._getJobAlertSettings(this.project, true) }; break
      }
      case 'defaultDB-settings': {
        this.form = { ...this.form, ..._getDefaultDBSettings(this.project) }; break
      }
      case 'yarn-name': {
        this.form = { ...this.form, ..._getYarnNameSetting(this.project) }; break
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
        case 'defaultDB-settings': {
          // 需要二次确认
          this.$confirm(this.$t('confirmDefaultDBContent', {dbName: this.form.default_database}), this.$t('confirmDefaultDBTitle'), {
            confirmButtonText: this.$t('kylinLang.common.submit'),
            cancelButtonText: this.$t('kylinLang.common.cancel'),
            type: 'warning'
          }).then(async () => {
            if (await this.$refs['setDefaultDB'].validate()) {
              const submitData = _getDefaultDBSettings(this.form)
              await this.updateDefaultDBSettings({project: submitData.project, default_database: submitData.defaultDatabase})
              successCallback()
              this.$emit('reload-setting')
              this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
            } else {
              return errorCallback()
            }
          }).catch(() => {
            return errorCallback()
          })
          break
        }
        case 'job-alert': {
          if (await this.$refs['job-alert'].validate()) {
            const submitData = _getJobAlertSettings(this.form)
            await this.updateJobAlertSettings(submitData); break
          } else {
            return errorCallback()
          }
        }
        case 'yarn-name': {
          if (await this.$refs['yarn-setting-form'].validate()) {
            let submitData = _getYarnNameSetting(this.form)
            submitData = 'yarn_queue' in submitData && {project: submitData.project, queue_name: submitData.yarn_queue}
            this.updateYarnQueue(submitData)
          } else {
            errorCallback()
          }
          break
        }
      }
      // 设置默认参数的有二次确认，所以成功的反馈不能在结尾调
      if (type !== 'defaultDB-settings') {
        successCallback()
        this.$emit('reload-setting')
        this.$message({ type: 'success', message: this.$t('kylinLang.common.updateSuccess') })
      }
    } catch (e) {
      errorCallback()
      handleError(e)
    }
  }
  async handleResetForm (type, successCallback, errorCallback) {
    try {
      switch (type) {
        case 'accelerate-settings': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'query_accelerate_threshold'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getAccelerationSettings(data) }
          this.$refs['accelerate-setting-form'].clearValidate()
          break
        }
        case 'job-alert': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'job_notification_config'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getJobAlertSettings(data, true) }
          this.$refs['job-alert'].clearValidate()
          break
        }
        case 'defaultDB-settings': {
          // 不用重置
          /* const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'query_accelerate_threshold'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getDefaultDBSettings(data) }
          this.$refs['setDefaultDB'].clearValidate() */
          break
        }
        case 'yarn-name': {
          const res = await this.resetConfig({project: this.currentSelectedProject, reset_item: 'yarn_queue'})
          const data = await handleSuccessAsync(res)
          this.form = { ...this.form, ..._getYarnNameSetting(data) }
          this.$refs['yarn-setting-form'].clearValidate()
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
      case 'defaultDB-settings':
        return JSON.stringify(_getDefaultDBSettings(form)) !== JSON.stringify(_getDefaultDBSettings(project))
      case 'job-alert':
        return JSON.stringify(_getJobAlertSettings(form, true)) !== JSON.stringify(_getJobAlertSettings(project, true))
      case 'yarn-name':
        return JSON.stringify(_getYarnNameSetting(form)) !== JSON.stringify(_getYarnNameSetting(project))
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
  .yarn-name-input {
    width: 120px;
  }
}
</style>
