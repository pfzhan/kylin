<template>
  <div class="accelerate-setting">
    <EditableBlock
      :headerContent="$t('accePreference')"
      @submit="savePreferrence"
      @cancel="handleCancelPreferrence">
      <div class="setting-item">
        <span class="ky-list-title">{{$t('acceThreshold')}}</span>
        <el-switch class="ksd-switch" v-model="preSettingObj.batch_enabled" :active-text="$t('kylinLang.common.ON')" :inactive-text="$t('kylinLang.common.OFF')" @change="savePreferrence"></el-switch>
        <div class="setting-inner" :class="{'disabled': !preSettingObj.batch_enabled}">
          <span>{{$t('notifyLeftTips')}}</span>
          <span class="setting-value ksd-fs-14 font-medium setting-input">{{preFormValue}}</span>
          <el-input size="small" @input="handleInputChange" class="acce-input" :disabled="!preSettingObj.batch_enabled" v-model="preSettingObj.threshold"></el-input>
          <span>{{$t('notifyRightTips')}}</span>
        </div>
      </div>
    </EditableBlock>
    <EditableBlock
      :headerContent="$t('jobAlert')"
      @submit="saveJobNotice"
      @cancel="handleCancelJobNotice">
      <div class="setting-item">
        <span class="ky-list-title">{{$t('emptyDataLoad')}}</span>
        <el-switch class="ksd-switch" v-model="jobNoticeObj.emptyLoad" :active-text="$t('kylinLang.common.ON')" :inactive-text="$t('kylinLang.common.OFF')"></el-switch>
        <p class="desc">Notify me if there is an empty data load job.</p>
        <div class="ksd-mt-10 setting-inner setting-value" :class="{'disabled': !jobNoticeObj.emptyLoad}">
          <span class="ky-list-title">{{$t('emails')}}</span>
          <span class="notice-email" v-for="e in jobNoticeObj.emptyNoticeEmails" :key="e">{{e}}</span>
        </div>
        <el-form ref="form" :model="jobNoticeForm" size="medium" label-width="42px" class="edit-form setting-input">
          <el-form-item :label="$t('emails')">
            <el-row class="item-value" v-for="(e, index) in jobNoticeForm.emptyNoticeEmails" :key="e">
              <el-input :value="e" @input="val => handleInput(`emptyNoticeEmails.${index}`, val)" class="ksd-inline"></el-input>
              <el-button icon="el-icon-plus" circle size="mini" class="ksd-ml-10" @click="addNoticeEmail"></el-button>
              <el-button icon="el-icon-minus" circle size="mini" class="ksd-ml-2" :disabled="jobNoticeForm.emptyNoticeEmails.length==1" @click="removeNoticeEmail(index)"></el-button>
            </el-row>
          </el-form-item>
        </el-form>
      </div>
      <div class="setting-item">
        <span class="ky-list-title">{{$t('errorJob')}}</span>
        <el-switch class="ksd-switch" v-model="jobNoticeObj.errorJob" :active-text="$t('kylinLang.common.ON')" :inactive-text="$t('kylinLang.common.OFF')"></el-switch>
        <p class="desc">Notify me if there is a error job. </p>
        <div class="ksd-mt-10 setting-inner setting-value" :class="{'disabled': !jobNoticeObj.errorJob}">
          <span class="ky-list-title">{{$t('emails')}}</span>
          <span class="notice-email" v-for="e in jobNoticeObj.errorNoticeEmails" :key="e">{{e}}</span>
        </div>
        <el-form ref="form" :model="jobNoticeForm" size="medium" label-width="42px" class="edit-form setting-input">
          <el-form-item :label="$t('emails')">
            <el-row class="item-value" v-for="(e, index) in jobNoticeForm.errorNoticeEmails" :key="e">
              <el-input :value="e" @input="val => handleInput(`errorNoticeEmails.${index}`, val)" class="ksd-inline"></el-input>
              <el-button icon="el-icon-plus" circle size="mini" class="ksd-ml-10" @click="addErrorNoticeEmail"></el-button>
              <el-button icon="el-icon-minus" circle size="mini" class="ksd-ml-2" :disabled="jobNoticeForm.errorNoticeEmails.length==1" @click="removeErrorNoticeEmail(index)"></el-button>
            </el-row>
          </el-form-item>
        </el-form>
      </div>
    </EditableBlock>
  </div>
</template>

<script>
import Vue from 'vue'
import locales from './locales'
import { mapActions, mapGetters } from 'vuex'
import { Component } from 'vue-property-decorator'

import { handleError, objectClone } from '../../../util'
import { handleSuccess } from '../../../util/business'
import { set } from '../../../util/object'
import EditableBlock from '../../common/EditableBlock/EditableBlock.vue'
@Component({
  methods: {
    ...mapActions({
      getPreferrence: 'GET_PREFERRENCE',
      updatePreferrence: 'UPDATE_PREFERRENCE'
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

export default class SettingAccelerate extends Vue {
  preSettingObj = {
    batch_enabled: true,
    threshold: 20
  }
  preFormValue = 20
  jobNoticeObj = {
    emptyLoad: true,
    emptyNoticeEmails: ['notice@kyligence.io', 'notice@kyligence.io'],
    errorJob: false,
    errorNoticeEmails: ['notice@kyligence.io', 'notice@kyligence.io']
  }
  jobNoticeForm = {
    emptyNoticeEmails: ['notice@kyligence.io', 'notice@kyligence.io'],
    errorNoticeEmails: ['notice@kyligence.io', 'notice@kyligence.io']
  }

  handleInput (key, value) {
    const newValue = set(this.jobNoticeForm, key, value)
    this.$emit('input', newValue)
  }

  handleInputChange (value) {
    this.$nextTick(() => {
      this.preSettingObj.threshold = (isNaN(value) || value === '' || value < 0) ? 0 : Number(value)
    })
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      this.savePreferrence()
    }, 500)
  }

  savePreferrence (successCallback) {
    this.preSettingObj['project'] = this.currentSelectedProject
    this.preSettingObj.threshold = this.preFormValue
    this.updatePreferrence(this.preSettingObj).then((res) => {
      successCallback()
    }, (res) => {
      handleError(res)
    })
  }
  handleCancelPreferrence () {
    this.initPreferrence()
  }
  initPreferrence () {
    this.preFormValue = this.preSettingObj.threshold
  }
  saveJobNotice (successCallback) {
    this.jobNoticeObj.emptyNoticeEmails = objectClone(this.jobNoticeForm.emptyNoticeEmails)
    this.jobNoticeObj.errorNoticeEmails = objectClone(this.jobNoticeForm.errorNoticeEmails)
    successCallback()
  }
  handleCancelJobNotice () {
    this.initJobNotice()
  }
  initJobNotice () {
    this.jobNoticeForm.emptyNoticeEmails = objectClone(this.jobNoticeObj.emptyNoticeEmails)
    this.jobNoticeForm.errorNoticeEmails = objectClone(this.jobNoticeObj.errorNoticeEmails)
  }
  addNoticeEmail () {
    this.jobNoticeForm.emptyNoticeEmails.push('')
  }
  removeNoticeEmail (index) {
    this.jobNoticeForm.emptyNoticeEmails.splice(index, 1)
  }
  addErrorNoticeEmail () {
    this.jobNoticeForm.errorNoticeEmails.push('')
  }
  removeErrorNoticeEmail (index) {
    this.jobNoticeForm.errorNoticeEmails.splice(index, 1)
  }
  created () {
    if (this.currentSelectedProject) {
      this.getPreferrence({project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, (data) => {
          this.preSettingObj = data
          this.initPreferrence()
        })
      }, (res) => {
        handleError(res)
      })
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
  .accelerate-setting {
    padding: 5px 0;
    .editable-block {
      padding: 0;
      .ksd-switch {
        transform: scale(0.8);
        margin-left: 5px;
      }
      .desc {
        font-size: 12px;
        color: @text-normal-color;
        line-height: 16px;
        margin-top: 5px;
      }
      .notice-email {
        font-size: 14px;
        color: @text-title-color;
        line-height: 18px;
        background-color: @grey-4;
        margin-right: 10px;
      }
      .setting-inner {
        margin-top: 10px;
        font-size: 14px;
        color: @text-title-color;
        &.disabled,
        &.disabled span {
          color: @text-disabled-color;
        }
      }
      .acce-input {
        width: 64px;
        .el-input__inner {
          text-align: right;
        }
      }
      .edit-form {
        width: 25%;
        .el-form-item {
          margin-bottom: 0;
          .el-form-item__label {
            padding: 0;
            text-align: left;
            color: @text-title-color;
            font-weight: 500;
          }
          .el-input {
            width: 80%;
          }
        }
      }
    }
  }
</style>
