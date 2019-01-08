<template>
  <div class="setting">
    <header class="setting-header">
      <h1 class="font-medium">{{currentProjectData.name}}</h1>
    </header>
    <section class="setting-body" v-loading="isLoading">
      <el-tabs v-model="viewType" v-if="projectSettings">
        <el-tab-pane :label="$t('basic')" :name="viewTypes.BASIC">
          <SettingBasic :project="projectSettings" @reload-setting="getCurrentSettings" @form-changed="handleFormChanged"></SettingBasic>
        </el-tab-pane>
        <el-tab-pane :label="$t('advanced')" :name="viewTypes.ADVANCED">
          <SettingAdvanced :project="projectSettings" @reload-setting="getCurrentSettings" @form-changed="handleFormChanged"></SettingAdvanced>
        </el-tab-pane>
        <el-tab-pane :label="$t('model')" :name="viewTypes.MODEL">
          <SettingModel :project="currentProjectData"></SettingModel>
        </el-tab-pane>
      </el-tabs>
      <EmptyData v-else>
      </EmptyData>
    </section>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { viewTypes } from './handler'
import { handleError, handleSuccessAsync } from '../../util'
import EmptyData from '../common/EmptyData/EmptyData.vue'
import SettingBasic from './SettingBasic/SettingBasic.vue'
import SettingAdvanced from './SettingAdvanced/SettingAdvanced.vue'
import SettingModel from './SettingModel/SettingModel.vue'

@Component({
  computed: {
    ...mapGetters([
      'currentProjectData'
    ])
  },
  methods: {
    ...mapActions({
      fetchProjectSettings: 'FETCH_PROJECT_SETTINGS'
    })
  },
  components: {
    EmptyData,
    SettingBasic,
    SettingAdvanced,
    SettingModel
  },
  locales
})
export default class Setting extends Vue {
  viewType = viewTypes.BASIC
  viewTypes = viewTypes
  isLoading = false
  projectSettings = null
  changedForm = {
    isBasicSettingChange: false,
    isAdvanceSettingChange: false
  }
  _showLoading () {
    this.isLoading = true
  }
  _hideLoading () {
    this.isLoading = false
  }
  handleFormChanged (changedForm) {
    this.changedForm = { ...this.changedForm, ...changedForm }
  }
  async beforeRouteLeave (to, from, next) {
    const [ tabName ] = Object.entries(this.changedForm).find(([tabName, isFormChanged]) => isFormChanged) || []
    try {
      tabName && await this.leaveConfirm()
      next()
    } catch (e) {
      this.viewType = tabName
    }
  }
  async getCurrentSettings () {
    this._showLoading()
    try {
      const projectName = this.currentProjectData.name
      const response = await this.fetchProjectSettings({ projectName })
      const result = await handleSuccessAsync(response)
      this.projectSettings = result
    } catch (e) {
      handleError(e)
    }
    this._hideLoading()
  }
  leaveConfirm () {
    const confirmTitle = this.$t('kylinLang.common.willGo')
    const confirmMessage = this.$t('kylinLang.common.tip')
    const confirmButtonText = this.$t('kylinLang.common.go')
    const cancelButtonText = this.$t('kylinLang.common.cancel')
    const type = 'warning'
    return this.$confirm(confirmTitle, confirmMessage, { confirmButtonText, cancelButtonText, type })
  }
  mounted () {
    if (this.currentProjectData) {
      this.getCurrentSettings()
    }
  }
}
</script>

<style lang="less">
@import '../../assets/styles/variables.less';

.setting {
  padding: 20px;
  .setting-header {
    margin-bottom: 20px;
  }
  .setting-header h1 {
    font-size: 16px;
  }
  .el-tabs__nav {
    margin-left: 10px;
  }
  .project-setting,
  .quota-setting,
  .snapshot-setting {
    background-color: @aceditor-bg-color;
    border: 1px solid @line-border-color;
    padding: 20px;
  }
  .project-setting:not(:last-child) {
    margin-bottom: 20px;
  }
  // editable-block style
  .editable-block {
    margin-bottom: 20px;
  }
  // setting-item 通用style
  .setting-item {
    border-bottom: 1px solid @line-border-color;
    padding: 15px 20px;
    margin: 0;
  }
  .setting-item:last-child {
    border: none;
  }
  .setting-label {
    margin-right: 10px;
  }
  .setting-input {
    display: none;
    margin-bottom: 0;
  }
  .setting-label,
  .setting-value {
    display: inline-block;
    color: @text-title-color;
  }
  .is-edit .setting-input {
    display: inline-block;
  }
  .is-edit .setting-value {
    display: none;
    &.fixed {
      display: inline-block;
    }
  }
  .is-edit .setting-input {
    display: inline-block;
  }
  .is-edit .clearfix .setting-value {
    display: none;
  }
  .is-edit .clearfix .setting-input {
    display: block;
  }
  .clearfix {
    .setting-label,
    .setting-value {
      display: block;
      float: left;
    }
    .setting-input {
      display: none;
      float: left;
      margin-left: 3px;
    }
  }
  .setting-desc {
    margin-top: 3px;
    font-size: 12px;
    line-height: 16px;
    color: @text-normal-color;
    &.large {
      font-size: 14px;
      color: @text-title-color;
    }
  }
  .setting-desc:last-child {
    margin-bottom: 0px;
  }
  .option-title {
    color: @text-title-color;
  }
  .field-item {
    margin-top: 10px;
    margin-left: 20px;
    .setting-label {
      position: relative;
      &:before {
        content: ' ';
        position: absolute;
        left: -10px;
        top: calc(~'50% + 1px');
        transform: translateY(-50%);
        width: 4px;
        height: 4px;
        border-radius: 50%;
        background: @text-normal-color;
      }
    }
  }
  .setting-input+.setting-input {
    margin-left: 7px;
  }
  .editable-block {
    .disabled,
    .disabled * {
      color: @text-disabled-color;
    }
    .disabled.field-item .setting-label:before {
      background: @text-disabled-color;
    }
  }
}
</style>
