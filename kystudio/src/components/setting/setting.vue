<template>
  <div class="setting">
    <header class="setting-header">
      <h1 class="font-medium">{{currentProjectData.name}}</h1>
    </header>
    <section class="setting-body" v-loading="isLoading">
      <el-tabs v-model="viewType" v-if="projectSettings">
        <el-tab-pane :label="$t('basic')" :name="viewTypes.BASIC">
          <SettingBasic :project="projectSettings" @reload-setting="getCurrentSettings"></SettingBasic>
        </el-tab-pane>
        <el-tab-pane :label="$t('advanced')" :name="viewTypes.ADVANCED">
          <SettingAdvanced :project="projectSettings" @reload-setting="getCurrentSettings"></SettingAdvanced>
        </el-tab-pane>
        <el-tab-pane :label="$t('model')" :name="viewTypes.MODEL">
          <SettingModel :project="currentProjectData"></SettingModel>
        </el-tab-pane>
      </el-tabs>
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
import emptyImg from '../../assets/img/empty.svg'
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
    SettingBasic,
    SettingAdvanced,
    SettingModel
  },
  locales
})
export default class Setting extends Vue {
  viewType = viewTypes.BASIC
  viewTypes = viewTypes
  emptyImg = emptyImg
  isLoading = false
  projectSettings = null
  _showLoading () {
    this.isLoading = true
  }
  _hideLoading () {
    this.isLoading = false
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
  mounted () {
    this.getCurrentSettings()
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
  }
  .setting-item:last-child {
    border: none;
  }
  .setting-label {
    margin-right: 10px;
  }
  .setting-input {
    display: none;
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
    }
  }
  .setting-desc {
    margin-top: 5px;
    font-size: 12px;
    line-height: 16px;
    color: @text-description;
    &.large {
      font-size: 14px;
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
  }
  .setting-input+.setting-input {
    margin-left: 7px;
  }
  .editable-block:not(.is-edit) {
    .disabled {
      * {
        color: @text-disabled-color;
      }
    }
  }
}
</style>
