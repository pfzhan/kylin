<template>
  <div class="setting">
    <header class="setting-header">
      <h1 class="font-medium">{{currentProjectData.name}}</h1>
    </header>
    <el-tabs v-model="viewType">
      <el-tab-pane :label="$t('basic')" :name="viewTypes.BASIC">
        <SettingBasic :project="currentProjectData"></SettingBasic>
      </el-tab-pane>
      <el-tab-pane :label="$t('acceleration')" :name="viewTypes.ACCELERATION">
        <SettingAccelerate></SettingAccelerate>
      </el-tab-pane>
      <el-tab-pane :label="$t('storage')" :name="viewTypes.STORAGE">
        <SettingStorage :project="currentProjectData"></SettingStorage>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { viewTypes } from './handler'
import emptyImg from '../../assets/img/empty.svg'
import SettingBasic from './SettingBasic/SettingBasic.vue'
import SettingAccelerate from './SettingAccelerate/SettingAccelerate.vue'
import SettingStorage from './SettingStorage/SettingStorage.vue'

@Component({
  computed: {
    ...mapGetters([
      'currentProjectData'
    ])
  },
  components: {
    SettingBasic,
    SettingAccelerate,
    SettingStorage
  },
  locales
})
export default class Setting extends Vue {
  viewType = viewTypes.BASIC
  viewTypes = viewTypes
  emptyImg = emptyImg
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
  .accelerate-setting-block,
  .quota-setting,
  .snapshot-setting {
    background-color: @aceditor-bg-color;
    border: 1px solid @line-border-color;
    padding: 20px;
  }
  .project-setting:not(:last-child) {
    margin-bottom: 20px;
  }
  .setting-item:not(:last-child) {
    margin-bottom: 10px;
  }
  .setting-label {
    margin-right: 7px;
    display: inline-block;
    white-space: nowrap;
  }
  .setting-label,
  .setting-value {
    color: @text-title-color;
  }
  .setting-desc {
    margin: -5px 0 10px 0;
    font-size: 12px;
    color: @text-description;
  }
  .setting-desc:last-child {
    margin-bottom: 0px;
  }
  .hr {
    border-top: 1px solid @grey-3;
    margin-bottom: 10px;
  }
  .option-title {
    color: @text-title-color;
  }
  .clearfix {
    .setting-label {
      float: left;
      display: block;
    }
    .setting-value {
      display: block;
    }
  }
}
</style>
