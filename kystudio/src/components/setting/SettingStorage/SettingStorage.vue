<template>
  <div class="setting-storage">
    <div class="project-setting">
      <div class="setting-item clearfix">
        <span class="setting-label font-medium">{{$t('segmentMerge')}}</span>
        <span class="setting-value">
          <el-switch
            v-model="form.isSegmentMerge"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
      </div>
      <div class="setting-desc">{{$t('segmentMergeDesc')}}</div>
      <SegmentMerge v-model="form"></SegmentMerge>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import SegmentMerge from '../SegmentMerge/SegmentMerge.vue'

@Component({
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  components: {
    SegmentMerge
  },
  locales
})
export default class SettingStorage extends Vue {
  form = {
    isSegmentMerge: true,
    autoMergeConfigs: [ 'WEEK', 'MONTH' ],
    volatileConfig: {
      value: 0,
      type: 'DAY'
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.setting-storage {
}
</style>
