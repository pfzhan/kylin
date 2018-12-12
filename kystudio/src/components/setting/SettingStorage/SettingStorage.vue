<template>
  <div class="setting-storage">
    <div class="quota-setting ksd-mb-20">
      <span class="setting-label font-medium">{{$t('storageQuota')}}</span>
      <span>{{storageQuotaSize | dataSize}}</span>
      <p class="desc">Granted storage quota by system admin. If your project storage exceeds the quota, you would be forbidden to build new index or load more data.  </p>
      <hr/>
      <span class="setting-label font-medium">{{$t('storageGarbage')}}</span>
      <p class="desc">
        <el-checkbox v-model="garbageChecked">Index or query repeatedly used 5 times or below in a month.</el-checkbox>
      </p>
    </div>
    <div class="project-setting project-switch ksd-mb-20">
      <div class="setting-item clearfix">
        <span class="setting-label font-medium">{{$t('segmentMerge')}}</span>
        <span class="setting-value">
          <el-switch
            class="ksd-switch"
            v-model="form.isSegmentMerge"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </span>
      </div>
      <div class="setting-desc">{{$t('segmentMergeDesc')}}</div>
      <SegmentMerge v-model="form"></SegmentMerge>
    </div>
    <div class="snapshot-setting">
      <span class="setting-label font-medium">{{$t('snapshotSize')}}</span>
      <span>300M</span>
      <p class="desc">Suggested snapshot storage size by system admin.</p>
      <hr/>
      <span class="setting-label font-medium">{{$t('storageGarbage')}}</span>
      <el-switch
        class="ksd-switch"
        v-model="storageGarbage"
        :active-text="$t('kylinLang.common.OFF')"
        :inactive-text="$t('kylinLang.common.ON')">
      </el-switch>
      <p class="desc">Tables exceed the storage size may perform not so well, the system will want to notify you. </p>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'

import locales from './locales'
import SegmentMerge from '../SegmentMerge/SegmentMerge.vue'
import { handleSuccessAsync } from '../../../util/index'

@Component({
  props: {
    project: {
      type: Object,
      default: () => ({})
    }
  },
  methods: {
    ...mapActions({
      getQuotaInfo: 'GET_QUOTA_INFO'
    })
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
  storageQuotaSize = 0
  garbageChecked = true
  storageGarbage = true
  async created () {
    const res = await this.getQuotaInfo({project: this.project.name})
    const resData = await handleSuccessAsync(res)
    this.storageQuotaSize = resData.storage_quota_size
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.setting-storage {
  padding: 5px 0;
  .ksd-switch {
    transform: scale(0.8);
  }
  .quota-setting,
  .snapshot-setting {
    hr {
      border: 1px solid @grey-3;
      margin: 10px 0;
    }
    .desc {
      font-size: 12px;
      color: @text-normal-color;
      line-height: 16px;
      margin-top: 5px;
    }
  }
}
</style>
