<template>
  <el-dialog
    :visible="true"
    width="600px"
    status-icon="el-ksd-icon-error_24"
    @close="closeErrorDetail"
    :close-on-click-modal="false">
    <span slot="title">{{$t('errorDetail')}}</span>
    <div class="error-contain">
      <p class="error-title"></p>
      <el-button class="error-solution-btn ksd-mt-8" nobg-text iconr="el-ksd-icon-spark_link_16">{{$t('resolveErrorBtn')}}</el-button>
      <el-input class="error-trace-msg ksd-mt-8" :disabled="true" type="textarea" :value="traceMsg"></el-input>
      <el-button class="view-details-btn ksd-mt-8" v-if="showViewMore" @click="showMore = !showMore" nobg-text :iconr="showMore ? 'el-ksd-icon-arrow_up_16' : 'el-ksd-icon-arrow_down_16'">{{$t('viewMore')}}</el-button>
      <build-segment-detail v-if="showMore" :segmentTesks="currentErrorJob.segment_sub_tasks" :jobStatus="currentErrorJob.step_status"/>
    </div>
    <span slot="footer" class="dialog-footer">
      <el-button type="primary" size="medium" @click="closeErrorDetail">{{$t('kylinLang.common.IKnow')}}</el-button>
    </span>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import buildSegmentDetail from './buildSegmentDetail.vue'
import locales from './locales'

@Component({
  props: {
    currentErrorJob: {
      type: Object,
      default () {
        return {}
      }
    }
  },
  components: {
    buildSegmentDetail
  },
  locales
})
export default class jobErrorDetail extends Vue {
  traceMsg = 'xxxxxxxxxxx'
  showMore = false

  get showViewMore () {
    return this.currentErrorJob.segment_sub_tasks && Object.keys(this.currentErrorJob.segment_sub_tasks).length > 1
  }

  closeErrorDetail () {
    this.$emit('close')
  }
}
</script>

<style lang="less">
  @import '../../../assets/styles/variables.less';
  .error-contain {
    .error-title {
      font-weight: 500;
    }
    .error-solution-btn {
      i {
        font-size: 16px;
      }
    }
    .error-trace-msg {
      .el-textarea__inner {
        background: @ke-background-color-secondary;
        -webkit-text-fill-color: @text-normal-color;
        max-height: 400px;
      }
    }
  }
</style>
