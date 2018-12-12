<template>
  <div class="accelerate-setting">
    <div class="accelerate-setting-block">
      <span class="ky-list-title">{{$t('acceThreshold')}}</span>
      <el-switch class="ksd-switch" v-model="preSettingObj.batch_enabled" :active-text="$t('kylinLang.common.ON')" :inactive-text="$t('kylinLang.common.OFF')" @change="savePreferrence"></el-switch>
      <div class="setting-inner" :class="{'disabled': !preSettingObj.batch_enabled}">
        <span>{{$t('notifyLeftTips')}}</span>
        <el-input size="small" @input="handleInputChange" class="acce-input" :disabled="!preSettingObj.batch_enabled" v-model="preSettingObj.threshold"></el-input>
        <span>{{$t('notifyRightTips')}}</span>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters } from 'vuex'
import { Component } from 'vue-property-decorator'

import { handleError } from '../../../util'
import { handleSuccess } from '../../../util/business'
@Component({
  methods: {
    ...mapActions({
      getPreferrence: 'GET_PREFERRENCE',
      updatePreferrence: 'UPDATE_PREFERRENCE'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  locales: {
    'en': {acceThreshold: 'Accelerating Threshold', notifyLeftTips: 'Notify me every time when there are ', notifyRightTips: ' favorite queries.'},
    'zh-cn': {acceThreshold: '加速阈值', notifyLeftTips: '每积累', notifyRightTips: ' 条加速查询时，提醒我。'}
  }
})

export default class SettingAccelerate extends Vue {
  preSettingObj = {
    batch_enabled: true,
    threshold: 20
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

  savePreferrence () {
    this.preSettingObj['project'] = this.currentSelectedProject
    this.updatePreferrence(this.preSettingObj).then((res) => {
      // this.$message({
      //   type: 'success',
      //   message: this.$t('kylinLang.common.actionSuccess')
      // })
    }, (res) => {
      handleError(res)
    })
  }

  created () {
    if (this.currentSelectedProject) {
      this.getPreferrence({project: this.currentSelectedProject}).then((res) => {
        handleSuccess(res, (data) => {
          this.preSettingObj = data
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
    .ksd-switch {
      transform: scale(0.8);
      margin-left: 5px;
    }
    .setting-inner {
      margin-top: 10px;
      font-size: 14px;
      color: @text-title-color;
      &.disabled {
        color: @text-disabled-color;
      }
    }
    .acce-input {
      width: 64px;
      .el-input__inner {
        text-align: right;
      }
    }
  }
</style>
