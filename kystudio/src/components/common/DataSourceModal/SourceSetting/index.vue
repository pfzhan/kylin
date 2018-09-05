<template>
  <el-form class="source-setting" label-position="top" size="medium" label-width="80px" :model="form">
    <el-form-item :label="$t('autoUpdateTip1')"></el-form-item>
    <el-form-item :label="$t('autoUpdateTip2')">
      <el-radio-group :value="form.isAutoUpdate">
        <el-radio :label="false" @change="handleChange('isAutoUpdate', false)">{{$t('manual')}}</el-radio>
        <el-radio :label="true" @change="handleChange('isAutoUpdate', true)">{{$t('system')}}</el-radio>
      </el-radio-group>
    </el-form-item>
  </el-form>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import locales from './locales'

@Component({
  props: {
    form: {
      type: Object
    }
  },
  locales
})
export default class SourceSetting extends Vue {
  handleChange (key, value) {
    this.$emit('input', {
      ...this.form,
      [key]: value
    })
  }
}
</script>

<style lang="less">
.source-setting {
  padding: 20px;
  .el-form-item {
    margin: 0;
  }
  .el-form-item__label {
    line-height: 21px !important;
  }
  .el-radio-group {
    color: #263238;
  }
}
</style>
