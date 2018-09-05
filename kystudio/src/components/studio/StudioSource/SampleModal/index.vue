<template>
  <el-dialog class="sample-modal"
    :title="$t('sample')"
    :visible="isShow"
    @close="hideModal">
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="hideModal">{{$t('cancel')}}</el-button>
      <el-button size="medium" plain type="primary" @click="submit">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

@Component
export default class SampleModal extends Vue {
  isShow = false

  showModal () {
    this.isShow = true
  }
  hideModal () {
    this.isShow = false
  }
  submit () {
  }
}
</script>

<style lang="less">
</style>
