<template>
  <el-dialog class="guide-box" :append-to-body="true" width="660px"
    :before-close="closeShowGuideModeCheckDialog"
    title="Which mode do you want to explore ?"
    :close-on-click-modal="false"
    :visible="showGuideModeCheckDialog"
    :close-on-press-escape="false">
    <div>
      <el-row :gutter="20">
        <el-col :span="12">
          <div class="guide-pic">
          </div>
          <div class="guide-title">Expert Mode Guide</div>
          <div class="guide-desc">Expert Mode will assist you to add data source and scratch tables to your own models from blank</div>
          <div class="guide-footer" @click="startManual">Start</div>
        </el-col>
        <el-col :span="12">
          <div class="guide-pic">
          </div>
          <div class="guide-title">Smart Mode Guide</div>
          <div class="guide-desc">Ecpert Mode will assist you to add data source and scratch tables to your own models from blank</div>
          <div class="guide-footer" @click="startAuto">Start</div>
        </el-col>
      </el-row>
    </div>
  </el-dialog>
</template>
<script>
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  @Component({
    computed: {
      showGuideModeCheckDialog () {
        return this.$store.state.system.guideConfig.guideModeCheckDialog
      },
      globalMaksShow () {
        let showGuideMask = this.$store.state.system.guideConfig.globalMaskVisible
        if (showGuideMask) {
          this.$nextTick(() => {
            this.showGuid.showTitle = showGuideMask
            this.showGuid.showContent = showGuideMask
          })
        }
        return showGuideMask
      }
    }
  })
  export default class GuidType extends Vue {
    showDetail = false
    showCopyStatus = false
    showGuid = {
      showTitle: false,
      showContent: false
    }
    guide = null
    showGlobalMask () {
      this.$store.state.system.guideConfig.globalMaskVisible = true
    }
    startManual () {
      this.closeShowGuideModeCheckDialog()
      this.showGlobalMask()
      this.$store.state.system.guideConfig.guideType = 'manual'
    }
    startAuto () {
      this.closeShowGuideModeCheckDialog()
      this.showGlobalMask()
      this.$store.state.system.guideConfig.guideType = 'auto'
    }
    closeShowGuideModeCheckDialog () {
      this.$store.state.system.guideConfig.guideModeCheckDialog = false
    }
    closeGuide () {
      this.showGuid.showTitle = false
      this.showGuid.showContent = false
      setTimeout(() => {
        this.$store.state.system.guideConfig.globalMaskVisible = false
      }, 500)
    }
    destroyed () {
      this.showCopyStatus = false
    }
    mounted () {
    }
  }
</script>
<style lang="less">
@import '../../../assets/styles/variables.less';
 .guide-box {
    .guide-pic{
      width:85px;
      height:85px;
      background-color: @grey-4;
      margin: 0 auto;
      margin-top:40px;
    }
    .guide-title{
      text-align: center;
      color:@text-title-color;
      margin-top:14px;
    }
    .guide-desc {
      text-align: center;
      color:@text-disabled-color;
      font-size:12px;
      margin-top:14px;
    }
    .guide-footer {
      font-size: 16px;
      margin-top:14px;
      text-align: center;
      color:@base-color;
      margin-bottom: 60px;
      cursor:pointer;
    }
  }
</style>

