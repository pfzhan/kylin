<template>
  <el-dialog class="guide-box" :append-to-body="true" width="720px"
    :before-close="closeShowGuideModeCheckDialog"
    :title="$t('switchModeTitle')"
    :close-on-click-modal="false"
    :visible="showGuideModeCheckDialog"
    :close-on-press-escape="false">
    <div>
      <el-row :gutter="20">
        <el-col :span="12" >
          <div class="guide-type-content">
            <div class="guide-pic">
              <i class="el-icon-ksd-smart_mode"></i>
            </div>
            <div class="guide-title">{{$t('smartMode')}}</div>
            <div class="guide-desc" :class="$lang=='en'? 'en' : ''">{{$t('smartModeDesc')}}</div>
            <div class="guide-footer" @click="startAuto"><el-button size="medium" type="primary"  style="width:100%">{{$t('start')}}</el-button></div>
         </div>
        </el-col>
        <el-col :span="12">
          <div class="guide-type-content">
            <div class="guide-pic">
              <i class="el-icon-ksd-expert_mode"></i>
            </div>
            <div class="guide-title">{{$t('exportMode')}}</div>
            <div class="guide-desc" :class="$lang=='en'? 'en' : ''">{{$t('exportModeDesc')}}</div>
            <div class="guide-footer" @click="startManual"><el-button size="medium" type="primary" style="width:100%">{{$t('start')}}</el-button></div>
          </div>
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
    },
    locales: {
      'en': {
        start: 'Start',
        exportMode: 'Expert mode',
        smartMode: 'Smart mode',
        switchModeTitle: 'Which mode do you want to explore?',
        exportModeDesc: 'The expert mode is recommended for analysis based on multi-dimensional models.',
        smartModeDesc: 'The smart mode is recommended for exploring data via Business Intelligence tools, and SQL statements will be accelerated transparently by the system.'
      },
      'zh-cn': {
        start: '开始',
        exportMode: '专家模式',
        smartMode: '智能模式',
        switchModeTitle: '请选择你想探索的模式',
        exportModeDesc: '专家模式适用于对多维建模进行分析。',
        smartModeDesc: '智能模式适用于直接通过 BI 探索数据，系统将透明地加速 BI 发出的 SQL 语句。'
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
   .guide-type-content {
     width:220px;
     margin: 0 auto;
   }
    .guide-pic{
      width:85px;
      height:85px;
      background-color: @base-color-9;
      margin: 0 auto;
      border-radius: 50%;
      margin-top:40px;
      text-align: center;
      i {
        margin-top:10px;
        font-size:67px;
        color:@base-color;
      }
    }
    .guide-title{
      text-align: center;
      color:@text-title-color;
      margin-top:14px;
    }
    .guide-desc {
      text-align: left;
      color:@text-disabled-color;
      font-size:12px;
      margin-top: 14px;
      height:34px;
      &.en{
        height:56px;
      }
    }
    .guide-footer {
      font-size: 16px;
      margin-top:20px;
      text-align: center;
      color:@base-color;
      margin-bottom: 60px;
      cursor:pointer;
    }
  }
</style>

