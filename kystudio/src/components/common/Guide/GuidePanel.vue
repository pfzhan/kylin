<template>
  <div class="global-mask" v-if="globalMaksShow" :style="{'z-index': maskZindex}">
        <el-button v-visible v-guide.moveGuidePanelBtn @click="moveGuidePanel"> </el-button>
        <div id="guid-panel" :style="guidePanelStyle">
          <div class="guid-icon">
            <img style="width:30px;height:30px;" v-if="guideType !== 'auto'" src="../../../assets/img/guide/expert_mode_small.png"/>
             <img style="width:30px;height:30px;" v-else src="../../../assets/img/guide/smart_mode_small.png"/>
          </div>
          <transition name="bounce">
            <div class="guid-title" v-if="showGuid.showTitle">
              <span v-if="guideType !== 'auto'">{{$t('expertMode')}}</span>
              <span v-else>{{$t('smartMode')}}</span>
              <i class="el-icon-close ksd-fright ksd-mt-8 ksd-mr-10" @click="closeGuide"></i>
            </div>
          </transition>
          <transition name="panelani">
            <div class="guid-content" v-if="showGuid.showContent">
                <el-tabs v-model="activeName" @tab-click="handleClickTab">
                  <el-tab-pane :label="$t(step.label)" :key="step.name" :name="step.name" v-for="step in guideSteps">
                    <!-- <div class="ksd-ml-20 ksd-mtb-6">操作提示：</div> -->
                    <ul class="steps-info">
                      <li class="guiding-step" :class="{'guide-end': x.done}" :key="x.tip" v-for="x in stepTipData">
                        <img v-if="x.done" style="width:12px;height:12px;" src="../../../assets/img/guide/icon_flag.png"/>
                        <span v-else class="dot"></span>
                        {{$t('kylinLang.guide.' + x.tip)}}
                      </li>
                    </ul>
                  </el-tab-pane>
                </el-tabs>
                <el-button class="ksd-fright guide-btn" size="mini" :loading="guideLoading" @click="goNextStep" plain>{{getNextBtnText}}</el-button>
                <el-button class="ksd-fright guide-btn" size="mini"  @click="stopGuide" v-if="showPauseBtn" plain>{{isPause ? $t('goon') : $t('pause')}}</el-button>
                <!-- <el-button class="ksd-fright guide-btn" size="mini"  @click="retryGuide" v-if="showRetryStep" plain>重放该步骤</el-button> -->
            </div>
          </transition>
        </div>
        <img src="../../../assets/img/guide/cursor-pointer.png" v-if="globalMouseShow" class="pointer-pic" :style="mousePos"/>
        <img src="../../../assets/img/guide/cursor-click.png" v-if="globalMouseClick" class="pointer-pic" :style="mousePos"/>
        <img src="../../../assets/img/guide/cursor-bg.png" v-if="globalMouseDrag" class="pointer-pic" :style="mousePos"/>

  </div>
</template>
<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import Guide from 'util/guide'
@Component({
  methods: {
    ...mapActions({
      getKybotAccount: 'GET_CUR_ACCOUNTNAME'
    })
  },
  components: {
  },
  computed: {
    getNextBtnText () {
      if (this.guideLoading) {
        return this.$t('guiding')
      }
      if (this.currentStep === this.guideSteps.length - 1 && this.guideSteps[this.currentStep].done) {
        return this.$t('end')
      }
      if (this.currentStep === 0 && !this.guideLoading && !this.guideSteps[this.currentStep].done) {
        return this.$t('start')
      }
      return this.$t('next')
    },
    showRetryAll () {
      return this.currentStep === this.stepTipData.length - 1 && !this.guideLoading
    },
    showPauseBtn () {
      return this.guideLoading
    },
    showRetryStep () {
      let cur = this.currentStep
      while (cur > 0) {
        if (!this.guideSteps[--cur].done) {
          return false
        }
      }
      if (cur > 0) {
        return true
      }
      return false
    },
    guidePanelStyle () {
      let styleObj = {}
      if (this.guidePanelPos.hasOwnProperty('left')) {
        styleObj.left = this.guidePanelPos.left + 'px'
      }
      if (this.guidePanelPos.hasOwnProperty('right')) {
        styleObj.right = this.guidePanelPos.right + 'px'
      }
      if (this.guidePanelPos.hasOwnProperty('top')) {
        styleObj.top = this.guidePanelPos.top + 'px'
      }
      return styleObj
    },
    guideType () {
      return this.$store.state.system.guideConfig.guideType
    },
    guideSteps () {
      if (this.guideType === 'auto') {
        return this.autoGuideSteps
      } else {
        return this.manualGuideSteps
      }
    },
    globalMaksShow () {
      let showGuideMask = this.$store.state.system.guideConfig.globalMaskVisible
      if (showGuideMask) {
        this.$nextTick(() => {
          this.showGuid.showTitle = showGuideMask
          this.showGuid.showContent = showGuideMask
        })
        this.activeName = this.$store.state.system.guideConfig.guideType === 'auto' ? 'autoProject' : 'project'
        let currentGuidePage = this.guideSteps[0]
        this.guide = new Guide({
          mode: currentGuidePage.name
        }, this)
      }
      return showGuideMask
    },
    globalMouseShow () {
      return this.$store.state.system.guideConfig.globalMouseVisible
    },
    globalMouseClick () {
      return this.$store.state.system.guideConfig.globalMouseClick
    },
    globalMouseDrag () {
      return this.$store.state.system.guideConfig.globalMouseDrag
    },
    mousePos () {
      return {
        position: 'absolute',
        left: this.$store.state.system.guideConfig.mousePos.x + 'px',
        top: this.$store.state.system.guideConfig.mousePos.y + 'px'
      }
    }
  },
  locales: {
    'en': {
      expertMode: 'Expert mode',
      smartMode: 'Smart mode',
      start: 'Start',
      next: 'Next',
      end: 'End',
      guiding: 'Running',
      pause: 'Pause',
      goon: 'Continue',
      addProjectTitle: 'Add project',
      loadTableTitle: 'Sync table schema',
      addModelTitle: 'Create a model',
      monitorTitle: 'Load data',
      speedSqlTitle: 'Accelerate SQL',
      queryTitle: 'Time to insight',
      sysErrorInGuide: 'The guide reports an error and cannot continue. You may exit and restart the guide later. ',
      timeoutInGuide: 'The step has been time out, and the guide couldn\'t continue. You may retry the step or exit the guide.'
    },
    'zh-cn': {
      expertMode: '专家模式',
      smartMode: '智能模式',
      start: '开始',
      next: '下一步',
      end: '结束',
      guiding: '演示中',
      pause: '暂停',
      goon: '继续',
      addProjectTitle: '添加项目',
      loadTableTitle: '同步表的元数据',
      addModelTitle: '创建模型',
      monitorTitle: '加载数据',
      speedSqlTitle: '加速查询',
      queryTitle: '数据探索',
      sysErrorInGuide: '系统出现异常，新手导览暂时无法继续。您可以暂时退出，稍后重新启动新手导览。',
      timeoutInGuide: '当前步骤已超时，新手导览暂时无法继续。您可以重试一次或暂时退出。'
    }
  }
})
export default class GuidePannel extends Vue {
  showGuid = {
    showTitle: false,
    showContent: false
  }
  guideMount = {}
  currentStep = 0
  guidePanelPos = {
    right: 20,
    top: 60
  }
  maskZindex = 999999
  isPause = false
  guideLoading = false
  stepsList = null
  activeName = 'project'
  manualGuideSteps = [
    {name: 'project', label: 'addProjectTitle', done: false},
    {name: 'loadTable', label: 'loadTableTitle', done: false},
    {name: 'addModel', label: 'addModelTitle', done: false},
    {name: 'monitor', label: 'monitorTitle', done: false}
  ]
  autoGuideSteps = [
    {name: 'autoProject', label: 'addProjectTitle', done: false},
    {name: 'autoLoadTable', label: 'loadTableTitle', done: false},
    {name: 'query', label: 'queryTitle', done: false},
    {name: 'acceleration', label: 'speedSqlTitle', done: false},
    {name: 'monitor', label: 'monitorTitle', done: false}
  ]
  guide = null
  resetGuidSteps () {
    this.guideSteps.forEach((step) => {
      step.done = false
    })
  }
  get stepTipData () {
    if (this.guideMount.stepsInfo) {
      return this.guideMount.stepsInfo.filter((step) => {
        return !!step.tip
      })
    }
    return []
  }
  handleClickTab (tab) {
    let tabIndex = +tab.index
    let currentGuidePage = this.guideSteps[tabIndex]
    this.guide = new Guide({
      mode: currentGuidePage.name,
      guideType: this.guideType
    }, this)
    this.stepsList = this.guide.stepsInfo
    this.activeName = currentGuidePage.name
  }
  _guideGo () {
    this.guideLoading = true
    if (!this.guide) {
      return
    }
    this.guide.go().then(() => {
      this.guideSteps[this.currentStep].done = true
      this.guideLoading = false
    }, () => {
      this.openGuideErrorDialog(this.$t('timeoutInGuide'), {
        showCancelButton: true,
        cancelButtonText: this.$t('kylinLang.common.retry')
      }).catch(() => {
        this.maskZindex = 999999
        this.retryGuide()
      })
      this.guideLoading = false
    })
  }
  // 停止
  stopGuide () {
    this.isPause = !this.isPause
    if (this.isPause) {
      this.guide.pause()
    } else {
      this.guide.restart()
    }
  }
  // 下一步
  goNextStep () {
    // 到了最后一步，且最后一步结束
    if (this.currentStep === this.guideSteps.length - 1 && this.guideSteps[this.currentStep].done) {
      this.closeGuide()
      return
    }
    if (this.guideSteps[this.currentStep].done) {
      this.currentStep++
    }
    let currentGuidePage = this.guideSteps[this.currentStep]
    this.guide = new Guide({
      mode: currentGuidePage.name,
      guideType: this.guideType
    }, this)
    this.stepsList = this.guide.stepsInfo
    this.activeName = currentGuidePage.name
    this._guideGo()
  }
  // 重新开始演示当前的模块
  retryGuide () {
    // this.currentStep = 0
    this.maskZindex = 999999
    // this.guide.stop()
    this.goNextStep()
  }
  moveGuidePanel (pos) {
    this.guidePanelPos = pos || {
      right: 20,
      top: 60
    }
  }
  // 关闭演示
  closeGuide () {
    this.guideLoading = false
    this.currentStep = 0
    this.guide.stop()
    this.showGuid.showTitle = false
    this.showGuid.showContent = false
    this.resetGuidSteps()
  }
  destroyed () {
    this.showCopyStatus = false
  }
  mounted () {
  }
  openGuideErrorDialog (tip, options) {
    this.maskZindex = 100
    let dialogOptions = {
      type: 'error',
      confirmButtonText: this.$t('kylinLang.common.exit'),
      showClose: false
    }
    Object.assign(dialogOptions, options)
    return this.$confirm(tip, this.$t('kylinLang.common.tip'), dialogOptions).then(() => {
      this.maskZindex = 999999
      this.closeGuide()
    })
  }
  @Watch('$store.state.config.errorMsgBox.isShow')
  hasSysError (val) {
    if (val && this.$store.state.system.guideConfig.globalMaskVisible) {
      this.openGuideErrorDialog(this.$t('sysErrorInGuide'))
    }
  }
}
</script>
<style lang="less">
@import '../../../assets/styles/variables.less';
.global-mask {
  .pointer-pic {
    transform: scale(0.7);
  }
  .bounce-enter-active {
    transform-origin:50% 50%;
    animation: bounce-in .5s;
  }
  .bounce-leave-active {
    transform-origin:50% 50%;
    animation: bounce-in .5s reverse;
  }
  .panelani-enter-active {
    animation: panelani-in .5s;
  }
  .panelani-leave-active {
    animation: panelani-in .5s reverse;
  }
  @keyframes bounce-in {
    0% {
      transform: scale(0);
    }
    50% {
      transform: scale(1.5);
    }
    100% {
      transform: scale(1);
    }
  }
  @keyframes panelani-in {
    0% {
      transform: translateY(-100px);
      opacity: 1;
    }
    50% {
      transform: translateY(40px);
      opacity: 0.5;
    }
    100% {
      transform: translateY(0px);
      opacity: 0;
    }
  }
  position: absolute;
  top:0;
  bottom:0;
  overflow:hidden;
  left:0;
  right:0;
  background: transparent;
  #guid-panel {
    color:@fff;
    font-size:12px;
    .guide-btn {
      background: transparent;
      color:@fff;
      margin:10px;
      &:hover {
        background: @fff;
        color:@base-color;
      }
    }
    // tab 组件演示覆盖
    .el-tabs__item {
      color:@fff;
      font-weight:normal;
      font-size:12px;
      height:30px;
      line-height:30px;
    }
    .el-tabs__active-bar {
      background-color:@warning-color-1;
    }
    .el-tabs__nav-wrap::after {
      background-color: rgba(255, 255, 255, 0.2);
    }
    .el-tabs__nav{
      margin-left:0;
    }
    .el-tabs__nav-prev, .el-tabs__nav-next {
      height:30px;
      line-height:30px;
      color:@fff;
    }
    .el-tabs__header {
      margin-bottom:0;
    }
    // tab 组件演示覆盖
    width:370px;
    position:absolute;
    .guid-icon {
      width: 41px;
      height: 41px;
      box-shadow: 0 1px 3px 0 @text-normal-color;
      background-color: @fff;
      border-radius: 50%;
      position: absolute;
      top:-5px;
      left:-20px;
      img {
        width:30px;
        height:30px;
        margin: 5px 6px;
        line-height: 41px;
      }
    }
    .guid-title {
      span{
        padding-left:28px;
      }
      i {
        width: 12px;
        height: 12px;
        background-color: @text-placeholder-color;
        border-radius: 50%;
        font-size:12px;
        color:@base-color;
        line-height:12px;
        margin-right:10px;
      }
      width: 369px;
      height: 30px;
      line-height:30px;
      color:@fff;
      font-weight: @font-medium;
      border-radius: 2px;
      box-shadow: 0 0 4px 0 rgba(58, 160, 229, 0.9);
      background-image: linear-gradient(193deg, #15bdf1, @base-color);
    }
    .guid-content {
      width: 360px;
      float: right;
      border-radius: 2px;
      box-shadow: 0 0 4px 0 rgba(58, 160, 229, 0.9);
      background-color: @base-color;
      margin-top: 10px;
      .steps-info {
        li {
          // &:last-child {
          //   padding-bottom:10px;
          // }
          cursor:pointer;
          width: 325px;
          background-color: #087ac8;
          padding-left:24px;
          padding-right:10px;
          padding-top:5px;
          padding-bottom: 5px;
          .dot {
            .ky-square-box(4px, 4px);
            border-radius:50%;
            background-color:@fff;
            display:inline-block;
            margin-right:9px;
          }
          &.guiding-step {
            width: 325px;
            &.guide-end {
              background-image: linear-gradient(193deg, #15bdf1, #0988de);
            }
          }
        }
      }
    }
  }
}
</style>
