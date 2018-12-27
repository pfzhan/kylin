<template>
  <div class="global-mask" v-if="globalMaksShow">
        <div id="guid-panel">
          <div class="guid-icon"></div>
          <transition name="bounce">
            <div class="guid-title" v-if="showGuid.showTitle">
              <span>Manual Mode Guide</span>
              <i class="el-icon-close ksd-fright ksd-mt-8 ksd-mr-10" @click="closeGuide"></i>
            </div>
          </transition>
          <transition name="panelani">
            <div class="guid-content" v-if="showGuid.showContent">
                <el-tabs v-model="activeName" @tab-click="">
                  <el-tab-pane label="New Project" name="project">
                    <div class="ksd-ml-20 ksd-mtb-6">操作提示：</div>
                    <ul class="steps-info">
                      <li class="guiding-step" @click="guide.go(10)"><span class="dot"></span>添加project</li>
                      <li></li>
                      <li></li>
                    </ul>
                  </el-tab-pane>
                  <el-tab-pane label="Load Source" name="loadTable">
                    <div class="ksd-ml-20 ksd-mtb-6">操作提示：</div>
                    <ul class="steps-info">
                      <li class="guiding-step"><span class="dot"></span>加载数据源</li>
                      <li></li>
                      <li></li>
                    </ul>
                  </el-tab-pane>
                  <el-tab-pane label="Add Model" name="addModel">
                    <div class="ksd-ml-20 ksd-mtb-6">操作提示：</div>
                    <ul class="steps-info">
                      <li class="guiding-step"><span class="dot"></span>添加模型</li>
                      <li></li>
                      <li></li>
                    </ul>
                  </el-tab-pane>
                  <el-tab-pane label="Monitor" name="fourth">Monitor</el-tab-pane>
                </el-tabs>
                <el-button class="ksd-fright guide-btn" size="mini" :loading="guideLoading" @click="goNextStep" plain>{{currentStep === 0 ? '开始' : guideLoading ? '演示中' : '下一步'}}</el-button>
            </div>
          </transition>
        </div>
        <img src="../../../assets/img/guide/cursor-pointer.png" v-if="globalMouseShow" key="pointer-pic" :style="mousePos"/>
        <img src="../../../assets/img/guide/cursor-click.png" v-if="globalMouseClick" key="pointer-pic" :style="mousePos"/>
        <img src="../../../assets/img/guide/cursor-bg.png" v-if="globalMouseDrag" key="pointer-pic" :style="mousePos"/>
    </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
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
    globalMaksShow () {
      let showGuideMask = this.$store.state.system.guideConfig.globalMaskVisible
      if (showGuideMask) {
        this.$nextTick(() => {
          this.showGuid.showTitle = showGuideMask
          this.showGuid.showContent = showGuideMask
        })
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
  }
})
export default class GuidePannel extends Vue {
  showGuid = {
    showTitle: false,
    showContent: false
  }
  currentStep = 0
  guideLoading = false
  activeName = 'project'
  guideSteps = ['project', 'loadTable', 'addModel', 'monitor']
  guide = null
  goNextStep () {
    let currentGuidePage = this.guideSteps[this.currentStep]
    this.guide = new Guide({
      mode: currentGuidePage
    }, this)
    this.activeName = currentGuidePage
    this.currentStep++
    this.guideLoading = true
    this.guide.start().go().then(() => {
      this.guideLoading = false
    }, () => {
      this.guideLoading = false
    })
  }
  closeGuide () {
    this.guideLoading = false
    this.currentStep = 0
    this.guide.stop()
    this.showGuid.showTitle = false
    this.showGuid.showContent = false
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
.global-mask {
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
  z-index: 999999;
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
    right:20px;
    top:60px;
    .guid-icon {
      width: 41px;
      height: 41px;
      box-shadow: 0 1px 3px 0 @text-normal-color;
      background-color: @fff;
      border-radius: 50%;
      position: absolute;
      top:-5px;
      left:-20px;
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
          cursor:pointer;
          width: 360px;
          height: 28px;
          line-height:28px;
          background-color: #087ac8;
          padding-left:24px;
          .dot {
            .ky-square-box(4px, 4px);
            border-radius:50%;
            background-color:@fff;
            display:inline-block;
            margin-right:9px;
          }
          &.guiding-step {
            width: 360px;
            height: 28px;
            background-image: linear-gradient(193deg, #15bdf1, #0988de);
          }
        }
      }
    }
  }
}
</style>
