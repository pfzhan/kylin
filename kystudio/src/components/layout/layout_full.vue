<template>
   <div>
    <router-view></router-view>
    <!-- 全局级别loading -->
    <kap-loading v-show="$store.state.config.showLoadingBox"></kap-loading>
    <!-- 全局级别错误提示框 -->
    <el-dialog class="errMsgBox"
       width="660px"
      :before-close="handleClose"
      :title="$t('kylinLang.common.tip')"
      :close-on-click-modal="false"
      :append-to-body="true"
      :visible.sync="$store.state.config.errorMsgBox.isShow"
      :close-on-press-escape="false">
      <div class="el-message-box__status el-icon-error" style="display: inline-block"></div>
      <span style="margin-left: 48px;display:inline-block;word-break: break-word;" v-html="$store.state.config.errorMsgBox.msg.replace(/</, '&lt;').replace(/>/, '&gt;').replace(/\r\n/g, '<br/><br/>')"></span>
      <div slot="footer" class="dialog-footer" style="overflow: hidden">
        <div class="ksd-mb-10">
          <el-button type="default" size="medium" @click="handleClose">{{$t('kylinLang.common.ok')}}</el-button>
          <el-button type="primary" plain size="medium" @click="toggleDetail">
            {{$t('kylinLang.common.seeDetail')}} 
            <i class="el-icon-arrow-down" v-show="!showDetail"></i>
            <i class="el-icon-arrow-up" v-show="showDetail"></i>
          </el-button>
        </div>
        <!-- <div @click="toggleDetail()" class="ksd-mb-10" v-html="$t('kylinLang.common.seeDetail')"></div> -->
        <el-input :rows="4" ref="detailBox" readonly type="textarea" v-show="showDetail" id="errorDetail" v-model="$store.state.config.errorMsgBox.detail"></el-input>
      <div class="ksd-left">
        <el-button size="small" v-clipboard:copy="$store.state.config.errorMsgBox.detail"
        v-clipboard:success="onCopy" v-clipboard:error="onError" type="default" v-show="showDetail" class="ksd-fleft ksd-mt-10 ksd-mb-20">{{$t('kylinLang.common.copy')}}</el-button>
        <transition name="fade">
          <div class="copyStatusMsg" v-show="showCopyStatus && showDetail" ><i class="el-icon-circle-check"></i> <span>{{$t('kylinLang.common.copySuccess')}}</span></div>
        </transition>
      </div>
    </div>
    </el-dialog>
    <!-- 全局级别的kyaccount登陆框 -->
    <el-dialog class="login-kybotAccount" :visible.sync="$store.state.kybot.loginKyaccountDialog" :title="$t('kylinLang.login.signIn')" @close="resetLoginKybotForm" :close-on-click-modal="false">
      <login_kybot ref="loginKybotForm" @closeLoginForm="closeLoginForm" @closeLoginOpenKybot="closeLoginForm"></login_kybot>
    </el-dialog>
    <Modal />
    <!-- 引导模式选择弹窗 -->
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
            <div class="guide-footer">Start</div>
          </el-col>
        </el-row>
      </div>
    </el-dialog>
    <!-- 引导控制面板 -->
    <div id="global-mask" v-if="globalMaksShow">
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
                  <el-tab-pane label="New Project" name="first">
                    <div class="ksd-ml-20 ksd-mtb-6">操作提示：</div>
                    <ul class="steps-info">
                      <li class="guiding-step" @click="guide.go(10)"><span class="dot"></span>添加project</li>
                      <li></li>
                      <li></li>
                    </ul>
                  </el-tab-pane>
                  <el-tab-pane label="Load Source" name="second">
                    <div class="ksd-ml-20 ksd-mtb-6">操作提示：</div>
                    <ul class="steps-info">
                      <li class="guiding-step" @click="guide.go(21)"><span class="dot"></span>加载数据源</li>
                      <li></li>
                      <li></li>
                    </ul>
                  </el-tab-pane>
                  <el-tab-pane label="Add Model" name="third">
                    <div class="ksd-ml-20 ksd-mtb-6">操作提示：</div>
                    <ul class="steps-info">
                      <li class="guiding-step" @click="guide.go(21)"><span class="dot"></span>添加模型</li>
                      <li></li>
                      <li></li>
                    </ul>
                  </el-tab-pane>
                  <el-tab-pane label="Monitor" name="fourth">Monitor</el-tab-pane>
                </el-tabs>
            </div>
          </transition>
        </div>
        <img src="../../assets/img/guide/cursor-pointer.png" v-if="globalMouseShow" key="pointer-pic" :style="mousePos"/>
        <img src="../../assets/img/guide/cursor-click.png" v-if="globalMouseClick" key="pointer-pic" :style="mousePos"/>
        <img src="../../assets/img/guide/cursor-bg.png" v-if="globalMouseDrag" key="pointer-pic" :style="mousePos"/>
    </div>
   </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import loginKybot from '../common/login_kybot.vue'
import Modal from '../common/Modal/Modal'
import Guide from '../../util/guide'
import { handleSuccess, handleError } from '../../util/business'
@Component({
  methods: {
    ...mapActions({
      getKybotAccount: 'GET_CUR_ACCOUNTNAME'
    })
  },
  components: {
    'login_kybot': loginKybot,
    Modal
  },
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
export default class LayoutFull extends Vue {
  showDetail = false
  showCopyStatus = false
  showGuid = {
    showTitle: false,
    showContent: false
  }
  guide = null
  startManual () {
    this.closeShowGuideModeCheckDialog()
    setTimeout(() => {
      this.guide = new Guide({}, this)
      // this.guide.start()
    }, 1000)
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
  toggleDetail () {
    this.showDetail = !this.showDetail
  }
  onCopy () {
    this.showCopyStatus = true
    setTimeout(() => {
      this.showCopyStatus = false
    }, 3000)
  }
  resetLoginKybotForm () {
    this.$refs['loginKybotForm'].$refs['loginKybotForm'].resetFields()
  }
  closeLoginForm () {
    this.$store.state.kybot.loginKyaccountDialog = false
    this.getKybotAccount().then((res) => {
      handleSuccess(res, (data, code, status, msg) => {
        this.$store.state.kybot.hasLoginAccount = data
      })
    }, (res) => {
      handleError(res)
    })
  }
  onError () {
    this.$message(this.$t('kylinLang.common.copyfail'))
  }
  handleClose () {
    this.showDetail = false
    this.$store.state.config.errorMsgBox.isShow = false
    this.$refs.detailBox.$el.firstChild.scrollTop = 0
  }
  destroyed () {
    this.showCopyStatus = false
  }
  mounted () {
  }
}
</script>

<style lang="less">
@import '../../assets/styles/variables.less';
*{
	margin: 0;
	padding: 0;
}
body{
	-webkit-font-smoothing: antialiased;
	-moz-osx-font-smoothing: grayscale;
}

#global-mask {
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
      height: 165px;
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
.errMsgBox {
  .el-dialog__body{
    // border-top: 1px solid #2b2d3c;
    // box-shadow: 0 -1px 0 0 #424860;
    position: relative;
    .el-input{
      font-size: 12px;
    }
  }
  .copyStatusMsg{
    display: inline-block;
    // float: left;
    margin-top: 18px;
    margin-left: 10px;
    i{
      color:#28cd6b;
    }
    span{
      font-size: 12px;
    }
  }
  .el-textarea__inner {
    background-color: @table-stripe-color;
    font-size: 12px;
  }
}
</style>
