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
   </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import loginKybot from '../common/login_kybot.vue'
import Modal from '../common/Modal'
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
  }
})
export default class LayoutFull extends Vue {
  showDetail = false
  showCopyStatus = false

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
