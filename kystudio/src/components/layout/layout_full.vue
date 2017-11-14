<template>
   <div>
    <router-view></router-view>

    <!-- 全局级别错误提示框 -->
    <el-dialog class="errMsgBox"
      :before-close="handleClose"
      :title="$t('kylinLang.common.tip')"
      :close-on-click-modal="false"
      :visible.sync="$store.state.config.errorMsgBox.isShow"
      :close-on-press-escape="false"
      size="tiny">
      <div class="el-message-box__status el-icon-circle-cross" style="display: inline-block"></div>
      <span style="margin-left: 48px;display:inline-block;word-break: break-word;" v-html="$store.state.config.errorMsgBox.msg.replace(/</, '&lt;').replace(/>/, '&gt;').replace(/\r\n/g, '<br/><br/>')"></span>
      <span slot="footer" class="dialog-footer">
        <div class="ksd-mb-10">
          <el-button type="primary" @click="handleClose">{{$t('kylinLang.common.ok')}}</el-button>
          <el-button type="default" @click="toggleDetail()">{{$t('kylinLang.common.seeDetail')}}</el-button>
        </div>
        <!-- <div @click="toggleDetail()" class="ksd-mb-10" v-html="$t('kylinLang.common.seeDetail')"></div> -->
        <el-input :rows="4" ref="detailBox" readonly type="textarea" v-show="showDetail" id="errorDetail" v-model="$store.state.config.errorMsgBox.detail"></el-input>
        <el-button v-clipboard:copy="$store.state.config.errorMsgBox.detail"
      v-clipboard:success="onCopy" v-clipboard:error="onError" type="default" v-show="showDetail" class="ksd-fleft ksd-mt-10 ksd-mb-20">{{$t('kylinLang.common.copy')}}</el-button>
      <transition name="fade">
        <div class="copyStatusMsg" v-show="showCopyStatus" ><i class="el-icon-circle-check"></i> <span>{{$t('kylinLang.common.copySuccess')}}</span></div>
      </transition>
      </span>
    </el-dialog>
    <!-- 全局级别的kyaccount登陆框 -->
    <el-dialog class="login-kybotAccount" v-model="$store.state.kybot.loginKyaccountDialog" :title="$t('kylinLang.login.signIn')" size="tiny" @close="resetLoginKybotForm" :close-on-click-modal="false">
      <login_kybot ref="loginKybotForm" @closeLoginForm="closeLoginForm" @closeLoginOpenKybot="closeLoginForm"></login_kybot>
    </el-dialog>
   </div>
</template>

<script>
import { mapActions } from 'vuex'
import loginKybot from '../common/login_kybot.vue'
import { handleSuccess, handleError } from '../../util/business'
export default {
  data () {
    return {
      showDetail: false,
      showCopyStatus: false
    }
  },
  methods: {
    ...mapActions({
      getKybotAccount: 'GET_CUR_ACCOUNTNAME'
    }),
    toggleDetail () {
      this.showDetail = !this.showDetail
    },
    onCopy () {
      this.showCopyStatus = true
      setTimeout(() => {
        this.showCopyStatus = false
      }, 3000)
    },
    resetLoginKybotForm () {
      this.$refs['loginKybotForm'].$refs['loginKybotForm'].resetFields()
    },
    closeLoginForm () {
      this.$store.state.kybot.loginKyaccountDialog = false
      this.getKybotAccount().then((res) => {
        handleSuccess(res, (data, code, status, msg) => {
          this.$store.state.kybot.hasLoginAccount = data
        })
      }, (res) => {
        handleError(res)
      })
    },
    onError () {
      this.$message(this.$t('kylinLang.common.copyfail'))
    },
    handleClose () {
      this.showDetail = false
      this.$store.state.config.errorMsgBox.isShow = false
      this.$refs.detailBox.$el.firstChild.scrollTop = 0
    }
  },
  mounted () {
  },
  computed: {
  },
  components: {
    'login_kybot': loginKybot
  },
  destroyed () {
    this.showCopyStatus = false
  },
  locales: {
    'en': {},
    'zh-cn': {}
  }
}
</script>
<style lang="less">
@import '../../less/config.less';
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
    position: relative;
    .el-input{
      font-size: 12px;
    }
  }
  .copyStatusMsg{
    float: left;
    margin-top: 18px;
    margin-left: 10px;
    color:#fff;
    i{
      color:#28cd6b;
    }
    span{
      color:#fff;
      font-size: 12px;
    }
  }
  .el-dialog__footer {
    border-top: none;
    padding-bottom: 20px;
    div {
      font-size: 14px;
      color:rgba(255, 255, 255, 0.6);
      cursor: pointer;
    }
  }
}
</style>
