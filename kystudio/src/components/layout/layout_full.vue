<template>
   <div>
    <router-view></router-view>
    <el-dialog class="errMsgBox"
      :before-close="handleClose"
      :title="$t('kylinLang.common.tip')"
      :close-on-click-modal="false"
      :visible.sync="$store.state.config.errorMsgBox.isShow"
      :close-on-press-escape="false"
      size="tiny">
      <div class="el-message-box__status el-icon-circle-cross" style="display: inline-block"></div>
      <span style="margin-left: 48px;display:inline-block;word-break: break-all;" v-html="$store.state.config.errorMsgBox.msg.replace(/\r\n/g, '<br/><br/>')"></span>
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
        <div class="copyStatusMsg" v-show="showCopyStatus" ><i class="el-icon-circle-check"></i> <span>{{$t('copySuccess')}}</span></div>
      </transition>
      </span>
    </el-dialog>
   </div>
</template>

<script>
export default {
  data () {
    return {
      showDetail: false,
      showCopyStatus: false
    }
  },
  methods: {
    toggleDetail () {
      this.showDetail = !this.showDetail
    },
    onCopy () {
      this.showCopyStatus = true
      setTimeout(() => {
        this.showCopyStatus = false
      }, 3000)
    },
    onError () {
      this.$message(this.$t('copyfail'))
    },
    handleClose () {
      this.showDetail = false
      this.$store.state.config.errorMsgBox.isShow = false
      this.$refs.detailBox.$el.firstChild.scrollTop = 0
    }
  },
  mounted () {
  },
  destroyed () {
    this.showCopyStatus = false
  },
  locales: {
    'en': {'copySuccess': 'Content has been copied to the Clipboard.', 'copyfail': 'Failed to copy! Your browser does not support paste boards!'},
    'zh-cn': {'copySuccess': '复制成功！', 'copyfail': '复制失败，您的浏览器不支持粘贴板功能!'}
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
