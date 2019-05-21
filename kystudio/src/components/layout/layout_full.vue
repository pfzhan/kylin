<template>
   <div>
    <router-view></router-view>
    <!-- 全局级别loading -->
    <kap-loading v-show="$store.state.config.showLoadingBox"></kap-loading>
    <!-- 全局级别错误提示框 -->
    <el-dialog class="errMsgBox"
       width="480px"
      :before-close="handleClose"
      :title="$t('kylinLang.common.notice')"
      :close-on-click-modal="false"
      :append-to-body="true"
      :visible.sync="$store.state.config.errorMsgBox.isShow"
      :close-on-press-escape="false">
      <el-alert
        type="error"
        :show-background="false"
        :closable="false"
        show-icon>
        <span style="word-break: break-word;" v-html="filterInjectScript($store.state.config.errorMsgBox.msg).replace(/\r\n/g, '<br/><br/>')"></span>
        <a href="javascript:;" @click="toggleDetail">{{$t('kylinLang.common.seeDetail')}}  
          <i class="el-icon-arrow-down" v-show="!showDetail"></i>
          <i class="el-icon-arrow-up" v-show="showDetail"></i>
        </a>
        <el-input :rows="4" ref="detailBox" readonly type="textarea" v-show="showDetail" class="ksd-mt-10" v-model="$store.state.config.errorMsgBox.detail"></el-input>
        <div class="ksd-left">
          <el-button size="small" v-clipboard:copy="$store.state.config.errorMsgBox.detail"
          v-clipboard:success="onCopy" v-clipboard:error="onError" type="default" v-show="showDetail" class="ksd-fleft ksd-mt-10">{{$t('kylinLang.common.copy')}}</el-button>
          <transition name="fade">
            <div class="copy-status-msg" v-show="showCopyStatus && showDetail" ><i class="el-icon-circle-check"></i> <span>{{$t('kylinLang.common.copySuccess')}}</span></div>
          </transition>
        </div>
      </el-alert>    
      <div slot="footer" class="dialog-footer">
        <el-button type="default" size="medium" @click="handleClose">{{$t('kylinLang.common.close')}}</el-button>
      </div>
    </el-dialog>
    <Modal />
    <GuidType />
    <GuidePanel />
    <!-- 引导模式选择弹窗 -->
    <!-- 引导控制面板 -->
    
   </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import GuidType from '../common/Guide/GuideType'
import Modal from '../common/Modal/Modal'
import GuidePanel from '../common/Guide/GuidePanel'
import { filterInjectScript } from 'util'
@Component({
  methods: {
    ...mapActions({
      getKybotAccount: 'GET_CUR_ACCOUNTNAME'
    })
  },
  components: {
    Modal,
    GuidType,
    GuidePanel
  }
})
export default class LayoutFull extends Vue {
  showDetail = false
  showCopyStatus = false
  filterInjectScript = filterInjectScript
  toggleDetail () {
    this.showDetail = !this.showDetail
  }
  onCopy () {
    this.showCopyStatus = true
    setTimeout(() => {
      this.showCopyStatus = false
    }, 3000)
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
.errMsgBox {
  .el-dialog__body{
    .el-alert{
      padding:0;
    }
    .el-alert__content {
      width:100%;
      padding-right: 0;
    }
    // border-top: 1px solid #2b2d3c;
    // box-shadow: 0 -1px 0 0 #424860;
    position: relative;
    .el-input{
      font-size: 12px;
    }
  }
  .copy-status-msg{
    display: inline-block;
    margin-top: 10px;
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
