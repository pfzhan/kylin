<template>
  <el-dialog class="global-dialog-box"
    width="480px"
    :before-close="handleClose"
    :title="title || $t('kylinLang.common.notice')"
    :close-on-click-modal="false"
    :append-to-body="true"
    :visible.sync="isShow"
    limited-area
    :close-on-press-escape="false">
    <el-alert
      :type="dialogType"
      :show-background="false"
      :closable="false"
      show-icon>
      <span style="word-break: break-word;" v-html="filterInjectScript(msg).replace(/\r\n/g, '<br/><br/>')"></span>
      <a href="javascript:;" @click="toggleDetail" v-if="showDetailBtn" class="show-detail">{{$t('kylinLang.common.seeDetail')}}  
        <i class="el-icon-arrow-down" v-show="!showDetail"></i>
        <i class="el-icon-arrow-up" v-show="showDetail"></i>
      </a>
      <p v-if="showDetail && detailMsg" class="ksd-mt-15 detailMsg" :class="{'en-lang': $store.state.system.lang === 'en'}" v-html="filterInjectScript(detailMsg).replace(/\n/g, '<br/>')"></p>
      <div v-if="showDetail" style="padding-top:10px;">
        <template v-if="theme === 'plain-mult'">
          <div v-for="item in details">
            <p class="mult-title">{{item.title}}</p>
            <div class="dialog-detail">
              <div v-scroll class="dialog-detail-scroll">
                <ul>
                  <li v-for="(p, index) in item.list" :key="index">{{p}}</li>
                </ul>
              </div>
              <el-button class="copyBtn" v-if="showCopyBtn" size="mini" v-clipboard:copy="item.list.join('\r\n')"
          v-clipboard:success="onCopy" v-clipboard:error="onError">{{$t('kylinLang.common.copy')}}</el-button>
            </div>
          </div>
        </template>
        <template v-if="theme === 'plain'">
          <div class="dialog-detail">
            <div v-scroll class="dialog-detail-scroll">
              <ul>
                <li v-for="(p, index) in details" :key="index">{{p}}</li>
              </ul>
            </div>
            <el-button class="copyBtn" v-if="showCopyBtn" size="mini" v-clipboard:copy="details.join('\r\n')"
          v-clipboard:success="onCopy" v-clipboard:error="onError">{{$t('kylinLang.common.copy')}}</el-button>
          </div>
        </template>
        <template v-if="theme === 'sql'">
          <div v-scroll class="dialog-detail">
            <kap-editor :readOnly="true" :isFormatter="true" class="list-editor" v-for="(s, index) in details" :key="index" height="96" lang="sql" theme="chrome" v-bind:value="s"></kap-editor>
          </div>
        </template>
      </div>
    </el-alert>
    <div slot="footer" class="dialog-footer ky-no-br-space">
      <template v-if="dialogType === 'error'">
        <el-button type="default" @click="handleClose">{{$t('kylinLang.common.close')}}</el-button>
      </template>
      <template v-else>
        <el-button type="default" @click="handleClose">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain :loading="loading" @click="handleSubmit">{{$t('kylinLang.common.submit')}}</el-button>
      </template>
    </div>
  </el-dialog>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapMutations } from 'vuex'

import vuex from '../../../../store'
import store, { types } from './store'
import { filterInjectScript } from 'util'
vuex.registerModule(['modals', 'DetailDialogModal'], store)

@Component({
  computed: {
    ...mapState('DetailDialogModal', {
      title: state => state.title,
      details: state => state.details,
      theme: state => state.theme,
      msg: state => state.msg,
      detailMsg: state => state.detailMsg, // 详情里其他的文案信息
      isShow: state => state.isShow,
      dialogType: state => state.dialogType,
      showDetailBtn: state => state.showDetailBtn, // 控制是否需要显示详情按钮，默认是显示的
      showCopyBtn: state => state.showCopyBtn,
      callback: state => state.callback
    })
  },
  methods: {
    ...mapMutations('DetailDialogModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      initForm: types.INIT_FORM,
      resetModal: types.RESET_MODAL
    })
  }
})
export default class DetailDialogModal extends Vue {
  filterInjectScript = filterInjectScript
  loading = false
  showDetail = false
  handleClose () {
    this.hideModal()
    this.resetModal()
    this.loading = false
    this.showDetail = false
  }
  handleSubmit () {
    this.loading = true
    setTimeout(() => {
      this.callback && this.callback()
      this.handleClose()
    }, 200)
  }
  toggleDetail () {
    this.showDetail = !this.showDetail
  }
  onCopy () {
    this.$message({
      type: 'success',
      message: this.$t('kylinLang.common.copySuccess')
    })
  }
  onError () {
    this.$message({
      type: 'error',
      message: this.$t('kylinLang.common.copyfail')
    })
  }
}
</script>
<style lang="less">
@import '../../../../assets/styles/variables.less';
.global-dialog-box {
  .show-detail{
    display: inline-block;
  }
  .detailMsg{
    font-size: 12px;
    color: @text-normal-color;
    line-height: 1.5;
    margin-bottom: -5px;
    &.en-lang{
      line-height: 1.2;
    }
  }
  .mult-title{
    margin-bottom:5px;
    margin-top:5px;
  }
  .dialog-detail{
    border-radius: 2px;
    border:solid 1px @line-border-color;
    background:@background-disabled-color;
    margin-bottom:10px;
    position: relative;
    .dialog-detail-scroll{
      max-height:95px;
      ul {
        margin:10px;
        li {
          font-size: 12px;
        }
      }
    }
    .copyBtn{
      position: absolute;
      right:5px;
      top:5px;
    }
  }
  .el-alert__content {
    width:100%;
  }
  .el-alert {
    padding: 0;
  }
  .list-editor {
    margin-bottom:10px;
    overflow: hidden;
    margin: 10px auto;
    width:calc(~'100% - 20px')!important;
  }
}
</style>
