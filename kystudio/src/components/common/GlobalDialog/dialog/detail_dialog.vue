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
      <a href="javascript:;" @click="toggleDetail">{{$t('kylinLang.common.seeDetail')}}  
        <i class="el-icon-arrow-down" v-show="!showDetail"></i>
        <i class="el-icon-arrow-up" v-show="showDetail"></i>
      </a>
      <div v-if="showDetail" v-scroll class="dialog-detail">
        <template v-if="theme === 'plain'">
          <ul>
            <li v-for="(p, index) in details" :key="index">{{p}}</li>
          </ul>
        </template>
        <template v-if="theme === 'sql'">
          <kap-editor :readOnly="true" :isFormatter="true" class="list-editor" v-for="(s, index) in details" :key="index" height="96" lang="sql" theme="chrome" v-bind:value="s"></kap-editor>
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
      isShow: state => state.isShow,
      dialogType: state => state.dialogType,
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
}
</script>
<style lang="less">
@import '../../../../assets/styles/variables.less';
.global-dialog-box {
  .dialog-detail{
    border:solid 1px @line-border-color;
    background:@background-disabled-color;
    max-height:230px;
    margin-top:10px;
    ul {
      margin:10px;
      li {
        font-size: 12px;
      }
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
