<template>
  <el-dialog :title="$t('computedDetail')" append-to-body width="440px" :visible="isShow" :close-on-press-escape="false" :close-on-click-modal="false" @close="isShow && closeModal()">
    <div class="cc-detail-box" v-if="ccDetail">
      <el-row :gutter="4">
        <el-col :span="7" class="ksd-right">{{$t('columnName')}}</el-col>
        <el-col :span="17" class="ksd-left">{{ccDetail.columnName}}</el-col>
      </el-row>
      <el-row :gutter="4">
        <el-col :span="7" class="ksd-right">{{$t('returnType')}}</el-col>
        <el-col :span="17" class="ksd-left">{{ccDetail.datatype}}</el-col>
      </el-row>
      <div class="ksd-mt-18 express-box">
        <p>{{$t('kylinLang.dataSource.expression')}}</p>
        <div class="ksd-mt-6">
          <kap-editor ref="ccSql" height="100" lang="sql" theme="chrome" v-model="ccDetail.expression"></kap-editor>
        </div>
      </div>
    </div>
  </el-dialog>
</template>
<script>
import Vue from 'vue'
import vuex from '../../../../store'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations } from 'vuex'
import locales from './locales'
import store, { types } from './store'
vuex.registerModule(['modals', 'ShowCCDialogModal'], store)
@Component({
  computed: {
    ...mapState('ShowCCDialogModal', {
      isShow: state => state.isShow,
      ccDetail: state => state.form.ccDetail,
      callback: state => state.callback
    })
  },
  methods: {
    ...mapMutations('ShowCCDialogModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  locales
})
export default class ShowCCDialogModal extends Vue {
  @Watch('isShow')
  initDialog () {
    if (this.isShow) {
      this.$nextTick(() => {
        this.$refs.ccSql.$emit('setReadOnly')
      })
    }
  }
  closeModal (isSubmit) {
    this.hideModal()
    setTimeout(() => {
      this.callback && this.callback(isSubmit)
      this.resetModalForm()
    }, 200)
  }
}
</script>

<style lang="less">
  @import '../../../../assets/styles/variables.less';
  .cc-detail-box {
    margin-bottom:70px;
    .el-row {
      margin:0 auto;
      width:400px;
      height:46px;
      background-color:@fff;
      border:solid 1px @text-placeholder-color;
      &:first-child{
        border-bottom:none;
        background-color: @table-stripe-color;
      }
      .el-col {
        &:first-child {
          font-weight: @font-medium;
        }
        line-height: 46px;
      }
    }
    .express-box {
      p {
        font-weight: @font-medium;
      }
    }
  }
</style>
