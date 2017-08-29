<template>
   <div>
    <router-view></router-view>
    <el-dialog class="errMsgBox"
      :before-close="handleClose"
      :title="$t('kylinLang.common.tip')"
      :visible.sync="$store.state.config.errorMsgBox.isShow"
      size="tiny">
      <div class="el-message-box__status el-icon-circle-cross" style="display: inline-block"></div>
      <span style="margin-left: 48px;display:inline-block;">{{$store.state.config.errorMsgBox.msg}}</span>
      <span slot="footer" class="dialog-footer">
        <div @click="toggleDetail()" class="ksd-mb-10" v-html="$t('kylinLang.common.seeDetail')"></div>
        <el-input :rows="4" ref="detailBox" readonly type="textarea" v-show="showDetail" v-model="$store.state.config.errorMsgBox.detail"></el-input>
      </span>
    </el-dialog>
   </div>
</template>

<script>
export default {
  data () {
    return {
      showDetail: false
    }
  },
  methods: {
    toggleDetail () {
      this.showDetail = !this.showDetail
    },
    handleClose () {
      this.showDetail = false
      this.$store.state.config.errorMsgBox.isShow = false
      this.$refs.detailBox.$el.firstChild.scrollTop = 0
    }
  }
}
</script>
<style lang="less">
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
