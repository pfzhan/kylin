<template>
  <div class="query_panel_box">
    <div class="resultTips"  v-show="errinfo">
      <div class="grid-content bg-purple"><p><i class="el-icon-ksd-status"></i> {{$t('kylinLang.query.status')}}<span>error</span></p></div>
      <div class="grid-content bg-purple"><p><i class="el-icon-ksd-calendar"></i> {{$t('kylinLang.query.startTime')}}<span>{{transToGmtTime(startTime)}}</span></p></div>
      <!-- <div class="grid-content bg-purple"><p style="visibility:hidden">{{$t('kylinLang.query.duration')}}<span> </span></p></div> -->
      <div class="grid-content bg-purple"><p><i class="el-icon-ksd-project"></i> {{$t('kylinLang.query.project')}}<span>{{extraoption.project}}</span></p></div>
    </div>
    <div class="grid-content bg-purple" style="text-align:right; margin-top: 10px;"  v-show="errinfo">
      <kap-icon-button size="small" type="primary" plain="plain" @click.native="openSaveQueryDialog" style="display:inline-block">{{$t('kylinLang.query.saveQuery')}}</kap-icon-button>
      <kap-icon-button size="small" type="blue" @click.native="refreshQuery" style="display:inline-block">{{$t('kylinLang.query.refreshQuery')}}</kap-icon-button>
    </div>
    <div v-show="!errinfo">
      <el-progress type="circle" :percentage="percent"></el-progress>
    </div>
	  <div v-show="errinfo" class="errorBox">
      <i class="el-icon-ksd-error_01 ksd-fs-16"></i>
      <span class = "errorText">{{$t(''+errinfo)}}</span>
    </div>
    <save_query_dialog :show="saveQueryFormVisible" :extraoption="extraoption" v-on:closeModal="closeModal" v-on:reloadSavedProject="reloadSavedProject"></save_query_dialog>
  </div>

</template>
<script>
import { mapActions } from 'vuex'
import saveQueryDialog from 'components/insight/save_query_dialog'
import { handleSuccess, handleError, transToGmtTime } from '../../util/business'
export default {
  name: 'queryPanel',
  props: ['extraoption'],
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      query: 'QUERY_BUILD_TABLES',
      saveQueryToServer: 'SAVE_QUERY'
    }),
    refreshQuery () {
      this.queryResult()
    },
    queryResult () {
      this.errinfo = ''
      this.pending = 0
      this.percent = 0
      this.query({
        acceptPartial: this.extraoption.acceptPartial,
        limit: this.extraoption.limit,
        offset: this.extraoption.offset,
        project: this.extraoption.project,
        sql: this.extraoption.sql,
        backdoorToggles: this.extraoption.backdoorToggles
      }).then((res) => {
        clearInterval(this.ST)
        handleSuccess(res, (data) => {
          this.$emit('changeView', this.extraoption.index, data)
        })
      }, (res) => {
        handleError(res, (data, code, status, msg) => {
          this.errinfo = msg || 'kylinLang.common.timeOut'
          this.$emit('changeView', this.extraoption.index, data, 'el-icon-ksd-error_01', 'querypanel')
        })
      })
    },
    openSaveQueryDialog () {
      this.saveQueryFormVisible = true
    },
    closeModal () {
      this.saveQueryFormVisible = false
    },
    reloadSavedProject () {
      this.$emit('reloadSavedProject', 0)
    }
  },
  components: {
    'save_query_dialog': saveQueryDialog
  },
  data () {
    return {
      errinfo: '',
      percent: 0,
      ST: null,
      pending: 0,
      startTime: Date.now(),
      saveQueryFormVisible: false
    }
  },
  mounted () {
    var _this = this
    clearInterval(this.ST)
    this.ST = setInterval(() => {
      this.pending += 300
      var randomPlus = Math.round(10 * Math.random())
      if (_this.percent + randomPlus < 99) {
        _this.percent += randomPlus
      } else {
        clearInterval(_this.ST)
      }
    }, 300)
    this.queryResult()
  },
  locales: {
    'en': {username: 'Username', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin'},
    'zh-cn': {username: '用户名', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '管理人员'}
  },
  destoryed () {
    clearInterval(this.ST)
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .query_panel_box{
    text-align: center;
    .resultTips {
      overflow: hidden;
      clear: both;
      text-align: left;
      .grid-content {
        display: inline-block;
        margin-right: 30px;
      }
    }
    .el-dialog__header{
      text-align: left;
    }
    .tips{
      font-size: 12px;
      margin-bottom: 20px;
      span{
        color: #20a0ff;
      }
    }
    .el-progress{
      margin-bottom: 10px;
    }
    .panel_btn{
      width: 140px;
      margin-top: 20px;
      border-top: solid 1px #ccc;
      margin: 0 auto;
      padding-top: 10px;
    }
    .errorBox{
      color: @color-text-primary;
      font-size: 14px;
      border-top: 1px dashed @border-color-base;
      margin-top: 20px;
      padding: 50px 20px;
      i{
        font-size: 12px;
        margin-bottom: 20px;
        &.el-icon-ksd-error_01 {
          color:red;
          font-size: 12px;
        }
      }
    }
  }
</style>
