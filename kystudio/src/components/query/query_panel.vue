<template>
  <div id="queryPanelBox">
    <div class="resultTipsLine" v-show="errinfo">
      <div class="resultTips">
        <p class="resultText"><span class="label">{{$t('kylinLang.query.queryId')}}</span>
        <span class="text">{{queryId}}</span></p>
        <p class="resultText"><span class="label">{{$t('kylinLang.query.status')}}</span>
        <span class="ky-error">{{$t('kylinLang.common.error')}}</span></p>
      </div>
      <div class="error-block">{{errinfo}}</div>
    </div>
    <div v-show="!errinfo" class="ksd-center ksd-mt-10">
      <el-progress type="circle" :percentage="percent"></el-progress>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import saveQueryDialog from './save_query_dialog'
import { handleSuccess, handleError, transToGmtTime } from '../../util/business'
@Component({
  props: ['extraoption'],
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      query: 'QUERY_BUILD_TABLES'
    })
  },
  components: {
    'save_query_dialog': saveQueryDialog
  },
  locales: {
    'en': {username: 'Username', role: 'Role', analyst: 'Analyst', modeler: 'Modeler', admin: 'Admin'},
    'zh-cn': {username: '用户名', role: '角色', analyst: '分析人员', modeler: '建模人员', admin: '管理人员'}
  }
})
export default class queryPanel extends Vue {
  errinfo = ''
  percent = 0
  ST = null
  pending = 0
  startTime = Date.now()
  saveQueryFormVisible = false
  queryId = '-'

  refreshQuery () {
    this.queryResult()
  }
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
        this.queryId = data.queryId
        if (data.isException) {
          this.errinfo = data.exceptionMessage
          this.$emit('changeView', this.extraoption.index, data, 'el-icon-ksd-error_01', 'querypanel')
        } else {
          this.$emit('changeView', this.extraoption.index, data)
        }
      })
    }, (res) => {
      handleError(res, (data, code, status, msg) => {
        this.errinfo = msg || 'kylinLang.common.timeOut'
        this.$emit('changeView', this.extraoption.index, data, 'el-icon-ksd-error_01', 'querypanel')
      })
    })
  }
  openSaveQueryDialog () {
    this.saveQueryFormVisible = true
  }
  closeModal () {
    this.saveQueryFormVisible = false
  }
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
  }
  destoryed () {
    clearInterval(this.ST)
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #queryPanelBox {
    .el-progress{
      margin-bottom: 10px;
    }
    .error-block {
      height: 200px;
      overflow-y: scroll;
      background-color: @fff;
      border: 1px solid @grey-2;
      padding: 10px;
      margin-top: 6px;
    }
  }
</style>
