<template>
  <div class="query_panel_box">
   <el-row  class="resultTips"  v-show="errinfo" >
      <el-col :span="3"><div class="grid-content bg-purple"><p>{{$t('kylinLang.query.status')}}<span style="color:red"> error</span></p></div></el-col>
      <el-col :span="6"><div class="grid-content bg-purple"><p>{{$t('kylinLang.query.startTime')}}<span> {{startTime|timeFormatHasTimeZone}}</span></p></div></el-col>
      <el-col :span="3"><div class="grid-content bg-purple"><p style="visibility:hidden">{{$t('kylinLang.query.duration')}}<span> </span></p></div></el-col>
      <el-col :span="7"><div class="grid-content bg-purple"><p>{{$t('kylinLang.query.project')}}<span> {{extraoption.project}}</span></p></div></el-col>
      <el-col :span="5"><div class="grid-content bg-purple" style="text-align:right" >
      <kap-icon-button size="small" icon="refresh" type="blue" @click.native="refreshQuery" style="display:inline-block"></kap-icon-button>
      <kap-icon-button size="small"  icon="save" type="primary" @click.native="openSaveQueryDialog" style="display:inline-block">{{$t('kylinLang.query.saveQuery')}}</kap-icon-button>
      </div></el-col>
    </el-row>
    <!-- <p class="tips">querying <span>{{pending}}</span>S in Cube: <span>aire_line</span></p> -->
    <div v-show="!errinfo">
      <el-progress type="circle" :percentage="percent"></el-progress>
      <!-- <div class="panel_btn"><icon name="close"></icon> cancel</div> -->
    </div>
	  <div v-show="errinfo" class="errorBox">
      <i class="el-icon-circle-cross"></i>
      <p class = "errorText">{{$t(''+errinfo)}}</p>
    </div>
    <el-dialog :title="$t('kylinLang.common.save')" v-model="saveQueryFormVisible">
    <el-form :model="saveQueryMeta"  ref="saveQueryForm" :rules="rules" label-width="100px">
      <el-form-item :label="$t('kylinLang.query.querySql')" prop="sql">
        <kap_editor height="200" lang="sql" theme="chrome" v-model="saveQueryMeta.sql" dragbar="#393e53">
        </kap_editor>
      </el-form-item>
      <el-form-item :label="$t('kylinLang.query.name')" prop="name">
        <el-input v-model="saveQueryMeta.name" auto-complete="off"></el-input>
      </el-form-item>
      <el-form-item :label="$t('kylinLang.query.desc')" prop="description">
        <el-input v-model="saveQueryMeta.description"></el-input>
      </el-form-item>
    </el-form>
     <div slot="footer" class="dialog-footer">
    <el-button @click="saveQueryFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
    <el-button type="primary" @click="saveQuery">{{$t('kylinLang.common.ok')}}</el-button>
  </div>
    </el-dialog>
  </div>

</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError } from '../../util/business'
export default {
  name: 'queryPanel',
  props: ['extraoption'],
  methods: {
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
          this.$emit('changeView', this.extraoption.index, data, 'warning', 'querypanel')
        })
      })
    },
    openSaveQueryDialog () {
      this.saveQueryFormVisible = true
      this.saveQueryMeta.name = ''
      this.saveQueryMeta.description = ''
    },
    saveQuery () {
      this.$refs['saveQueryForm'].validate((valid) => {
        if (valid) {
          this.saveQueryToServer(this.saveQueryMeta).then((response) => {
            this.$message(this.$t('kylinLang.common.saveSuccess'))
            this.saveQueryFormVisible = false
            this.$emit('reloadSavedProject', 0)
          }, (res) => {
            handleError(res)
            this.saveQueryFormVisible = false
          })
        }
      })
    }
  },
  data () {
    return {
      errinfo: '',
      percent: 0,
      ST: null,
      pending: 0,
      startTime: Date.now(),
      saveQueryFormVisible: false,
      rules: {
        name: [
          { required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'blur' }
        ]
      },
      saveQueryMeta: {
        name: '',
        description: '',
        project: this.extraoption.project,
        sql: this.extraoption.sql
      }
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
  .query_panel_box{
    text-align: center;
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
      color:red;
      font-size: 12px;
      i{
        font-size: 14px;
        margin-bottom: 20px;
        &.el-icon-circle-close {
          color:red
        }
      }
    }
  }
</style>
