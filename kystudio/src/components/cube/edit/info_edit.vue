<template>
<div class="cube-info-edit">
  <el-form ref="info_form" label-width="150px" :model="cubeDesc" :rules="rules">
    <h2 class="title">{{$t("basicInfo")}}</h2>
    <el-form-item :label="$t('modelName')">
      {{modelDesc.name}}
    </el-form-item>
    <el-form-item :label="$t('cubeName')" prop="name">
      <el-input v-model="cubeDesc.name" :disabled="isEdit"></el-input>
    </el-form-item>
    <el-form-item :label="$t('description')">
      <el-input v-model="cubeDesc.description"></el-input>
    </el-form-item>
    <div class="line-primary" style="margin-left: -30px;margin-right: -30px;"></div>
    <h2 class="title">{{$t('noticeSetting')}}</h2>
    <el-form-item :label="$t('notificationEmailList')">
      <el-input v-model="getNotifyList" placeholder="Comma Separated" @change="changeNotifyList"></el-input>
    </el-form-item>
    <el-form-item :label="$t('notificationEvents')">
      <span slot="label">{{$t('notificationEvents')}}
        <common-tip :content="$t('kylinLang.cube.noticeTip')" ><icon name="question-circle" class="ksd-question-circle"></icon></common-tip>
      </span>
      <area_label  :labels="options" :placeholder="$t('kylinLang.common.pleaseSelect')" :datamap="{label: 'label', value: 'value'}" :selectedlabels="cubeDesc.status_need_notify" @refreshData="refreshNotificationEvents">
      </area_label>
    </el-form-item>
    <div class="line-primary" style="margin-left: -30px;margin-right: -30px;"></div>
  </el-form>
  <h2 class="title">{{$t('optimizerInput')}}
    <common-tip :content="$t('kylinLang.cube.optimizerInputTip')" >
      <icon name="question-circle" class="ksd-question-circle"></icon>
    </common-tip>
  </h2>
  <ul class="list">
    <li>
      <span style="font-size: 14px;">{{$t('modelCheck')}} </span>
      <common-tip :content="healthStatus.messages.join('<br/>')" ><icon v-if="healthStatus.status!=='RUNNING' && healthStatus.status!=='ERROR'" :name="modelHealthStatus[healthStatus.status].icon" :style="{color:modelHealthStatus[healthStatus.status].color}"></icon></common-tip>
      <common-tip v-if="healthStatus.status==='RUNNING'"  :content="healthStatus.messages.join('<br/>')" ><el-progress  :width="15" type="circle" :stroke-width="2" :show-text="false" :percentage="healthStatus.progress||0" style="width:20px;vertical-align: baseline;"></el-progress></common-tip>
      <common-tip v-if="healthStatus.status==='ERROR'" :content="healthStatus.messages.join('<br/>')" ><el-progress  :width="15" type="circle" :stroke-width="2" :show-text="false" status="exception"  :percentage="healthStatus.progress||0" style="width:20px;vertical-align: baseline;"></el-progress></common-tip>
    </li>
    <li>
      <span style="font-size: 14px;">2. </span>
      <span @click="collectSql" class="action_sql" style="font-size: 14px;">{{$t('sqlPattens')}} <i class="el-icon-edit"></i></span>
      <span v-show="sampleSql.sqlCount>0" style="font-size: 14px;"> ( {{sampleSql.sqlCount}} ) </span>
    </li>
  </ul>
  <div class="line" style="margin-left: -30px;margin-right: -30px;margin-top: 105px;"></div>
  <el-dialog :title="$t('collectsqlPatterns')" v-model="addSQLFormVisible" :before-close="sqlClose" :close-on-press-escape="false" :close-on-click-modal="false">
    <p>{{$t('kylinLang.cube.inputSqlTip1')}}</p>
    <p>{{$t('kylinLang.cube.inputSqlTip2')}}</p>
    <div :class="{hasCheck: hasCheck}">
    <editor v-model="sampleSql.sqlString" ref="sqlbox" theme="chrome"  class="ksd-mt-20" width="95%" height="200" ></editor>
    </div>
    <!-- <div class="checkSqlResult">{{errorMsg}}</div> -->
    <!-- <div> <icon v-if="result && result.length === 0" name="check" style="color:green"></icon></div> -->
    <transition name="fade">
    <div v-if="errorMsg">
     <el-alert class="ksd-mt-10 trans"
        :title="errorMsg"
        show-icon
        :closable="false"
        type="error">
      </el-alert>
    </div>
    </transition>
    <transition name="fade">
    <div v-if="successMsg">
     <el-alert class="ksd-mt-10 trans"
        :title="successMsg"
        show-icon
        :closable="false"
        type="success">
      </el-alert>
    </div>
    </transition>
    <div class="ksd-mt-4"><el-button :loading="checkSqlLoadBtn" @click="validateSql" >{{$t('kylinLang.common.check')}}</el-button> <el-button type="text" v-show="checkSqlLoadBtn" @click="cancelCheckSql">{{$t('kylinLang.common.cancel')}}</el-button></div>
    <span slot="footer" class="dialog-footer">
      <el-button @click="sqlClose()">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" :loading="sqlBtnLoading" @click="collectSqlToServer">{{$t('kylinLang.common.submit')}}</el-button>
    </span>
  </el-dialog>
</div>
</template>
<script>
import areaLabel from '../../common/area_label'
import { mapActions } from 'vuex'
import { modelHealthStatus } from '../../../config/index'
import {handleSuccess, handleError, kapConfirm, filterMutileSqlsToOneLine} from 'util/business'
export default {
  name: 'info',
  props: ['cubeDesc', 'modelDesc', 'isEdit', 'cubeInstance', 'sampleSql', 'healthStatus'],
  data () {
    return {
      modelHealthStatus: modelHealthStatus,
      sqlBtnLoading: false,
      checkSqlLoadBtn: false,
      errorMsg: '',
      successMsg: '',
      hasCheck: false,
      firstLoadSql: false,
      addSQLFormVisible: false,
      getNotifyList: this.cubeDesc.notify_list && this.cubeDesc.notify_list.toString() || '',
      options: [{label: 'ERROR', value: 'ERROR'}, {label: 'DISCARDED', value: 'DISCARDED'}, {label: 'SUCCEED', value: 'SUCCEED'}],
      selected_project: localStorage.getItem('selected_project'),
      rules: {
        name: [
        { required: true, message: '', trigger: 'change' },
        {validator: this.validate, trigger: 'blur'}
        ]
      }
    }
  },
  methods: {
    ...mapActions({
      saveSampleSql: 'SAVE_SAMPLE_SQL',
      getSql: 'GET_SAMPLE_SQL',
      checkSql: 'CHECK_SQL'
    }),
    changeNotifyList: function () {
      this.cubeDesc.notify_list = this.getNotifyList.split(',')
    },
    validate: function (rule, value, callback) {
      if (!(/^\w+$/).test(this.cubeDesc.name)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    },
    collectSql () {
      this.addSQLFormVisible = true
      this.hasCheck = false
      this.loadSql()
    },
    sqlClose () {
      kapConfirm(this.$t('kylinLang.common.willClose'), {
        confirmButtonText: this.$t('kylinLang.common.continue'),
        cancelButtonText: this.$t('kylinLang.common.cancel')
      }).then(() => {
        this.addSQLFormVisible = false
      })
    },
    filterSqls () {
      var sqls = this.sampleSql.sqlString.split(/;/)
      sqls = sqls.filter((s) => {
        return !!(s.replace(/[\r\n]/g, ''))
      })
      sqls = sqls.map((s) => {
        var r = s.replace(/[\r\n](\s+)?/g, ' ')
        return r
      })
      return sqls
    },
    addBreakPoint (data, editor) {
      this.errorMsg = ''
      this.successMsg = ''
      if (!editor) {
        return
      }
      if (data && data.length) {
        var hasFailValid = false
        data.forEach((r, index) => {
          if (r.status === 'FAILED') {
            hasFailValid = true
            editor.session.setBreakpoint(index)
          } else {
            editor.session.clearBreakpoint(index)
          }
        })
        if (hasFailValid) {
          this.errorMsg = this.$t('validFail')
        } else {
          this.successMsg = this.$t('validSuccess')
        }
      }
    },
    bindBreakClickEvent (editor) {
      if (!editor) {
        return
      }
      editor.on('guttermousedown', (e) => {
        var row = e.getDocumentPosition().row
        var data = this.sampleSql.result
        if (data && data.length) {
          if (data[row].status === 'FAILED') {
            if (data[row].message) {
              this.errorMsg = data[row].message.replace(/^.*?:/, '')
            }
          } else {
            this.errorMsg = ''
            this.successMsg = ''
          }
        }
        e.stop()
      })
    },
    renderEditerRender (editor) {
      // var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor || ''
      if (!(editor && editor.session)) {
        return
      }
      editor.session.gutterRenderer = {
        getWidth: (session, lastLineNumber, config) => {
          return lastLineNumber.toString().length * 12
        },
        getText: (session, row) => {
          return row + 1
        }
      }
    },
    validateSql () {
      var sqls = filterMutileSqlsToOneLine(this.sampleSql.sqlString)
      if (sqls.length === 0) {
        return
      }
      var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor || ''
      this.renderEditerRender(editor)
      this.errorMsg = false
      this.checkSqlLoadBtn = true
      editor.setOption('wrap', 'free')
      // this.sampleSql.sqlString = sqls.join(';\r\n')
      this.sampleSql.sqlString = sqls.length > 0 ? sqls.join(';\r\n') + ';' : ''
      this.checkSql({modelName: this.modelDesc.name, cubeName: this.cubeDesc.name, sqls: sqls}).then((res) => {
        handleSuccess(res, (data) => {
          this.hasCheck = true
          this.checkSqlLoadBtn = false
          this.sampleSql.result = data
          this.$nextTick(() => {
            this.addBreakPoint(data, editor)
            this.bindBreakClickEvent(editor)
          })
        })
      }, (res) => {
        this.checkSqlLoadBtn = false
        handleError(res)
      })
    },
    cancelCheckSql () {
      this.checkSqlLoadBtn = false
    },
    loadSql () {
      this.errorMsg = ''
      this.$nextTick(() => {
        var editor = this.$refs.sqlbox && this.$refs.sqlbox.editor || ''
        editor && editor.removeListener('change', this.editerChangeHandle)
        this.renderEditerRender(editor)
        editor.setOption('wrap', 'free')
        this.getSql(this.cubeDesc.name).then((res) => {
          handleSuccess(res, (data) => {
            if (data.sqls) {
              this.sampleSql.sqlCount = data.sqls.length
              if (data.results && data.results.length) {
                this.hasCheck = true
              }
              this.sampleSql.result = data.results
              this.sampleSql.sqlString = ''
              this.$nextTick(() => {
                this.sampleSql.sqlString = data.sqls.length > 0 ? data.sqls.join(';\r\n') + ';' : ''
                this.addBreakPoint(this.sampleSql.result, editor)
                this.bindBreakClickEvent(editor)
                this.$nextTick(() => {
                  editor && editor.on('change', this.editerChangeHandle)
                })
              })
            }
          })
        })
      })
    },
    editerChangeHandle () {
      if (!this.firstLoadSql) {
        this.sampleSql.result = []
        this.hasCheck = false
      }
      this.firstLoadSql = false
      // editor && editor.removeListener('change', this.editerChangeHandle)
    },
    saveSql () {
      this.errorMsg = ''
      this.sqlBtnLoading = true
      var sqls = filterMutileSqlsToOneLine(this.sampleSql.sqlString)
      this.saveSampleSql({modelName: this.modelDesc.name, cubeName: this.cubeDesc.name, sqls: sqls}).then((res) => {
        this.sqlBtnLoading = false
        handleSuccess(res, (data, code, status, msg) => {
          this.addSQLFormVisible = false
          this.sampleSql.sqlCount = sqls.length
        })
      }, (res) => {
        this.sqlBtnLoading = false
        handleError(res)
      })
    },
    collectSqlToServer () {
      if (this.checkSqlLoadBtn) {
        kapConfirm(this.$t('checkingTip')).then(() => {
          this.saveSql()
        })
      } else {
        this.saveSql()
      }
    },
    refreshNotificationEvents (data) {
      this.$set(this.cubeDesc, 'status_need_notify', data)
    }
  },
  components: {
    'area_label': areaLabel
  },
  computed: {
    isReadyCube () {
      return this.cubeInstance && this.cubeInstance.segments && this.cubeInstance.segments.length > 0
      // return this.cubeDesc.status === 'READY'
    }
  },
  locales: {
    'en': {modelName: 'Model Name: ', cubeName: 'Cube Name: ', notificationEmailList: 'Notification Email List: ', notificationEvents: 'Notification Events: ', description: 'Description: ', cubeNameInvalid: 'Cube name is invalid. ', cubeNameRequired: 'Cube name is required. ', basicInfo: 'Basic Info', collectsqlPatterns: 'Collect SQL Patterns', noticeSetting: 'Notification Setting', optimizerInput: 'Optimizer Inputs', modelCheck: '1. Model Check', sqlPattens: 'SQL Patterns', check: 'Validate', checkingTip: 'The SQL statements check is about to complete, are you sure to break it and save?', 'validFail': 'Some SQL statements are not checked through, click on \'x\' before the line number of SQL statements to see error details.', validSuccess: 'Great, the SQL statements are valid.'},
    'zh-cn': {modelName: '模型名称：', cubeName: 'Cube名称：', notificationEmailList: '通知邮件列表：', notificationEvents: '需通知的事件：', description: '描述：', cubeNameInvalid: 'Cube名称不合法。', cubeNameRequired: 'Cube名称不可为空。', basicInfo: '基本信息', collectsqlPatterns: '输入SQL', noticeSetting: '通知设置', optimizerInput: '优化器输入', modelCheck: '1. 模型检测', sqlPattens: 'SQL查询记录', check: '校验', checkingTip: 'SQL语句校验即将完成，您确定要现在保存？', 'validFail': 'SQL检测未通过，您可以通过点击SQL语句行号前的x号查看详细错误。', validSuccess: 'SQL校验结果正确。'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .select {
    background-color:white;
  }

  .cube-info-edit{
    .hasCheck{
      .ace_gutter-cell:before {
        content:"✓";
        text-align: center;
        font-weight: bolder;
        cursor: pointer;
        font-size: 12px;
        position: absolute;
        left: 4px;
        color:green;
        background-color: rgba(0,0,0, 0.3);
        width: 12px;
        height: 12px;
        line-height: 12px;
      }
      .ace_gutter-cell.ace_breakpoint:before{
        content:"x";
        color:red;
        display: block;

      }
      .ace_gutter-cell:hover:before{
       background-color: rgba(0,0,0, 0.5);
      }
    }
    .action_sql{
      color: @base-color;
      cursor: pointer;
      border-bottom: @base-color 1px solid;
    }
    .check-required {
      font-family:Montserrat-Regular;
      font-size:12px;
      letter-spacing:0;
      line-height:14px;
      text-align:left;
    }
    .title{
      margin-top: 30px;
      margin-bottom: 20px;
      font-size: 14px!important;
    }
    .el-form-item__label{
      color: @select;
    }
    .list{
      color: @select;
      line-height: 25px;
      font-size: 13px;
    }
    .ace_content{
      background: #20222e;
    }
    .el-form-item__content{
      // background: yellow;
      // &.el-input__inner{
      //   background: yellow!important;
      // }
    }
  }
</style>
