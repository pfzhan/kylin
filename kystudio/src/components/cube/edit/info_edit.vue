<template>
<div class="info-edit">
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
        <common-tip :content="$t('kylinLang.cube.noticeTip')" ><icon name="exclamation-circle"></icon></common-tip>
      </span>
      <area_label  :labels="options" :placeholder="$t('kylinLang.common.pleaseSelect')" :selectedlabels="cubeDesc.status_need_notify" :datamap="{label: 'label', value: 'value'}"> 
      </area_label>
    </el-form-item>
    <div class="line-primary" style="margin-left: -30px;margin-right: -30px;"></div>
  </el-form>
  <h2 class="title">{{$t('optimizerInput')}}
        <common-tip :content="$t('kylinLang.cube.optimizerInputTip')" ><icon name="question-circle-o"></icon></common-tip></h2>
  <ul class="list">
    <li>{{$t('modelCheck')}}
      <common-tip :content="healthStatus.messages.join('<br/>')" ><icon v-if="healthStatus.status!=='RUNNING'" :name="modelHealthStatus[healthStatus.status].icon" :style="{color:modelHealthStatus[healthStatus.status].color}"></icon></common-tip>
      <el-progress  :width="20" type="circle" :stroke-width="2" :show-text="false" v-if="healthStatus.status==='RUNNING'" :percentage="healthStatus.progress||0" style="width:20px;vertical-align: baseline;"></el-progress>
    </li>
    <li>{{$t('sqlPattens')}} <span v-show="sqlCount>0">({{sqlCount}})</span></li>
  </ul>
  <el-row style="margin-top: 10px;">
    <el-col :span="24">
      <el-button type="blue"  @click.native="collectSql" :disabled="isReadyCube" >{{$t('collectsqlPatterns')}}</el-button> 
    </el-col>
  </el-row>
  <div class="line" style="margin-left: -30px;margin-right: -30px;margin-top: 105px;"></div>
  <el-dialog :title="$t('collectsqlPatterns')" v-model="addSQLFormVisible">
    <p>{{$t('kylinLang.cube.inputSqlTip1')}}</p>
    <p>{{$t('kylinLang.cube.inputSqlTip2')}}</p>
    <editor v-model="sqlString"  theme="chrome" class="ksd-mt-20" width="95%" height="200" ></editor>
    <span slot="footer" class="dialog-footer">
      <el-button @click="addSQLFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" :loading="sqlBtnLoading" @click="collectSqlToServer">{{$t('kylinLang.common.ok')}}</el-button>
    </span>     
  </el-dialog> 
</div>
</template>
<script>
import areaLabel from '../../common/area_label'
import { mapActions } from 'vuex'
import { modelHealthStatus } from '../../../config/index'
import {handleSuccess, handleError} from 'util/business'
export default {
  name: 'info',
  props: ['cubeDesc', 'modelDesc', 'isEdit'],
  data () {
    return {
      modelHealthStatus: modelHealthStatus,
      sqlBtnLoading: false,
      sqlCount: 0,
      sqlString: '',
      healthStatus: {
        status: 'NONE',
        progress: 0,
        messages: []
      },
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
      getModelDiagnose: 'DIAGNOSE'
    }),
    changeNotifyList: function () {
      this.cubeDesc.notify_list = this.getNotifyList.split(',')
    },
    validate: function (rule, value, callback) {
      if (!(/^\w+$/).test(this.newUser.password)) {
        callback(new Error(this.$t('tip_password_unsafe')))
      } else {
        callback()
      }
    },
    collectSql () {
      this.addSQLFormVisible = true
      this.loadSql()
    },
    loadSql () {
      this.getSql(this.cubeDesc.name).then((res) => {
        handleSuccess(res, (data) => {
          this.sqlString = data.join(';')
          this.sqlCount = data.length
        })
      })
    },
    collectSqlToServer () {
      // if (this.sqlString !== '') {
      this.sqlBtnLoading = true
      var sqls = this.sqlString.split(/;/)
      if (!sqls[sqls.length - 1]) {
        sqls.splice(sqls.length - 1, 1)
      }
      this.saveSampleSql({modelName: this.modelDesc.name, cubeName: this.cubeDesc.name, sqls: sqls}).then((res) => {
        this.sqlBtnLoading = false
        handleSuccess(res, (data, code, status, msg) => {
          this.addSQLFormVisible = false
          this.sqlCount = sqls.length
        })
      }, (res) => {
        this.sqlBtnLoading = false
        handleError(res)
      })
      // }
    },
    getModelHelthInfo (project, modelName) {
      this.getModelDiagnose({
        project: project,
        modelName: modelName
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.healthStatus.status = data.heathStatus
          this.healthStatus.progress = data.progress
          this.healthStatus.messages = data.messages && data.messages.length ? data.messages.map((x) => {
            return x.replace(/\r\n/g, '<br/>')
          }) : [modelHealthStatus[data.heathStatus].message]
        })
      })
    }
  },
  components: {
    'area_label': areaLabel
  },
  computed: {
    isReadyCube () {
      return this.cubeDesc.status === 'READY'
    }
  },
  mounted () {
    this.loadSql()
    // this.getModelDiagnose({
    //   project: this.modelDesc.project,
    //   modelName: this.modelDesc.name
    // }).then((res) => {
    //   handleSuccess(res, (data) => {
    //     this.healthStatus.status = data.heathStatus
    //     this.healthStatus.progress = data.progress
    //     this.healthStatus.messages = data.messages && data.messages.length ? data.messages.map((x) => {
    //       return x.replace(/\r\n/g, '<br/>')
    //     }) : [modelHealthStatus[data.heathStatus].message]
    //   })
    // })
  },
  locales: {
    'en': {modelName: 'Model Name : ', cubeName: 'Cube Name : ', notificationEmailList: 'Notification Email List : ', notificationEvents: 'Notification Events : ', description: 'Description : ', cubeNameInvalid: 'Cube name is invalid. ', cubeNameRequired: 'Cube name is required. ', basicInfo: 'Basic Info', collectsqlPatterns: 'Collect SQL Patterns', noticeSetting: 'Notification Setting', optimizerInput: 'Optimizer Inputs', modelCheck: '1.Model Check', sqlPattens: '2.SQL Pattens'},
    'zh-cn': {modelName: '模型名称 : ', cubeName: 'Cube名称 : ', notificationEmailList: '通知邮件列表 : ', notificationEvents: '需通知的事件 : ', description: '描述 : ', cubeNameInvalid: 'Cube名称不合法. ', cubeNameRequired: 'Cube名称不可为空.', basicInfo: '基本信息', collectsqlPatterns: '输入SQL', noticeSetting: '通知设置', optimizerInput: '优化器输入', modelCheck: '1.模型检测', sqlPattens: '2. SQL查询记录'}
  }
}
</script>
<style lang="less">
  @import '../../../less/config.less';
  .select {
    background-color:white;
  }
  .info-edit{
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
