<template>
<div class="info-edit">
  <el-form ref="info_form" label-width="200px" :model="cubeDesc" :rules="rules">
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
    <h2 class="title">Notification Setting</h2>
    <el-form-item :label="$t('notificationEmailList')">
      <el-input v-model="getNotifyList" placeholder="Comma Separated" @change="changeNotifyList"></el-input>
    </el-form-item>
    <el-form-item :label="$t('notificationEvents')">
      <area_label  :labels="options" :placeholder="$t('kylinLang.common.pleaseSelect')" :selectedlabels="cubeDesc.status_need_notify" :datamap="{label: 'label', value: 'value'}"> 
      </area_label>
    </el-form-item>
    <div class="line-primary" style="margin-left: -30px;margin-right: -30px;"></div>
  </el-form>
  <h2 class="title">Optimizer Inputs</h2>
  <ul class="list">
    <li>1. Model Check</li>
    <li>2. SQL Pattens</li>
  </ul>
  <el-row style="margin-top: 10px;">
    <el-col :span="24">
      <el-button type="blue"  @click.native="collectSql" :disabled="isReadyCube" >{{$t('collectsqlPatterns')}}</el-button>
    </el-col>
  </el-row>
  <div class="line" style="margin-left: -30px;margin-right: -30px;margin-top: 105px;"></div>
  <el-dialog :title="$t('collectsqlPatterns')" v-model="addSQLFormVisible">
    <editor v-model="sqlString"  theme="chrome" class="ksd-mt-20" width="100%" height="400" ></editor>
    <span slot="footer" class="dialog-footer">
      <el-button @click="addSQLFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" :loading="sqlBtnLoading" @click="collectSqlToServer">{{$t('yes')}}</el-button>
    </span>     
  </el-dialog> 
</div>
</template>
<script>
import areaLabel from '../../common/area_label'
import { mapActions } from 'vuex'
import {handleSuccess, handleError} from 'util/business'
export default {
  name: 'info',
  props: ['cubeDesc', 'modelDesc', 'isEdit'],
  data () {
    return {
      sqlBtnLoading: false,
      sqlString: '',
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
      saveSampleSql: 'SAVE_SAMPLE_SQL'
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
      this.sqlString = ''
      this.addSQLFormVisible = true
    },
    collectSqlToServer () {
      if (this.sqlString !== '') {
        this.sqlBtnLoading = true
        this.saveSampleSql({modelName: this.modelDesc.name, cubeName: this.cubeDesc.name, sqls: this.sqlString.split(/;/)}).then((res) => {
          this.sqlBtnLoading = false
          handleSuccess(res, (data, code, status, msg) => {
            this.addSQLFormVisible = false
          })
        }, (res) => {
          this.sqlBtnLoading = false
          handleError(res)
        })
      }
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
  locales: {
    'en': {modelName: 'Model Name : ', cubeName: 'Cube Name : ', notificationEmailList: 'Notification Email List : ', notificationEvents: 'Notification Events : ', description: 'Description : ', cubeNameInvalid: 'Cube name is invalid. ', cubeNameRequired: 'Cube name is required. ', basicInfo: 'Basic Info', collectsqlPatterns: 'Collect SQL Patterns'},
    'zh-cn': {modelName: '模型名称 : ', cubeName: 'Cube名称 : ', notificationEmailList: '通知邮件列表 : ', notificationEvents: '需通知的事件 : ', description: '描述 : ', cubeNameInvalid: 'Cube名称不合法. ', cubeNameRequired: 'Cube名称不可为空.', basicInfo: '基本信息', collectsqlPatterns: '输入sql'}
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
  }
</style>
