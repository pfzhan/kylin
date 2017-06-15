<template>
<el-form ref="info_form" label-width="200px" :model="cubeDesc" :rules="rules">
  <el-form-item :label="$t('modelName')">
    {{modelDesc.name}}
  </el-form-item>
  <el-form-item :label="$t('cubeName')" prop="name">
    <el-input v-model="cubeDesc.name" :disabled="isEdit"></el-input>
  </el-form-item>
  <el-form-item :label="$t('notificationEmailList')">
    <el-input v-model="getNotifyList" placeholder="Comma Separated" @change="changeNotifyList"></el-input>
  </el-form-item>
  <el-form-item :label="$t('notificationEvents')">
    <area_label  :labels="options" :placeholder="$t('kylinLang.common.pleaseSelect')" :selectedlabels="cubeDesc.status_need_notify" :datamap="{label: 'label', value: 'value'}"> 
    </area_label>
  </el-form-item>
  <el-form-item :label="$t('description')">
    <el-input v-model="cubeDesc.description"></el-input>
  </el-form-item>        
</el-form>
</template>
<script>
import areaLabel from '../../common/area_label'
export default {
  name: 'info',
  props: ['cubeDesc', 'modelDesc', 'isEdit'],
  data () {
    return {
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
    changeNotifyList: function () {
      this.cubeDesc.notify_list = this.getNotifyList.split(',')
    },
    validate: function (rule, value, callback) {
      if (!(/^\w+$/).test(this.newUser.password)) {
        callback(new Error(this.$t('tip_password_unsafe')))
      } else {
        callback()
      }
    }
  },
  components: {
    'area_label': areaLabel
  },
  locales: {
    'en': {modelName: 'Model Name : ', cubeName: 'Cube Name : ', notificationEmailList: 'Notification Email List : ', notificationEvents: 'Notification Events : ', description: 'Description : ', cubeNameInvalid: 'Cube name is invalid. ', cubeNameRequired: 'Cube name is required. '},
    'zh-cn': {modelName: '模型名称 : ', cubeName: 'Cube名称 : ', notificationEmailList: '通知邮件列表 : ', notificationEvents: '需通知的事件 : ', description: '描述 : ', cubeNameInvalid: 'Cube名称不合法. ', cubeNameRequired: 'Cube名称不可为空.'}
  }
}
</script>
<style scoped="">
.select {
  background-color:white
}
</style>
