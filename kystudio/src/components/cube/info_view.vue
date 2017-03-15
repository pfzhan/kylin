<template>
<el-card class="box-card">
  <el-row>
    <el-col :span="8">{{$t('modelName')}}</el-col>
    <el-col :span="16">{{info.model_name}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="8">{{$t('cubeName')}}</el-col>
    <el-col :span="16">{{info.name}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="8">{{$t('notificationEmailList')}}</el-col>
    <el-col :span="16">{{info.notify_list}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="8">{{$t('notificationEvents')}}</el-col>
    <el-col :span="16">{{info.status_need_notify}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="8">{{$t('description')}}</el-col>
    <el-col :span="16">{{info.description}}</el-col>
  </el-row>        
</el-card>
</template>

<script>
export default {
  name: 'cubedesc',
  props: ['info'],
  data () {
    return {
      activeStep: 1,
      selected_project: localStorage.getItem('selected_project')
    }
  },
  methods: {
    step: function (num) {
      console.log('11')
    }
  },
  locales: {
    'en': {modelName: 'Model Name : ', cubeName: 'Cube Name : ', notificationEmailList: 'Notification Email List : ', notificationEvents: 'Notification Events : ', description: 'Description : '},
    'zh-cn': {modelName: '模型名称 : ', cubeName: 'Cube名称 : ', notificationEmailList: '通知邮件列表 : ', notificationEvents: '需通知的事件 : ', description: '描述 : '}
  }
}
</script>
<style scoped="">

</style>
