<template>
<el-card class="box-card">
  <el-row>
    <el-col :span="8">{{$t('modelName')}}</el-col>
    <el-col :span="16">{{desc.model_name}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="8">{{$t('cubeName')}}</el-col>
    <el-col :span="16">{{desc.name}}</el-col>
  </el-row>
  <el-row>
    <el-col :span="8">{{$t('description')}}</el-col>
    <el-col :span="16">{{desc.description}}</el-col>
  </el-row>        
</el-card>
</template>

<script>
export default {
  name: 'overview',
  props: ['desc'],
  data () {
    return {
      selected_project: localStorage.getItem('selected_project')
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
