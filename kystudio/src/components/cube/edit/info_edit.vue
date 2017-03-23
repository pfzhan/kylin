<template>
<el-form ref="info_form" label-width="200px">
  <el-form-item :label="$t('modelName')">
    <el-input v-model="cubeDesc.model_name"></el-input>
  </el-form-item>
  <el-form-item :label="$t('cubeName')">
    <el-input v-model="cubeDesc.name"></el-input>
  </el-form-item>
  <el-form-item :label="$t('notificationEmailList')">
    <el-input v-model="cubeDesc.notify_list"></el-input>
  </el-form-item>
  <el-form-item :label="$t('notificationEvents')">
  <v-select class="select" multiple :value.sync="cubeDesc.status_need_notify" :options="options" ></v-select>
  </el-form-item>
  <el-form-item :label="$t('description')">
    <el-input v-model="cubeDesc.description"></el-input>
  </el-form-item>        
</el-form>
</template>
<script>
import vSelect from 'vue-select'
export default {
  name: 'info',
  props: ['cubeDesc'],
  data () {
    return {
      options: ['ERROR', 'DISCARDED', 'SUCCEED'],
      selected_project: localStorage.getItem('selected_project')
    }
  },
  components: {
    vSelect
  },
  created () {
    console.log('this.cubeDesc')
  },
  locales: {
    'en': {modelName: 'Model Name : ', cubeName: 'Cube Name : ', notificationEmailList: 'Notification Email List : ', notificationEvents: 'Notification Events : ', description: 'Description : '},
    'zh-cn': {modelName: '模型名称 : ', cubeName: 'Cube名称 : ', notificationEmailList: '通知邮件列表 : ', notificationEvents: '需通知的事件 : ', description: '描述 : '}
  }
}
</script>
<style scoped="">
.select {
  background-color:white
}
</style>
