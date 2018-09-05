<template>
  <table class="cube-info-view ksd-table ksd-mt-20 ">
    <tr class="ksd-tr">
      <th>{{$t('modelName')}}</th>
      <td>{{cubeDesc.model}}</td>
    </tr>
    <tr class="ksd-tr">
      <th>{{$t('cubeName')}}</th>
      <td>{{cubeDesc.name}}</td>
    </tr>
    <tr class="ksd-tr">
      <th>{{$t('notificationEmailList')}}</th>
      <td>     
        <el-tag size="small" class="ksd-mr-10" v-for="address in cubeDesc.desc && cubeDesc.desc.notify_list" :key="address">{{address}}</el-tag>
      </td>
    </tr>
    <tr class="ksd-tr">
      <th>{{$t('notificationEvents')}}</th>
      <td>      
        <el-tag size="small" class="ksd-mr-10" v-for="status in cubeDesc.desc && cubeDesc.desc.status_need_notify" :key="status">{{status}}</el-tag>
      </td>
    </tr>
    <tr class="ksd-tr">
      <th>{{$t('description')}}</th>
      <td>{{cubeDesc.desc && cubeDesc.desc.description}}</td>
    </tr>
  </table>
</template>

<script>
export default {
  name: 'info',
  props: ['cubeDesc'],
  data () {
    return {
      selected_project: localStorage.getItem('selected_project')
    }
  },
  locales: {
    'en': {modelName: 'Model Name', cubeName: 'Cube Name', notificationEmailList: 'Notification Email List', notificationEvents: 'Notification Events', description: 'Description'},
    'zh-cn': {modelName: '模型名称', cubeName: 'Cube名称', notificationEmailList: '通知邮件列表', notificationEvents: '需通知的事件', description: '描述'}
  }
}
</script>
<style lang="less">
  .cube-info-view {
    tr th {
      width: 168px;
    }
  }
</style>
