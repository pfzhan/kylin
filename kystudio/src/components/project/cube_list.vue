<template>
  <el-table
    :data="cubeList"
    style="width: 100%">
    </el-table-column>
    <el-table-column
      :label="$t('cubeName')"
      prop="realization">
    </el-table-column>
    <el-table-column
      :label="$t('kylinLang.common.action')">
      <template scope="scope">
        detail
      </template>      
    </el-table-column>
  </el-table>
</template>

<script>
export default {
  name: 'cubelist',
  props: ['cubeList'],
  data () {
    return {
      selected_project: localStorage.getItem('selected_project')
    }
  },
  locales: {
    'en': {cubeName: 'Cube Name'},
    'zh-cn': {cubeName: 'Cube 名称'}
  }
}
</script>
<style scoped="">
</style>
