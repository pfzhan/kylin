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
  methods: {
    getModels () {
      // this.currentPage = 1
      // var params = {pageSize: pageCount, pageOffset: currentPage - 1}
      // if (this.project) {
      //   params.projectName = this.project
      //   params1.project = this.project
      // }
      // if (this.project) {
      //   params.projectName = this.project
      // }
      // this.loadModels(params).then(() => {
      //   this.loadModelDiagnoseList(params1)
      // })
    }
  },
  created () {
    this.getModels()
  },
  locales: {
    'en': {cubeName: 'Cube Name'},
    'zh-cn': {cubeName: 'Cube 名称'}
  }
}
</script>
<style scoped="">
</style>
