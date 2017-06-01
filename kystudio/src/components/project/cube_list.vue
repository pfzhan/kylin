<template>
  <div>
    <el-table
      :data="cubeItem"
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
    <pager class="ksd-center" :perPageSize="pageSize" :totalSize="cubeList.length" :currentPage='currentPage' v-on:handleCurrentChange='pageCurrentChange' ></pager>
  </div>
</template>

<script>
export default {
  name: 'cubelist',
  props: ['cubeList'],
  data () {
    return {
      selected_project: localStorage.getItem('selected_project'),
      pageSize: 5,
      currentPage: 1,
      cubeItem: []
    }
  },
  created () {
    this.cubeItem = this.cubeList.slice(0, this.pageSize)
  },
  methods: {
    pageCurrentChange (currentPage) {
      this.cubeItem = this.cubeList.slice(this.pageSize * (currentPage - 1), this.pageSize * currentPage)
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
