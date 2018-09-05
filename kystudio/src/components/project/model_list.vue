<template>
  <div class="project_model_list">
    <el-table
      :data="modelItem"
      border
      style="width: 100%">
      </el-table-column>
      <el-table-column
        :label="$t('modelName')"
        prop="modelName">
      </el-table-column>
    </el-table>
    <el-pagination
      background
      layout="prev, pager, next"
      :total="modelsTotal"
      :currentPage='currentPage'
      @current-change="pageCurrentChange"
      class="ksd-right ksd-mt-10"
      v-show="modelsTotal">
    </el-pagination>
    <!-- <pager class="ksd-right" :perPageSize="pageSize" :totalSize="modelsTotal" :currentPage='currentPage' v-on:handleCurrentChange='pageCurrentChange' ></pager> -->
  </div>
</template>
<script>

export default {
  name: 'modelList',
  props: ['modelList'],
  data () {
    return {
      selected_project: localStorage.getItem('selected_project'),
      pageSize: 5,
      currentPage: 1,
      modelItem: []
    }
  },
  computed: {
    modellist () {
      let arr = []
      for (let i = 0; i < this.modelList.length; i++) {
        let obj = {}
        obj.modelName = this.modelList[i]
        arr.push(obj)
      }
      return arr
    },
    modelsTotal () {
      return this.modellist.length
    }
  },
  created () {
    this.modelItem = this.modellist.slice(0, this.pageSize)
  },
  methods: {
    pageCurrentChange (currentPage) {
      this.modelItem = this.modellist.slice(this.pageSize * (currentPage - 1), this.pageSize * currentPage)
    },
    gottoModel () {
      this.$router.push('/studio/model')
    }
  },
  locales: {
    'en': {modelName: 'Model Name'},
    'zh-cn': {modelName: 'Model 名称'}
  }
}
</script>
<style lang="less">
</style>
