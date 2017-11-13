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
     <!--  <el-table-column
        :label="$t('kylinLang.common.action')">
        <template scope="scope">
          <span @click="gottoCube" style="cursor:pointer">{{$t('kylinLang.common.detail')}}</span>
        </template>      
      </el-table-column> -->
    </el-table>
    <pager class="ksd-center" :perPageSize="pageSize" :totalSize="renderCubeList.length" :currentPage='currentPage' v-on:handleCurrentChange='pageCurrentChange' ></pager>
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
      cubeItem: [],
      renderCubeList: []
    }
  },
  created () {
    this.renderCubeList = this.cubeList.filter((cu) => {
      return cu.type === 'CUBE'
    })
    this.cubeItem = this.renderCubeList.slice(0, this.pageSize)
  },
  methods: {
    gottoCube () {
      this.$router.push('/studio/cube')
    },
    pageCurrentChange (currentPage) {
      this.cubeItem = this.renderCubeList.slice(this.pageSize * (currentPage - 1), this.pageSize * currentPage)
    }
  },
  locales: {
    'en': {cubeName: 'Cube Name'},
    'zh-cn': {cubeName: 'Cube 名称'}
  }
}
</script>
<style lang="less">
  .cubeSearch{
    padding-top: 15px;
    padding-bottom: 15px;
  }
</style>
