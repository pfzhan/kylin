<template>
  <div class="project_edit">
    <el-table
      :data="convertedPropertiesItem"
      border
      style="width: 100%">
      </el-table-column>
      <el-table-column
        label="Key"
        prop="key">
      </el-table-column>
      <el-table-column
        label="Value"
        prop="value">
      </el-table-column>
    </el-table>
    <el-pagination
      background
      :page-size="pageSize"
      layout="prev, pager, next"
      :total="convertedProperties.length"
      :currentPage='currentPage'
      @current-change="pageCurrentChange"
      class="ksd-right ksd-mt-10"
      v-show="convertedProperties.length">
    </el-pagination>
    <!-- <pager class="ksd-center" :perPageSize="pageSize" :totalSize="convertedProperties.length" :currentPage='currentPage' v-on:handleCurrentChange='pageCurrentChange' ></pager> -->
  </div>
</template>
<script>
import { fromObjToArr } from '../../util/index'
export default {
  name: 'project_config',
  props: ['override'],
  data () {
    return {
      convertedProperties: fromObjToArr(this.override),
      pageSize: 5,
      currentPage: 1,
      convertedPropertiesItem: []
    }
  },
  created () {
    this.convertedPropertiesItem = this.convertedProperties.slice(0, this.pageSize)
  },
  methods: {
    pageCurrentChange (currentPage) {
      this.convertedPropertiesItem = this.convertedProperties.slice(this.pageSize * (currentPage - 1), this.pageSize * currentPage)
    }
  }
}
</script>
<style>

</style>
