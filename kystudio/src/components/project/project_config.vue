<template>
  <div class="project_edit">
    <el-table
      :data="convertedPropertiesItem"
      style="width: 100%">
      </el-table-column>
      <el-table-column
        label="key"
        prop="key">
      </el-table-column>
      <el-table-column
        label="value"
        prop="value">
      </el-table-column>
    </el-table>
    <pager class="ksd-center" :perPageSize="pageSize" :totalSize="convertedProperties.length" :currentPage='currentPage' v-on:handleCurrentChange='pageCurrentChange' ></pager>
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
      currentPage: 1
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
