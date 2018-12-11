<template>
  <div class="project_edit">
    <el-table
      :data="convertedPropertiesItem"
      border
      style="width: 100%">
      <el-table-column
        label="Key"
        prop="key">
      </el-table-column>
      <el-table-column
        label="Value"
        prop="value">
      </el-table-column>
    </el-table>
    <kap-pager
      class="ksd-right ksd-mt-10" ref="pager"
      :totalSize="convertedProperties.length"
      @handleCurrentChange="handleCurrentChange">
    </kap-pager>
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
      pageSize: 10,
      currentPage: 1,
      convertedPropertiesItem: []
    }
  },
  created () {
    this.convertedPropertiesItem = this.convertedProperties.slice(0, this.pageSize)
  },
  methods: {
    handleCurrentChange (currentPage, pageSize) {
      this.pageSize = pageSize
      this.convertedPropertiesItem = this.convertedProperties.slice(this.pageSize * currentPage, this.pageSize * (currentPage + 1))
    }
  }
}
</script>
<style>

</style>
