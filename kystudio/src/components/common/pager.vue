<template>
	<div class="block">
	  <el-pagination
	    layout="prev, pager, next"
	    :page-size="pageSize"
	    :total="totalSize"
	    :current-page="currentPage"
	    @current-change="handleCurrentChange"
	    @size-change="handleSizeChange">
	  </el-pagination>
	</div>
</template>
<script>
export default {
  name: 'pager',
  props: ['pageSize', 'totalSize', 'currentPage'],
  methods: {
    handleCurrentChange (value, data) {
      this.$emit('handleCurrentChange', data)
    },
    handleSizeChange (value, data) {
      this.$emit('handleSizeChange', data)
    }
  }
}
</script>
<style scoped="">
.box-card {
  width: 90%
}
</style>
