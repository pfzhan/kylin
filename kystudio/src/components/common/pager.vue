<template>
	<div class="block ksd-mt-20 ksd-mb-40 ksd-center" v-show="totalSize">
	  <el-pagination
	    layout="total, prev, pager, next, jumper"
	    :page-size="pageSize"
	    :total="totalSize"
	    :current-page="currentPage"
	    @current-change="currentChange">
	  </el-pagination>
	</div>
</template>
<script>
import { pageCount } from '../../config'
export default {
  name: 'pager',
  props: ['perPageSize', 'totalSize', 'curPage'],
  data () {
    return {
      pageSize: this.perPageSize || pageCount,
      currentPage: this.curPage || 1
    }
  },
  methods: {
    currentChange (value) {
      // this.currentPage = value
      this.$emit('handleCurrentChange', value)
    }
  }
}
</script>
<style scoped="">
.box-card {
  width: 90%
}
</style>
