<template>
	<div class="block ksd-mt-20 ksd-mb-40 ksd-center pager" v-show="totalSize">
    <span class="total_size" v-show="totalSum">{{$t('kylinLang.common.totalSize')}} {{totalSum}}</span>
	  <el-pagination
      :background="hasBackground"
	    :layout="layout || 'total, prev, pager, next, jumper'"
	    :page-size="pageSize"
	    :total="totalSize"
	    :current-page="currentPage"
	    @current-change="currentChange">
	  </el-pagination>
	</div>
</template>
<script>
import { bigPageCount } from '../../config'
export default {
  name: 'pager',
  props: ['perPageSize', 'totalSize', 'curPage', 'totalSum', 'noBackground', 'layout'],
  data () {
    return {
      pageSize: this.perPageSize || bigPageCount,
      currentPage: this.curPage || 1,
      hasBackground: !(this.noBackground || false)
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
<style lang="less">
  @import '../../assets/styles/variables.less';
.pager {
  font-size: 13px;
  .el-pagination {
    display: inline-block;
  }
  .total_size {
    color: #fff;
    display: inline-block;
    font-size: 13px;
    min-width: 28px;
    padding: 2px 5px 2px 5px;
    height: 32px;
    line-height: 28px;
    vertical-align: top;
    box-sizing: border-box;
  }
}
</style>
