<template>
  <div class="kap-pager" v-show="totalSize">
    <el-pagination
      background
      :layout="pagerLayout"
      :page-size="pageSize"
      :page-sizes="pageSizes"
      :total="totalSize"
      :current-page="currentPage"
      @current-change="pageChange"
      @size-change="sizeChange"
      >
    </el-pagination>
  </div>
</template>
<script>
import { pageCount, pageSizes } from '../../config'
export default {
  name: 'pager',
  props: ['perPageSize', 'totalSize', 'curPage', 'totalSum', 'layout'],
  data () {
    return {
      pagerLayout: this.layout || 'total, sizes, prev, pager, next, jumper',
      pageSize: this.perPageSize || pageCount,
      pageSizes: pageSizes,
      currentPage: this.curPage || 1
    }
  },
  methods: {
    pageChange (value) {
      this.currentPage = value
      this.$emit('handleCurrentChange', value - 1, this.pageSize)
    },
    sizeChange (size) {
      this.pageSize = size
      this.currentPage = 0
      this.$emit('handleCurrentChange', this.currentPage, size)
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
