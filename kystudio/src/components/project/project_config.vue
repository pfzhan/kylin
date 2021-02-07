<template>
  <div class="project_edit">
    <el-table
      :data="convertedPropertiesItem"
      border
      size="medium"
      nested
      style="width: 100%">
      <el-table-column
        :label="$t('key')"
        prop="key">
      </el-table-column>
      <el-table-column
        :label="$t('value')"
        prop="value">
      </el-table-column>
    </el-table>
    <kap-pager
      class="ksd-center ksd-mt-10" ref="pager"
      :refTag="pageRefTags.projectConfigPager"
      layout="prev, pager, next"
      :background="false"
      :curPage="currentPage+1"
      :totalSize="convertedProperties.length"
      @handleCurrentChange="handleCurrentChange">
    </kap-pager>
  </div>
</template>
<script>
import { fromObjToArr } from '../../util/index'
import { pageRefTags } from 'config'
export default {
  name: 'project_config',
  props: ['override'],
  locales: {
    'en': {
      key: 'Parameter Key',
      value: 'Parameter Value'
    },
    'zh-cn': {
      key: '配置项名称',
      value: '配置项值'
    }
  },
  data () {
    return {
      pageRefTags: pageRefTags,
      convertedProperties: fromObjToArr(this.override),
      pageSize: +localStorage.getItem(pageRefTags.projectConfigPager) || 10,
      currentPage: 0,
      convertedPropertiesItem: []
    }
  },
  created () {
    this.convertedPropertiesItem = this.convertedProperties.slice(0, this.pageSize)
  },
  methods: {
    handleCurrentChange (currentPage, pageSize) {
      this.currentPage = currentPage
      this.pageSize = pageSize
      this.convertedPropertiesItem = this.convertedProperties.slice(this.pageSize * currentPage, this.pageSize * (currentPage + 1))
    }
  }
}
</script>
<style>

</style>
