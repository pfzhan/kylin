<template>
  <el-table
    :data="desc.dimensions"
    style="width: 100%">
    <el-table-column
      type="index">
    </el-table-column>    
    <el-table-column
      property="name"
      :label="$t('name')">
    </el-table-column>
    <el-table-column
      :label="$t('type')">
      <template scope="scope">
        <el-tag type="primary" v-if='scope.row.derived'>normal</el-tag>
        <el-tag type="primary" v-else>derived</el-tag>
      </template>      
    </el-table-column>    
    <el-table-column
      property="table"
      :label="$t('tableAlias')">
    </el-table-column>
    <el-table-column
      :label="$t('column')">
      <template scope="scope">
        <span v-if='scope.row.derived'>{{scope.row.derived}}</span>
        <span v-else>{{scope.row.column}}</span>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('datatype')">
    </el-table-column>
    <el-table-column
      :label="$t('cardinality')">
    </el-table-column>
    <el-table-column
      :label="$t('comment')">
    </el-table-column>            
  </el-table>
</template>
<script>
export default {
  name: 'dimensions',
  props: ['desc'],
  data () {
    return {
      selected_project: localStorage.getItem('selected_project')
    }
  },
  locales: {
    'en': {name: 'Name', type: 'Type', column: 'Column', datatype: 'Data Type', cardinality: 'Cardinality', comment: 'Comment'},
    'zh-cn': {name: '名称', type: '类型', column: '列名', datatype: '数据类型', cardinality: '基数', comment: '注释'}
  }
}
</script>
<style scoped="">

</style>
