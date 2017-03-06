<!-- 树 -->
<template>
  <div>
  <el-input
    placeholder="输入关键字进行过滤"
    v-model="filterText" v-if="showfilter">
  </el-input>
  <el-tree
    class="filter-tree"
    :data="treedata"
    :props="defaultProps"
    default-expand-all
    :filter-node-method="filterNode"
    @node-click='nodeClick'
    ref="tree2">
  </el-tree>
  </div>
</template>
<script>
  export default {
    name: 'tree',
    watch: {
      filterText (val) {
        this.$refs.tree2.filter(val)
      }
    },
    props: ['treedata', 'showfilter', 'nodeclick'],
    methods: {
      filterNode (value, data) {
        if (!value) return true
        return data.label.indexOf(value) !== -1
      },
      nodeClick (data) {
        this.$emit('nodeclick', data)
      }
    },
    data () {
      return {
        filterText: '',
        data2: [],
        defaultProps: {
          children: 'children',
          label: 'label'
        }
      }
    }
  }
</script>
<style scoped="">
  .el-tree{
    border:none;
  }
</style>
