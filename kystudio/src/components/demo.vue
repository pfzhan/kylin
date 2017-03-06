<template>
    <div class="hello" id="demo">
    <h1 class="demo_title">KyStudio项目组件DEMO</h1>
    <el-row :gutter="20" >
      <el-col :span="4" :offset="1" class="left-menu"><tree v-bind:treedata="treeData" v-bind:showfilter="showFilter" v-on:nodeclick="nodeClick"></tree></el-col>
      <el-col :span="19" class="right-menu"><div class="grid-content bg-purple"><router-view></router-view></div></el-col>
    </el-row>
  </div>
</template>

<script>
import router from '../router'
import tree from './common/tree.vue'
export default {
  name: 'hello',
  data () {
    return {
      showFilter: false,
      treeData: [{
        id: 1,
        label: '基础',
        children: [{
          id: 9,
          label: '按钮',
          url: 'basic'
        }
        ]
      }, {
        id: 2,
        label: '弹窗',
        children: [{
          id: 5,
          label: 'Dialog 对话框'
        }, {
          id: 6,
          label: '二级 2-2'
        }]
      }, {
        id: 3,
        label: '表单',
        children: [{
          id: 7,
          label: '二级 3-1'
        }, {
          id: 8,
          label: '二级 3-2'
        }]
      }, {
        id: 4,
        label: '表格',
        children: [{
          id: 9,
          label: '二级 3-1'
        }, {
          id: 10,
          label: '二级 3-2'
        }]
      }, {
        id: 4,
        label: '通知',
        children: [{
          id: 9,
          label: '二级 3-1'
        }, {
          id: 10,
          label: '二级 3-2'
        }]
      }, {
        id: 5,
        label: '规范',
        children: [{
          id: 9,
          label: '命名规范',
          url: 'naming'
        }, {
          id: 10,
          label: '二级 3-2'
        }]
      }]
    }
  },
  components: {
    tree
  },
  methods: {
    nodeClick: function (data) {
      if (data.url) {
        router.push({path: '/demo/' + data.url})
      }
    }
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style lang='less' scoped >
#demo{
  .demo_title {
    text-align: left;
    margin: 10px 20px;
  }
  .el-row{
    border-top: solid 1px #ccc; 
  }
  .el-tree{
    margin-left: 10px; 
  }
  .left-menu{
    border-right: solid 1px #ccc;
  }
  .right-menu{
    padding-top: 20px;
  }
}

</style>
