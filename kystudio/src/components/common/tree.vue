<!-- 树 -->
<template>
  <div class="tree_box">
  <el-input
    :placeholder="placeholder"
    v-model="filterText" v-if="showfilter">
  </el-input>
  <el-tree
    :show-checkbox="showCheckbox"
    class="filter-tree"
    :indent="indent"
    :data="treedata"
    :props="defaultProps"
    :render-content="renderContent"
    :default-expand-all="expandall"
    :filter-node-method="filterNode"
    @check-change="nodeClick"
    @node-click='nodeClick'
    :load="loadNode"
    :lazy="lazy"
    ref="tree2">
  </el-tree>
  <div class="empty_text" v-show="showNodeCount==1 || treedata&&treedata[0]&&treedata[0].children.length<=0">{{emptytext||'No Data'}}</div>
  </div>
</template>
<script>
  import Vue from 'vue'
  export default {
    name: 'tree',
    watch: {
      filterText (val) {
        this.showNodeCount = 0
        this.$refs.tree2.filter(val)
      }
    },
    props: ['treedata', 'renderTree', 'placeholder', 'showfilter', 'allowdrag', 'showCheckbox', 'lazy', 'expandall', 'maxlevel', 'maxLabelLen', 'titleLabel', 'emptytext', 'indent'],
    methods: {
      filterNode (value, data) {
        if (!value) {
          this.showNodeCount++
          return true
        }
        var titleLabel = this.titleLabel || this.treedata && this.treedata[0] && this.treedata[0].label
        if (data.label === titleLabel) {
          this.showNodeCount ++
          return true
        }
        if (data.label.toUpperCase().indexOf(value.toUpperCase()) !== -1) {
          this.showNodeCount ++
          return true
        }
      },
      nodeClick (data) {
        this.$emit('nodeclick', data)
      },
      createLeafContent (data) {
        var len = data.tags && data.tags.length || 0
        var dom = []
        for (var i = 0; i < len; i++) {
          dom.push('<span class="tag tag_' + data.tags[i] + '">' + data.tags[i] + '</span>')
        }
        return dom.join('') + Vue.filter('omit')(data.label, this.maxLabelLen || 0, '...') + (data.subLabel ? ' <span class="sublabel" title="' + data.subLabel + '">' + data.subLabel + '</span>' : '')
      },
      renderContent (h, { node, data, store }) {
        if (this.renderTree) {
          return this.renderTree(h, { node, data, store })
        }
        var _this = this
        if (node.level === +this.maxlevel) {
          node.isLeaf = true
        }
        this.nodeCount++
        return this.$createElement('div', {
          class: [{'el-tree-node__label': true, 'leaf-label': node.isLeaf && node.level !== 1}, node.icon],
          domProps: {
            innerHTML: this.createLeafContent(data)
          },
          attrs: {
            title: data.label + (data.subLabel ? '(' + data.subLabel + ')' : ''),
            draggable: _this.allowdrag,
            class: node.icon || ''
          },
          style: {
            'width': '100%',
            'padding-left': '2px',
            'position': 'relative'
          },
          on: {
            dragstart: function (event) {
              // event.preventDefault()
              event.cancelBubble = true
              event.dataTransfer && event.dataTransfer.setData('tree', data)
              _this.$emit('treedrag', event.srcElement ? event.srcElement : event.target, data)
              return false
            }
          }
        })
      },
      loadNode (node, resolve) {
        this.$emit('lazyload', node, resolve)
      }
    },
    data () {
      return {
        filterText: '',
        showNodeCount: 0,
        defaultProps: {
          children: 'children',
          label: 'label',
          icon: 'icon'
        }
      }
    },
    created () {
      var _this = this
      this.$on('filter', function (filterChar) {
        _this.$refs.tree2.filter(filterChar)
      })
    },
    update () {
      this.showNodeCount = 0
    }
  }
</script>
<style  lang="less">
.tree_box{
  input {
    width: 80%;
    margin: 0 auto;
  }
  .sublabel {
    color:#ccc;
  }
  .tag{
    border:solid 1px #ccc;
    display: inline-block;
    width: 14px;
    height: 14px;
    border-radius: 7px;
    color:#ccc;
    line-height: 14px;
    text-align: center;
    margin-right: 2px;
  }
  .empty_text{
    font-size: 14px;
    color:#d1dbe5;
    text-align: center;
    padding: 10px;
  }
  .el-tree{
    border:none;
    padding-left: 0;
    padding-top: 20px;
    background-color: #f1f2f7;
    width: 250px;
    div{
      font-weight: bold;
      &.leaf-label{
        font-size: 12px;
        font-weight: normal;
      }
      &:hover{
        text-decoration: none;

      }
    }
    &>.el-tree-node{
      &>.el-tree-node__content{
        background-color: #e5e9f2;
        border:solid 1px #c0ccda;
        border-left: none;
        border-right: none;

      }
    }
  }
}
</style>
