<!-- 树 -->
<template>
  <div class="tree_box">
  <el-input style="background: transparent;margin-bottom: 10px;"
    :placeholder="placeholder"
    icon="search"
    v-model="filterText" v-if="showfilter">
  </el-input>
  <el-tree
    :show-checkbox="showCheckbox"
    node-key="id"
    class="filter-tree"
    :empty-text="emptyText"
    :indent="indent"
    :data="treedata"
    :props="defaultProps"
    :render-content="renderContent"
    :expand-on-click-node="expandnodeclick"
    :default-expand-all="expandAll"
    :filter-node-method="filterNode"
    :default-expanded-keys="expandKeys"
    @check-change="nodeClick"
    @node-click='nodeClick'
    :load="loadNode"
    :lazy="lazy"
    ref="tree2">
  </el-tree>
  <!-- <div class="empty_text" v-show="showNodeCount==1 || treedata&&treedata[0]&&treedata[0].children.length<=0">{{emptytext||$t('kylinLang.common.noData')}}</div> -->
  </div>
</template>
<script>
  import Vue from 'vue'
  import { isIE } from '../../util/index'
  export default {
    name: 'tree',
    watch: {
      filterText (val) {
        this.showNodeCount = 0
        this.$refs.tree2.filter(val)
      }
    },
    props: ['treedata', 'renderTree', 'placeholder', 'multiple', 'expandIdList', 'maxlevel', 'showfilter', 'allowdrag', 'showCheckbox', 'lazy', 'expandall', 'maxLabelLen', 'titleLabel', 'emptytext', 'indent', 'expandnodeclick', 'emptyText'],
    methods: {
      // 过滤点击变色
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
      toggleSelectClass (label, isShow) {
        var nodeDoms = this.$el.querySelectorAll("[title='" + label + "']")
        var nodeDom = nodeDoms && nodeDoms[0] || null
        if (nodeDom) {
          var repReg = new RegExp('\\s*?checked-leaf', 'g')
          nodeDom.className = nodeDom.className.replace(repReg, '')
          if (isShow) {
            nodeDom.className += ' checked-leaf'
          }
        }
      },
      // 节点点击事件，处理点击变色
      nodeClick (data, vnode) {
        if (!data.children || data.children.length <= 0) {
          if (this.lastCheckedNode && !this.multiple) {
            this.toggleSelectClass(this.lastCheckedNode.label, false)
            delete this.checkedNodes[this.lastCheckedNode.label]
          }
          this.toggleSelectClass(data.label, true)
          this.checkedNodes[data.label] = data
          this.lastCheckedNode = data
        }
        this.$emit('nodeclick', data, vnode)
      },
      // 取消某个节点的选中变色
      cancelNodeChecked (name) {
        for (var i in this.checkedNodes) {
          if (this.checkedNodes[i].id === name) {
            this.toggleSelectClass(this.checkedNodes[i].label, false)
            delete this.checkedNodes[i]
            return
          }
        }
      },
      // 取消所有的节点选中变色
      cancelCheckedAll () {
        for (var i in this.checkedNodes) {
          this.toggleSelectClass(this.checkedNodes[i].label, false)
        }
        this.checkedNodes = []
      },
      // 根据用户入参渲染树的节点DOM
      createLeafContent (data, store, node) {
        var len = data.tags && data.tags.length || 0
        var dom = []
        for (var i = 0; i < len; i++) {
          dom.push('<span class="tag tag_' + data.tags[i] + '">' + data.tags[i] + '</span>')
        }
        if (data.isMore) {
          data.parentNode = node.parent.data
          data.parentStore = node.parent.store
          return '<img class="loadmore_btn" title="load more" src="' + require('../../assets/img/loadmore.png') + '"/>'
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
        } else {
          node.isLeaf = false
        }
        this.nodeCount++
        return this.$createElement('div', {
          class: [{'el-tree-node__label': true, 'leaf-label': node.isLeaf && node.level !== 1, 'checked-leaf': data.checked}],
          domProps: {
            innerHTML: this.createLeafContent(data, store, node)
          },
          attrs: {
            title: data.label + (data.subLabel ? '(' + data.subLabel + ')' : ''),
            draggable: !data.children && _this.allowdrag,
            class: node.icon || ''
          },
          style: {
            'width': '100%',
            'padding-left': '2px',
            'position': 'relative',
            'cursor': _this.allowdrag ? 'move' : 'default'
          },
          on: {
            dragstart: function (event) {
              // event.preventDefault()
              event.cancelBubble = true
              if (!isIE()) {
                event.dataTransfer && event.dataTransfer.setData && event.dataTransfer.setData('tree', data)
              }
              _this.$emit('treedrag', event.srcElement ? event.srcElement : event.target, data)
              return false
            },
            click: function () {
              _this.$emit('contentClick', node)
            }
          }
        })
      },
      // 异步渲染节点
      loadNode (node, resolve) {
        this.$emit('lazyload', node, resolve)
      }
    },
    data () {
      return {
        checkedNodes: [],
        filterText: '',
        showNodeCount: 0,
        lastCheckedNode: null,
        defaultProps: {
          children: 'children',
          label: 'label',
          icon: 'icon'
        }
      }
    },
    computed: {
      expandAll () {
        return this.expandall && !(this.expandIdList && this.expandIdList.length)
      },
      expandKeys () {
        if (this.expandall || (this.expandIdList && this.expandIdList.length)) {
          return null
        } else {
          if (this.treedata && this.treedata.length) {
            return [this.treedata[0].id]
          }
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
@import '../../less/config.less';
.tree_box{
  position: relative;
  .el-input {
    width: 80%;
    position: relative;
    display: block;
    margin: 4px auto;
  }
  .el-input__inner{
    border: 0;
    border-radius: 20px;
    text-indent: 10px;
  }
  .sublabel {
    color:#9da3b3;
    font-style: italic;
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
    font-size: 12px;
    text-align: center;
    padding: 10px;
    color:#d4d7e3;
    background-color: #292b38;
  }
  .el-tree{
    border:none;
    padding-left: 0;
    padding-top: 20px;
    background-color: #f1f2f7;
    width: 250px;
    .loadmore_btn{
      font-weight: bolder;
      width: 20px;
      height: 6px;
      margin-left: 10px;
      cursor: pointer;
    }
    div{
      &.leaf-label{
        font-size: 12px;
        font-weight: normal;
      }
      &.checked-leaf.leaf-label {
        color:@base-color;
      }
      &:hover{
        text-decoration: none;

      }
    }
    .el-tree-node__label{
      font-size: 12px;
    }
    &>.el-tree-node{
      &>.el-tree-node__children{
        margin-bottom: 40px;
      }
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
