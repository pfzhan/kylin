<template>
  <div class="tree-list">
    <el-input
      class="filter-box"
      size="medium"
      prefix-icon="el-icon-search"
      v-if="isShowFilter && data.length"
      v-model="filterText"
      :placeholder="placeholder">
    </el-input>
    <el-tree
      ref="tree"
      class="filter-tree"
      node-key="id"
      :show-checkbox="isShowCheckbox"
      :empty-text="emptyText"
      :data="data"
      :props="props"
      :render-content="renderNode"
      :expand-on-click-node="isExpandOnClickNode"
      :default-expand-all="isExpandAll"
      :default-expanded-keys="defaultExpandedKeys"
      :filter-node-method="nodeFilter"
      @check-change="onNodeClick"
      @node-click="onNodeClick">
    </el-tree>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { isIE } from '../../../util'

@Component({
  props: {
    data: {
      type: Array,
      default: () => []
    },
    defaultExpandedKeys: {
      type: Array,
      default: () => []
    },
    draggableNodeKeys: {
      type: Array,
      default: () => []
    },
    searchableNodeKeys: {
      type: Array,
      default: () => []
    },
    isGroupTrees: {
      type: Boolean,
      default: false
    },
    isShowFilter: {
      type: Boolean,
      default: false
    },
    isShowCheckbox: {
      type: Boolean,
      default: false
    },
    isExpandOnClickNode: {
      type: Boolean,
      default: true
    },
    isExpandAll: {
      type: Boolean,
      default: false
    },
    emptyText: {
      type: String,
      default: ''
    },
    placeholder: {
      type: String,
      default: ''
    },
    props: {
      type: Object,
      default: () => ({
        children: 'children',
        label: 'label'
      })
    }
  }
})
export default class TreeList extends Vue {
  filterText = ''

  @Watch('filterText')
  onFilterTextChange (text) {
    this.$refs['tree'].filter(text)
  }

  renderNode (h, { node, data, store }) {
    const { render, draggable, id: nodeId } = data
    const isNodeDraggable = this.draggableNodeKeys.includes(nodeId) || draggable

    return (
      <div class="tree-item" draggable={isNodeDraggable} onMousedown={event => this.handleMouseDown(event)} onDragstart={event => this.handleDragstart(event, data)}>
        { render ? (
          render(h, { node, data, store })
        ) : node.label }
      </div>
    )
  }
  handleMouseDown (event) {
    event.stopPropagation()
  }
  handleDragstart (event, data) {
    event = event || window.event
    event.dataTransfer && (event.dataTransfer.effectAllowed = 'move')
    event.stopPropagation()
    if (!isIE()) {
      event.dataTransfer && event.dataTransfer.setData && event.dataTransfer.setData('text', '')
    }
    this.$emit('drag', event.srcElement ? event.srcElement : event.target, data)
    event.stop
  }

  nodeFilter (value, data, node) {
    // 如果有配置searchableNodeKeys，则只搜索key中内容
    if (this.searchableNodeKeys.length) {
      return this.searchableNodeKeys.includes(data.id) && data.label.toUpperCase().includes(value.toUpperCase())
    } else {
      // 如果没有设置，只搜索叶子节点
      return node.isLeaf && data.label.toUpperCase().includes(value.toUpperCase())
    }
  }

  onNodeClick (data, node) {
    this.$emit('click', data, node)
  }
}
</script>

<style lang="less">
.tree-list {
  height: 100%;
  .filter-box {
    margin-bottom: 10px;
  }
  .filter-box > .el-input__inner {
    border: 1px solid #8E9FA8;
    border-radius: 2px;
  }
}
</style>
