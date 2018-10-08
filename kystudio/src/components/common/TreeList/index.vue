<template>
  <div class="tree-list" :style="{ width: `${treeStyle.width}px` }">
    <div class="filter-box">
      <el-input
        size="medium"
        prefix-icon="el-icon-search"
        v-if="isShowFilter && data.length"
        v-model="filterText"
        :placeholder="placeholder">
      </el-input>
    </div>
    <el-tree
      v-loading="isLoading"
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
      :filter-node-method="handleNodeFilter"
      @check-change="handleNodeClick"
      @node-click="handleNodeClick"
      @node-expand="handleNodeExpand">
    </el-tree>
    <div class="resize-bar" v-show="isShowResizeBar" ref="resize-bar">
      <i></i>
      <i></i>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import loadMoreImg from '../../../assets/img/loadmore.png'
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
    isShowResizeBar: {
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
    onFilter: {
      type: Function
    },
    minWidth: {
      type: Number,
      default: 218
    },
    maxWidth: {
      type: Number,
      default: 440
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
  isResizing = false
  isLoading = false
  resizeFrom = 0
  movement = 0
  treeStyle = {
    width: null
  }
  fixTreeWidth () {
    this.treeStyle.width = this.$el.clientWidth
  }
  mounted () {
    const resizeBarEl = this.$refs['resize-bar']
    this.fixTreeWidth()
    resizeBarEl.addEventListener('mousedown', this.handleResizeStart)
    document.addEventListener('mousemove', this.handleResize)
    document.addEventListener('mouseup', this.handleResizeEnd)
  }
  beforeDestroy () {
    const resizeBarEl = this.$refs['resize-bar']

    resizeBarEl.removeEventListener('mousedown', this.handleResizeStart)
    document.removeEventListener('mousemove', this.handleResize)
    document.removeEventListener('mouseup', this.handleResizeEnd)
  }
  showLoading () {
    this.isLoading = true
  }
  hideLoading () {
    this.isLoading = false
  }
  @Watch('filterText')
  async onFilterTextChange (text) {
    if (this.onFilter) {
      this.showLoading()
      await this.onFilter(text)
      this.hideLoading()
    }
    this.$refs['tree'].filter(text)
  }
  getTreeItemStyle (data, node) {
    const treeItemStyle = {}
    // 18: this.$refs['tree'].indent
    const paddingLeft = (node.level - 1) * 18

    if (data.type === 'isMore') {
      treeItemStyle.width = `calc(100% + ${paddingLeft}px)`
      treeItemStyle.transform = `translateX(${-24 - paddingLeft}px)`
      treeItemStyle.paddingLeft = `calc(${paddingLeft + 24}px)`
    }

    return treeItemStyle
  }
  renderNode (h, { node, data, store }) {
    const { render, draggable, id: nodeId, handleClick, handleDbClick, type } = data
    const isNodeDraggable = this.draggableNodeKeys.includes(nodeId) || draggable
    const treeItemStyle = this.getTreeItemStyle(data, node)
    const props = {
      directives: [
        ...(type === 'isLoading' ? [{ name: 'loading', value: true }] : [])
      ],
      class: [
        ...(type === 'isMore' ? ['load-more'] : []),
        ...(data.isSelected ? ['selected'] : [])
      ]
    }

    if (data.children) {
      this.patchNodeLoading(data)
      this.patchNodeMore(data)
    }
    return (
      <div class="tree-item"
        style={treeItemStyle}
        draggable={isNodeDraggable}
        onMousedown={event => this.handleMouseDown(event, data, node)}
        onDragstart={event => this.handleDragstart(event, data, node)}
        onClick={event => handleClick && handleClick(event, data, node)}
        onDbClick={event => handleDbClick && handleDbClick(event, data, node)}
        {...props}>
        { render ? (
          render(h, { node, data, store })
        ) : node.label }
        { type === 'isMore' ? (
          <img class="load-more-img"
            title="load more"
            src={loadMoreImg} />
        ) : null}
      </div>
    )
  }
  handleNodeClick (data, node) {
    switch (data.type) {
      case 'isMore': this.$emit('load-more', data, node); break
      default: this.$emit('click', data, node); break
    }
  }
  handleNodeExpand (data, node) {
    this.$emit('node-expand', data, node)
  }
  handleMouseDown (event, data, node) {
    event.stopPropagation()
  }
  handleResizeStart (event) {
    const isLeftKey = event.which === 1
    if (isLeftKey) {
      this.isResizing = true
      this.resizeFrom = event.pageX
    }
  }
  handleResize (event) {
    if (this.isResizing) {
      const tempWidth = this.treeStyle.width + event.pageX - this.resizeFrom

      if (tempWidth > this.minWidth && tempWidth < this.maxWidth) {
        this.movement = event.pageX - this.resizeFrom
        this.treeStyle.width = this.treeStyle.width + this.movement
        this.resizeFrom = event.pageX
        this.$emit('resize', this.treeStyle.width)
      }
    }
  }
  handleResizeEnd (event) {
    this.isResizing = false
    this.resizeFrom = 0
    this.movement = 0
  }
  handleDragstart (event, data, node) {
    event = event || window.event
    event.dataTransfer && (event.dataTransfer.effectAllowed = 'move')
    event.stopPropagation()
    if (!isIE()) {
      event.dataTransfer && event.dataTransfer.setData && event.dataTransfer.setData('text', '')
    }
    this.$emit('drag', event.srcElement ? event.srcElement : event.target, data)
  }
  handleNodeFilter (value, data, node) {
    // 如果有配置searchableNodeKeys，则只搜索key中内容
    if (this.searchableNodeKeys.length) {
      return this.searchableNodeKeys.includes(data.id) && data.label.toUpperCase().includes(value.toUpperCase())
    } else {
      // 如果没有设置，只搜索叶子节点
      return node.isLeaf && data.label.toUpperCase().includes(value.toUpperCase())
    }
  }
  getSimplePropsData (data) {
    const simplePropsData = {}
    for (const [key, value] of Object.entries(data)) {
      if (typeof value !== 'object') {
        simplePropsData[key] = value
      }
    }
    return simplePropsData
  }
  patchNodeLoading (data) {
    const hasLoadingNode = data.children.some(child => child.type === 'isLoading')
    if (data.isLoading) {
      if (!hasLoadingNode) {
        data.children.push({ id: 'isLoading', label: '', type: 'isLoading', parent: this.getSimplePropsData(data) })
      }
    } else {
      if (hasLoadingNode) {
        data.children = data.children.filter(child => child.type !== 'isLoading')
      }
    }
  }
  patchNodeMore (data) {
    const hasMoreNode = data.children.some(child => child.type === 'isMore')
    const lastChild = data.children[data.children.length - 1]
    if (data.isMore) {
      if (!hasMoreNode) {
        data.children.push({ id: 'isMore', label: '', type: 'isMore', parent: this.getSimplePropsData(data) })
      }
      if (lastChild && lastChild.type !== 'isMore') {
        data.children = data.children.filter(child => child.type !== 'isMore')
        data.children.push({ id: 'isMore', label: '', type: 'isMore', parent: this.getSimplePropsData(data) })
      }
    } else {
      if (hasMoreNode) {
        data.children = data.children.filter(child => child.type !== 'isMore')
      }
    }
  }
}
</script>

<style lang="less">
.tree-list {
  position: relative;
  width: 100%;
  .filter-box {
    margin-bottom: 10px;
  }
  .filter-box .el-input__inner {
    border: 1px solid #8E9FA8;
    border-radius: 2px;
  }
  .tree-item.el-loading-parent--relative {
    position: relative !important;
    width: calc(~'100% + 18px');
    height: 36px;
    left: -42px;
    background: white;
    flex: none;
    cursor: default;
    .el-loading-spinner {
      height: 36px;
    }
    .circular {
      width: 36px;
      height: 36px;
    }
  }
  .load-more {
    flex: none;
    height: 100%;
    line-height: 36px;
    position: relative;
    box-sizing: border-box;
  }
  .load-more-img {
    font-weight: bolder;
    width: 20px;
    height: 6px;
    margin-left: 10px;
    cursor: pointer;
  }
  .resize-bar {
    position: absolute;
    top: 50%;
    right: 0;
    transform: translate(10px, -50%);
    width: 10px;
    height: 84px;
    line-height: 84px;
    background-color: #cfd8dc;
    border: 1px solid #b0bec5;
    text-align: center;
    cursor: col-resize;
    user-select: none;
  }
  .resize-bar i {
    border-left: 1px solid #fff;
    height: 20px;
    & + i {
      margin-left: 2px;
    }
  }
  .selected {
    color: #0988de;
  }
}
</style>
