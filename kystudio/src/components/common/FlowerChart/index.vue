<template>
  <div class="aggregate-chart">
    <svg ref="svg" :width="elementWidth" :height="elementHeight" v-if="isChartCreated">
      <defs>
        <filter id="shadow" x="-50%" y="-50%" width="200%" height="200%">
          <feOffset result="offOut" in="SourceAlpha" dx="0" dy="0" />
          <feGaussianBlur result="blurOut" in="offOut" stdDeviation="2" />
          <feBlend in="SourceGraphic" in2="blurOut" mode="normal" />
        </filter>
      </defs>
      <g class="flower-group"
        v-for="(flower, flowerIdx) in flowers"
        :key="`${flowerIdx}`">
        <g class="links">
          <line class="link"
            v-for="link in flower.links"
            :key="`link-${flowerIdx}-${link.id}`"
            :x1="link.source.x"
            :y1="link.source.y"
            :x2="link.target.x"
            :y2="link.target.y">
          </line>
        </g>
        <g class="nodes">
          <circle class="node"
            v-for="node in flower.nodes"
            :key="`node-${flowerIdx}-${node.id}`"
            :class="getNodeClass(node)"
            :r="Math.sqrt(node.size) / 10 || 4.5"
            :cx="node.x"
            :cy="node.y"
            :fill="node.id !== 'root' ? `hsl(${parseInt(360 / flower.nodes.length * node.id, 10)},90%,70%)` : 'white'"
            :filter="node.isSelected || node.isHover ? 'url(#shadow)' : null"
            :stroke="node.isSelected ? '#0988DE' : '#455A64'"
            :stroke-width="node.isSelected ? '2px' : '1px'"
            @mouseenter="handleMouseEnter(node)"
            @mouseout="handleMouseOut(node)"
            @click="handleClickNode(node)">
          </circle>
        </g>
      </g>
    </svg>
  </div>
</template>

<script>
import Vue from 'vue'
import * as d3 from 'd3'
import { Component, Watch } from 'vue-property-decorator'

@Component({
  props: {
    data: {
      type: Array
    }
  }
})
export default class FlowerChart extends Vue {
  isChartCreated = false
  elementWidth = 0
  elementHeight = 0
  oriData = []
  flowers = []
  flowerCountX = 1
  flowerCountY = 1

  get flowerWidth () {
    return this.elementWidth / this.flowerCountX
  }
  get flowerHeight () {
    return this.elementHeight / this.flowerCountY
  }
  getNodeClass (node) {
    return {
      collapsed: !!node._children,
      directory: !!(node._children || node.children)
    }
  }
  @Watch('data')
  async onDataChange () {
    this.oriData = this.formatData(JSON.parse(JSON.stringify(this.data)))
    await this.cleanSVG()
    await this.drawSVG()
  }
  async mounted () {
    this.oriData = this.formatData(JSON.parse(JSON.stringify(this.data)))
    await this.cleanSVG()
    await this.drawSVG()
  }
  handleMouseEnter (hoveredNode) {
    hoveredNode.isHover = true
    hoveredNode.size = hoveredNode.size ? hoveredNode.size * 10 : 45
    // this.updateSVG()
  }
  handleMouseOut (hoveredNode) {
    hoveredNode.isHover = false
    hoveredNode.size = hoveredNode.size ? hoveredNode.size / 10 : 4.5
    // this.updateSVG()
  }
  handleClickNode (clickedNode) {
    if (clickedNode.isSelected) {
      if (clickedNode.children) {
        clickedNode._children = clickedNode.children
        clickedNode.children = null
      } else {
        clickedNode.children = clickedNode._children
        clickedNode._children = null
      }
    } else {
      if (clickedNode.id !== 'root') {
        this.flowers.forEach(flower => flower.nodes.forEach(node => {
          node.isSelected = false
        }))
        clickedNode.isSelected = true
        this.$emit('on-click-node', clickedNode)
      }
    }
    this.initFlowers()
    this.updateSVG()
  }
  cleanSVG () {
    return new Promise(resolve => {
      this.isChartCreated = false
      this.d3data = {}
      while (this.flowerCountX * this.flowerCountY < this.data.length) {
        this.flowerCountX <= this.flowerCountY
          ? this.flowerCountX++
          : this.flowerCountY++
      }
      setTimeout(() => {
        this.isChartCreated = true
        resolve()
      }, 20)
    })
  }
  drawSVG () {
    return new Promise(resolve => {
      this.elementWidth = this.$el.clientWidth
      this.elementHeight = this.$el.clientHeight
      this.initFlowers()

      setTimeout(() => {
        this.initSVGData(this.d3data)
        this.updateSVG()
        resolve()
      }, 20)
    })
  }
  updateSVG () {
    const { d3data, flowers } = this
    flowers.forEach((flower, flowerIdx) => {
      const force = d3data.force[flowerIdx]
      force.nodes(flower.nodes).links(flower.links).start()
    })
  }
  initFlowers () {
    const { flowerWidth, flowerHeight, oriData, flatten } = this
    let useSplitX = 0
    let useSplitY = 0
    this.flowers = oriData.map((item, index) => {
      const data = {
        ...item,
        fixed: true,
        x: flowerWidth / 2 + useSplitX * flowerWidth,
        y: flowerHeight / 2 + useSplitY * flowerHeight
      }
      const nodes = flatten(data)
      const links = d3.layout.tree().links(nodes)

      if (useSplitX >= this.flowerCountX - 1) {
        useSplitX = 0
        useSplitY++
      } else {
        useSplitX++
      }
      return { data, nodes, links }
    })
  }
  initSVGData () {
    const { elementWidth, elementHeight } = this
    this.d3data.force = []
    this.flowers.forEach((flower) => {
      const force = d3.layout.force()
        .linkDistance(d => d.target._children ? 80 : 25)
        .size([elementWidth, elementHeight])
      this.d3data.force.push(force)
      d3.select(this.$refs['svg']).selectAll('.node').data(flower.nodes).call(force.drag)
    })
  }
  flatten (root) {
    const nodes = []
    let i = 0
    function recurse (node) {
      if (node.children) node.children.forEach(recurse)
      if (!node.id) node.id = ++i
      nodes.push(node)
    }
    recurse(root)
    return nodes
  }
  // 对数据响应预声明在这里定义
  // 原因：d3会对原对象进行拆分引用，nodes和links之间会相互影响
  formatData (root) {
    const recurse = (node, isSelected) => {
      if (node.children) node.children.forEach(recurse)
      !node.x && (node.x = null)
      !node.y && (node.y = null)
      !node._children && (node._children = null)
      !node.isHover && (node.isHover = false)
      !node.isSelected && (node.isSelected = false)
      return node
    }
    return root.map((data, index) => recurse(data, index === 0))
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.aggregate-chart {
  width: 100%;
  height: 638px;
  .node {
    cursor: pointer;
    transition: r .2s ease-out;
  }
  .link {
    fill: none;
    stroke: #455A64;
    stroke-width: 1px;
  }
  .el-tooltip__popper {
    padding: 5px;
    background: @text-normal-color;
    color: #fff;
  }
}
</style>
