<template>
  <div class="partition-chart">
    <!-- <div class="operator">
      <label><input type="radio" name="mode" value="size">Size</label>
      <label><input type="radio" name="mode" value="count" checked>Count</label>
    </div> -->
    <svg ref="svg" v-if="isChartCreated" :width="elementWidth" :height="elementHeight">
      <defs>
        <filter id="shadow" x="-50%" y="-50%" width="200%" height="200%">
          <feOffset result="offOut" in="SourceAlpha" dx="0" dy="0" />
          <feGaussianBlur result="blurOut" in="offOut" stdDeviation="2" />
          <feBlend in="SourceGraphic" in2="blurOut" mode="normal" />
        </filter>
      </defs>
    </svg>
    <div class="tip" v-if="tip" :style="{ left: `${tipX + 20}px`, top: `${tipY + 48}px` }">{{tip}}</div>
  </div>
</template>

<script>
import Vue from 'vue'
import * as d3 from 'd3'
import { Component, Watch } from 'vue-property-decorator'

@Component({
  props: ['data']
})
export default class PartitionChart extends Vue {
  isChartCreated = false
  elementWidth = 0
  elementHeight = 0
  tip = ''
  tipX = 0
  tipY = 0
  @Watch('data')
  async onDataChange () {
    if (this.data[0]) {
      await this.cleanSVG()
      await this.drawSVG()
    }
  }
  async mounted () {
    if (this.data[0]) {
      await this.cleanSVG()
      await this.drawSVG()
    }
  }
  cleanSVG () {
    return new Promise(resolve => {
      this.isChartCreated = false
      setTimeout(() => {
        this.isChartCreated = true
        resolve()
      }, 20)
    })
  }
  drawSVG () {
    return new Promise(resolve => {
      this.d3data = {}
      this.elementWidth = this.$el.clientWidth
      this.elementHeight = this.$el.clientHeight

      setTimeout(() => {
        this.initChart()
        resolve()
      }, 20)
    })
  }
  updateSVG () {
    this.d3data.path
      .attr('filter', function (d) { return d.isHover || d.isSelected ? 'url(#shadow)' : null })
      .style('stroke', function (d) { return d.isSelected ? '#0988DE' : '#FFFFFF' })

    this.d3data.path
      .filter(function (d) { return d.isSelected })
      .each(function () { this.parentNode.appendChild(this) })
    this.d3data.path
      .filter(function (d) { return d.isHover })
      .each(function () { this.parentNode.appendChild(this) })
  }
  initChart () {
    const self = this
    const width = this.elementWidth
    const height = this.elementHeight - 30
    const radius = Math.min(width, height) / 2
    const color = d3.scale.category20c()

    const svg = d3.select(this.$refs['svg'])
      .append('g')
      .attr('transform', 'translate(' + width / 2 + ',' + height * 0.52 + ')')

    const partition = d3.layout.partition()
      .sort(null)
      .size([2 * Math.PI, radius * radius])
      .value(function (d) { return 1 })

    const arc = d3.svg.arc()
      .startAngle(function (d) { return d.x })
      .endAngle(function (d) { return d.x + d.dx })
      .innerRadius(function (d) { return Math.sqrt(d.y) })
      .outerRadius(function (d) { return Math.sqrt(d.y + d.dy) })

    const path = this.d3data.path = svg.datum(this.data[0]).selectAll('path')
      .data(partition.nodes)
      .enter().append('path')
      .attr('display', function (d) { return d.depth ? null : 'none' }) // hide inner ring
      .attr('d', arc)
      .style('stroke', '#fff')
      .style('fill', function (d) { return color((d.children ? d : d.parent).name) })
      .style('fill-rule', 'evenodd')
      .each(stash)

    path.on('mouseenter', function (d) {
      d.isHover = true
      self.updateSVG()
    })
    path.on('mouseout', function (d) {
      d.isHover = false
      self.tip = ''
      self.updateSVG()
    })
    path.on('click', function (d) {
      path.each(function (d) { d.isSelected = false })
      d.isSelected = true
      self.updateSVG()
      self.$emit('on-click-node', d)
    })
    path.each(function (d) {
      this.addEventListener('mousemove', (event) => {
        self.tipX = event.offsetX
        self.tipY = event.offsetY
        self.tip = d.name || d.id
      })
    })

    d3.select(this.$el).selectAll('input').on('change', function change () {
      const value = this.value === 'count'
        ? function () { return 1 }
        : function (d) { return d.size }

      path
        .data(partition.value(value).nodes)
        .transition()
        .duration(1500)
        .attrTween('d', arcTween)
    })

    // Stash the old values for transition.
    function stash (d) {
      d.x0 = d.x
      d.dx0 = d.dx
    }

    // Interpolate the arcs in data space.
    function arcTween (a) {
      const i = d3.interpolate({x: a.x0, dx: a.dx0}, a)
      return function (t) {
        const b = i(t)
        a.x0 = b.x
        a.dx0 = b.dx
        return arc(b)
      }
    }

    d3.select(self.frameElement).style('height', height + 'px')
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.partition-chart {
  width: calc(~'100% - 20px');
  height: 563px;
  .operator {
    position: absolute;
    right: 0;
    top: 50px;
    label {
      margin-right: 10px;
    }
    input {
      margin-right: 10px;
    }
  }
  .tip {
    position: absolute;
    transform: translate(-50%, -100%);
    padding: 10px;
    background: @text-normal-color;
    color: #fff;
    pointer-events: none;
  }
}
</style>
