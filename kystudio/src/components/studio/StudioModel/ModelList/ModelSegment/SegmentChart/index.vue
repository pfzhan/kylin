<template>
  <div class="segment-chart">
    <svg ref="svg" :width="elementWidth" :height="elementHeight" v-if="isChartCreated">
      <defs>
        <filter id="mouseEnterStyle" x="-50%" y="-50%" width="200%" height="200%">
          <feOffset result="offOut" in="SourceAlpha" dx="0" dy="0" />
          <feGaussianBlur result="blurOut" in="offOut" stdDeviation="2" />
          <feBlend in="SourceGraphic" in2="blurOut" mode="normal" />
        </filter>
        <clipPath
          v-for="segment in segments"
          :key="`clip-${segment.id}`"
          :id="`clip-${segment.id}`">
          <rect
            :x="segment.x"
            :y="0"
            :width="segment.width"
            :height="chartHeight"
            fill="none"
          />
        </clipPath>
      </defs>
      <g class="stage" :width="stageWidth" :height="stageHeight" :transform="`translate(${margin.left}, ${margin.top})`" ref="stage">
        <g class="axis" ref="axis">
          <g class="x-axis" :transform="`translate(0, ${stageHeight})`" ref="x-axis"></g>
          <line class="x-line-top" x1="0" :x2="chartWidth" y1="-1" y2="-1" stroke="#b0bec5"></line>
          <line class="x-line-bottom" x1="0" :x2="chartWidth" :y1="chartHeight + 1" :y2="chartHeight + 1" stroke="#b0bec5"></line>
        </g>
        <g class="segment-group"
          v-for="segment in segments"
          :key="segment.id"
          :ref="segment.id"
          @mouseleave="event => handleMouseOut(event, segment)"
          @mouseenter="event => handleMouseEnter(event, segment)"
          @click="event => handleClick(event, segment)">
          <rect class="segment"
            :x="segment.x"
            :y="0"
            :width="segment.width"
            :height="chartHeight"
            :fill="getSegmentBgColor(segment)"
            :stroke="segment.isSelected ? '#0988DE' : 'transparent'"
            :stroke-width="2"
            stroke-linecap="round"
            :filter="segment.isHover ? 'url(#mouseEnterStyle)' : null"
          >
          </rect>
          <text
            v-if="segment.isMerging"
            :x="segment.x + segment.width / 2"
            :y="chartHeight / 2"
            fill="#263238"
            transform="translate(-31, -4)"
            :clip-path="segment.isMerging ? `url(#clip-${segment.id})` : null">
            Merging...
          </text>
          <rect
            v-if="segment.isMerging"
            :x="segment.x"
            :y="chartHeight - 10"
            :width="segment.width / 3"
            fill="#0988DE"
            height="10"
            :clip-path="segment.isMerging ? `url(#clip-${segment.id})` : null">
            <animate
              attributeType="XML"
              attributeName="x"
              :from="segment.x - segment.width / 3"
              :to="segment.x + segment.width + segment.width / 3"
              dur="3s"
              repeatCount="indefinite"/>
          </rect>
        </g>
      </g>
    </svg>
    <div
      v-if="hoveredSegment"
      class="el-popover"
      :style="{ left: `${hoveredSegment.x}px` }">
      <div class="popper__arrow"></div>
      <p v-for="(value, title) in hoveredSegmentInfo" :key="title">
        {{title}}: {{value}}
      </p>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import * as d3 from 'd3'
import { Component, Watch } from 'vue-property-decorator'

import { transToGmtTime } from '../../../../../../util/business'

@Component({
  props: {
    value: {
      type: Number,
      default: 100
    },
    scale: {
      type: String,
      default: 'Day'
    },
    segmentsData: {
      type: Array,
      default: () => []
    }
  }
})
export default class SegmentChart extends Vue {
  margin = { top: 10, right: 0, bottom: 20, left: 0 }
  fontMargin = 28
  isChartCreated = false
  segments = []
  // vue组件的整个元素宽度
  elementWidth = 0
  // vue组件的整个元素高度
  elementHeight = 0
  // 选出segment中from的最小时间
  get minDate () {
    return this.segments.length ? d3.min(this.segments, segment => new Date(segment.from)) : new Date()
  }
  // 选出segment中to的最大时间
  get maxDate () {
    const maxDate = new Date(this.minDate.getTime())
    switch (this.scale) {
      case 'Hour':
        return maxDate.setHours(maxDate.getHours() + 10)
      case 'Season':
        return maxDate.setMonth(maxDate.getMonth() + 3)
      case 'Month':
        return maxDate.setMonth(maxDate.getMonth() + 11)
      case 'Year':
        return maxDate.setFullYear(maxDate.getFullYear() + 2)
      default:
      case 'Day':
        return maxDate.setDate(maxDate.getDate() + 10)
    }
  }
  // svg-stage绘图高度
  get stageHeight () {
    const { elementHeight, margin } = this
    return elementHeight - margin.top - margin.bottom
  }
  // svg-stage绘图宽度
  get stageWidth () {
    const { elementWidth, margin } = this
    return elementWidth - margin.left - margin.bottom
  }
  // chart每个block的高度
  get chartHeight () {
    return this.stageHeight - this.fontMargin
  }
  // chart每个block的宽度
  get chartWidth () {
    return this.stageWidth
  }
  get selectedSegments () {
    return this.segments.filter(segment => segment.isSelected)
  }
  get hoveredSegment () {
    return this.segments.find(segment => segment.isHover)
  }
  get hoveredSegmentInfo () {
    return this.hoveredSegment ? {
      'Segment ID': this.hoveredSegment.id,
      'Storage Size': `${this.hoveredSegment.size_kb}KB`,
      'Start': transToGmtTime(this.hoveredSegment.dateRangeStart),
      'End': transToGmtTime(this.hoveredSegment.dateRangeEnd)
    } : null
  }
  @Watch('segmentsData')
  @Watch('maxDate')
  async onSegmentsDataChange () {
    await this.cleanSVG()
    await this.drawSVG()
  }
  cleanSVG () {
    return new Promise(resolve => {
      this.isChartCreated = false
      this.d3data = {}
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
      this.initSegments()

      setTimeout(() => {
        this.initSVGData(this.d3data)
        this.handleZoom()
        resolve()
      }, 20)
    })
  }
  async mounted () {
    await this.cleanSVG()
    await this.drawSVG()
  }
  // 事件: 处理缩放
  handleZoom () {
    const { xAxisEl, timeAxis, xAxis } = this.d3data
    // 根据zoom缩放重绘时间轴
    xAxisEl.call(xAxis.scale(timeAxis))
    // 更新segment渲染数据
    this.segments.forEach(segment => {
      segment.x = timeAxis(segment.from)
      segment.width = Math.max(8, timeAxis(segment.to) - timeAxis(segment.from))
    })
    const currentMinDate = timeAxis.domain()[0].getTime()
    const currentMaxDate = timeAxis.domain()[1].getTime()

    const zoomValue = 864000000 / (currentMaxDate - currentMinDate) * 100
    this.$emit('input', zoomValue)
  }
  handleMouseEnter (event, segment) {
    segment.isHover = true
    this.handleOrderZIndex(event.target, false)
  }
  handleMouseOut (event, segment) {
    segment.isHover = false
    this.handleOrderZIndex(event.target, true)
  }
  handleClick (event, segment) {
    if (!segment.isMerging && segment.hit_count) {
      this.$emit('select', segment, this.isSegmentSelectable(segment))
    }
  }
  handleOrderZIndex (element, isSelectedFirst) {
    if (element.tagName === 'g') {
      d3.select(element)
        .each(function () {
          this.parentNode.appendChild(this)
        })
    }
    if (isSelectedFirst) {
      this.selectedSegments
        .map(selectedSegment => {
          return d3.select(this.$refs[selectedSegment.id].length && this.$refs[selectedSegment.id][0])
        })
        .forEach(selectedSegmentEl => {
          selectedSegmentEl.each(function () {
            this.parentNode.appendChild(this)
          })
        })
    }
  }
  // 初始化segment不受vue数据响应影响的数据
  initSVGData (d3data) {
    const { minDate, maxDate, stageWidth, stageHeight } = this
    // segment绘图数据
    d3data.timeAxis = d3.time.scale().domain([minDate, maxDate]).range([0, stageWidth])
    d3data.xAxis = d3.svg.axis().scale(d3data.timeAxis).orient('bottom').tickSize(-stageHeight)
    d3data.zoom = d3.behavior.zoom().x(d3data.timeAxis).on('zoom', this.handleZoom)
    // segment绘图元素
    d3data.svgEl = d3.select(this.$refs['svg']).call(d3data.zoom)
    d3data.stageEl = d3.select(this.$refs['stage'])
    d3data.xAxisEl = d3.select(this.$refs['x-axis']).call(d3data.xAxis)
  }
  initSegments () {
    this.segments = this.segmentsData.map(segment => ({
      ...segment,
      x: 0,
      width: 0,
      isHover: false,
      from: segment.dateRangeStart,
      to: segment.dateRangeEnd
    }))
  }
  getSegmentBgColor (segment) {
    if (!segment.isMerging) {
      return segment.hit_count ? `rgb(255, ${Math.round((1 - segment.hit_count / 100) * 255)}, 0)` : 'white'
    } else {
      return '#E2ECF1'
    }
  }
  getStartTimeArray (data) {
    return this.selectedSegments
      .filter(segment => data.id !== segment.id)
      .map(segment => segment.date_range_start)
  }
  getEndTimeArray (data) {
    return this.selectedSegments
      .filter(segment => data.id !== segment.id)
      .map(segment => segment.date_range_end)
  }
  isSegmentSelectable (segment) {
    const { selectedSegments, getStartTimeArray, getEndTimeArray } = this
    const startTimeArray = getStartTimeArray(segment)
    const endTimeArray = getEndTimeArray(segment)

    const isStartTimeContinue = startTimeArray.includes(segment.date_range_end)
    const isEndTimeContinue = endTimeArray.includes(segment.date_range_start)
    const isSelfCancelSegment = selectedSegments.length === 1 && segment.id === selectedSegments[0].id
    const isNoSegment = selectedSegments.length === 0
    const isCancelMiddleSegment = isStartTimeContinue && isEndTimeContinue
    // 对用户选择的segment进行判断
    // 已选中的segment判断是不是中间取消segment
    // 未选中的segment判断是不是时间连续
    const unselectedCondition = !segment.isSelected && (isStartTimeContinue || isEndTimeContinue)
    const selectedCondition = segment.isSelected && !isCancelMiddleSegment

    return unselectedCondition || selectedCondition || isNoSegment || isSelfCancelSegment
  }
}
</script>

<style lang="less">
@import '../../../../../../assets/styles/variables.less';

.segment-chart {
  width: 100%;
  height: 220px;
  position: relative;
  line {
    stroke: @text-secondary-color;
  }
  .domain {
    fill: transparent;
  }
  .el-popover {
    position: absolute;
    top: 0;
    transform: translateY(-100%);
  }
  .el-popover .popper__arrow {
    position: absolute;
    display: block;
    width: 0;
    height: 0;
    bottom: -7px;
    left: 10%;
    margin-right: 2px;
    filter: drop-shadow(0 2px 12px rgba(0, 0, 0, 0.03));
    border: 6px solid transparent;
    border-top-color: @line-border-color1;
    border-bottom-width: 0;
    &::after {
      position: absolute;
      display: block;
      width: 0;
      height: 0;
      border: 6px solid transparent;
      content: '';
      border-top-color: #fff;
      border-bottom-width: 0;
      bottom: 1px;
      margin-left: -6px;
    }
  }
}
</style>
