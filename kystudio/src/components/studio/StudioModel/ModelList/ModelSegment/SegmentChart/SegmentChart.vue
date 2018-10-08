<template>
  <div class="segment-chart">
    <div class="container" @scroll="handleScroll">
      <div class="stage" :style="stageStyle">
        <div class="segment"
          v-for="segment in inviewSegments"
          :style="segment.style"
          :key="segment.uuid"
          :class="{ selected: segment.isSelected }"
          @mouseout="event => handleMouseOut(event, segment)"
          @mousemove="event => handleMouseMove(event, segment)"
          @click="event => handleClick(event, segment)">
        </div>
        <div class="tick" v-for="tick in inviewTicks" :key="tick.timestamp" :style="tick.style">
          <div class="tick-label">{{tick.label}}</div>
        </div>
      </div>
    </div>
    <div class="el-popover" :style="tip.style" v-if="tip.id !== ''">
      <div class="popper__arrow"></div>
      <p>Segment ID: {{tip.id}}</p>
      <p>Storage Size: {{tip.storage}}</p>
      <p>Start: {{tip.startDate}}</p>
      <p>End: {{tip.endDate}}</p>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import dayjs from 'dayjs'
import { Component, Watch } from 'vue-property-decorator'

@Component({
  props: {
    dateRange: {
      type: Array,
      default: () => []
    },
    data: {
      type: Array,
      default: () => []
    },
    scaleType: {
      type: String,
      default: 'day',
      validator (value) {
        return ['minute', 'hour', 'day', 'month', 'year'].includes(value)
      }
    },
    gridWidth: {
      type: Number,
      default: 152
    }
  }
})
export default class SegmentChart extends Vue {
  timer = null
  isLoading = false
  segments = []
  initJobId = ''
  breakJobIds = []
  scrollOffset = 0
  elementWidth = 0
  tip = {
    id: '',
    storage: 0,
    startDate: '',
    endDate: '',
    style: {
      left: 0
    }
  }
  msTick = {
    minute: 1000 * 60,
    hour: 1000 * 3600,
    day: 1000 * 3600 * 24,
    month: 1000 * 3600 * 24 * 31,
    year: 1000 * 3600 * 24 * 31 * 12
  }
  labelFormats = {
    minute: 'mm:ss',
    hour: 'HH:mm A',
    day: 'MMM DD',
    month: 'MMM',
    year: 'YYYY'
  }
  get xTicks () {
    const ticks = []
    const startDate = this.getZeroAM(this.minStartTime)
    const endDate = this.getZeroAM(this.maxEndTime, 1)
    const formatType = this.labelFormats[this.scaleType]
    const tickCount = endDate.diff(startDate, this.scaleType)

    let currentDate = startDate
    for (let i = 0; i < tickCount; i++) {
      const label = dayjs(currentDate).format(formatType)
      ticks.push({
        label,
        timestamp: currentDate.valueOf(),
        style: this.getTickStyle(currentDate.valueOf()),
        isShow: this.isTickShow(currentDate.valueOf())
      })
      currentDate = currentDate.add(1, this.scaleType)
    }

    return ticks
  }
  get selectedSegments () {
    return this.segments.filter(segment => segment.isSelected)
  }
  get inviewSegments () {
    return this.segments.filter(segment => segment.isShow)
  }
  get inviewTicks () {
    return this.xTicks.filter(tick => tick.isShow)
  }
  get stageStyle () {
    const timeLong = this.maxEndTime - this.minStartTime
    const msTick = this.msTick[this.scaleType]
    return {
      width: `${timeLong / msTick * this.gridWidth}px`
    }
  }
  get minStartTime () {
    let minStartTime = Infinity
    this.segments.forEach(segment => {
      if (segment.dateRangeStart < minStartTime) {
        minStartTime = segment.dateRangeStart
      }
    })
    return dayjs(minStartTime !== Infinity ? minStartTime : undefined)
      .set('millisecond', 0)
      .set('millisecond', 0)
      .set('second', 0)
      .set('minute', 0)
      .set('hour', 0)
      .valueOf()
  }
  get maxEndTime () {
    const defaultEndTime = dayjs(this.minStartTime).add(15, this.scaleType).valueOf()
    let maxEndTime = -Infinity
    this.segments.forEach(segment => {
      if (segment.dateRangeEnd > maxEndTime) {
        maxEndTime = segment.dateRangeEnd
      }
    })
    return (maxEndTime === -Infinity || maxEndTime < defaultEndTime)
      ? defaultEndTime
      : maxEndTime
  }
  get segmentsMap () {
    const segmentsMap = {}
    for (const segment of this.segments) {
      segmentsMap[segment.uuid] = segment
    }
    return segmentsMap
  }
  @Watch('data')
  async onDataChange () {
    await this.initSegments()
    this.freshSegments()
  }
  @Watch('scaleType')
  async onScaleTypeChange () {
    this.freshSegments()
  }
  setInitJob (uuid) {
    this.initJobId = uuid
  }
  checkIsInitBreak (uuid) {
    return this.initJobId !== uuid
  }
  startLoadingData () {
    this.isLoading = true
  }
  endLoadingData () {
    this.isLoading = false
  }
  cleanupSegments (data) {
    this.segments = this.segments.filter(segment => data.some(item => item.uuid === segment.uuid))
  }
  getNewSegmentsData (data) {
    return data.filter(item => !(item.uuid in this.segmentsMap))
  }
  getUpdateSegmentsData (data) {
    return data.filter(item => item.uuid in this.segmentsMap)
  }
  getZeroAM (time, addition = 0) {
    const zeroAM = dayjs(time).add(addition, this.scaleType)
    zeroAM.set(0, 'hour')
    zeroAM.set(0, 'minute')
    zeroAM.set(0, 'second')
    zeroAM.set(0, 'millisecond')
    return zeroAM
  }
  getStartTimeArray (data) {
    return this.selectedSegments
      .filter(segment => data.id !== segment.id)
      .map(segment => segment.dateRangeStart)
  }
  getEndTimeArray (data) {
    return this.selectedSegments
      .filter(segment => data.id !== segment.id)
      .map(segment => segment.dateRangeEnd)
  }
  async pushSegments (data, offset, limit) {
    const newSegments = data.slice(offset, offset + limit)

    return new Promise(resolve => {
      setTimeout(() => {
        for (const newSegment of newSegments) {
          this.segments.push({ ...newSegment, isShow: false, style: {}, isSelected: false })
          this.segments[this.segments.length - 1].isShow = this.isSegmentShow(newSegment)
          this.segments[this.segments.length - 1].style = this.getSegmentStyle(newSegment)
        }
        resolve()
      }, 500)
    })
  }
  async initSegments () {
    const newData = JSON.parse(JSON.stringify(this.data))
    const limit = 20
    const jobId = new Date().getTime()

    this.setInitJob(jobId)
    this.startLoadingData()
    this.cleanupSegments(newData)
    const newSegment = this.getNewSegmentsData(newData)
    // const updateSegment = this.getUpdateSegmentsData(newData)

    let offset = 0
    for (let i = 0; i < newSegment.length / limit; i++) {
      if (this.checkIsInitBreak(jobId)) { return }

      await this.pushSegments(newSegment, offset, limit)
      offset += limit
    }

    this.endLoadingData()
  }
  async freshSegments () {
    this.segments.forEach(segment => {
      segment.isShow = this.isSegmentShow(segment)
      segment.style = this.getSegmentStyle(segment)
    })
  }
  getTickStyle (timestamp) {
    const msTick = this.msTick[this.scaleType]
    const left = (timestamp - this.minStartTime) / msTick * this.gridWidth
    return {
      left: `${left}px`
    }
  }
  getSegmentStyle (segment) {
    const timeLong = segment.dateRangeEnd - segment.dateRangeStart
    const msTick = this.msTick[this.scaleType]
    const width = timeLong / msTick * this.gridWidth
    const left = (segment.dateRangeStart - this.minStartTime) / msTick * this.gridWidth
    return {
      left: `${left}px`,
      width: `${width}px`,
      background: segment.hit_count === 0 ? 'white' : `rgb(255, ${(1 - segment.hit_count / 100) * 255}, 0)`
    }
  }
  isSegmentShow (segment) {
    const timeLong = segment.dateRangeEnd - segment.dateRangeStart
    const msTick = this.msTick[this.scaleType]
    const width = timeLong / msTick * this.gridWidth
    const segmentLeft = (segment.dateRangeStart - this.minStartTime) / msTick * this.gridWidth
    const segmentRight = segmentLeft + width

    const isOutOfLeft = segmentRight < this.scrollOffset - this.elementWidth
    const isOutOfRight = segmentLeft > this.scrollOffset + this.elementWidth + this.elementWidth

    return !(isOutOfLeft || isOutOfRight)
  }
  isTickShow (timestamp) {
    const msTick = this.msTick[this.scaleType]
    const tickLeft = (timestamp - this.minStartTime) / msTick * this.gridWidth
    const tickRight = tickLeft + 1

    const isOutOfLeft = tickRight < this.scrollOffset - this.elementWidth
    const isOutOfRight = tickLeft > this.scrollOffset + this.elementWidth + this.elementWidth

    return !(isOutOfLeft || isOutOfRight)
  }
  isSegmentSelectable (segment) {
    const { selectedSegments, getStartTimeArray, getEndTimeArray } = this
    const startTimeArray = getStartTimeArray(segment)
    const endTimeArray = getEndTimeArray(segment)

    const isStartTimeContinue = startTimeArray.includes(segment.dateRangeEnd)
    const isEndTimeContinue = endTimeArray.includes(segment.dateRangeStart)
    const isSelfCancelSegment = selectedSegments.length === 1 && segment.uuid === selectedSegments[0].uuid
    const isNoSegment = selectedSegments.length === 0
    const isCancelMiddleSegment = isStartTimeContinue && isEndTimeContinue
    // 对用户选择的segment进行判断
    // 已选中的segment判断是不是中间取消segment
    // 未选中的segment判断是不是时间连续
    const unselectedCondition = !segment.isSelected && (isStartTimeContinue || isEndTimeContinue)
    const selectedCondition = segment.isSelected && !isCancelMiddleSegment

    return unselectedCondition || selectedCondition || isNoSegment || isSelfCancelSegment
  }
  handleScroll (event) {
    clearInterval(this.timer)
    this.timer = setTimeout(() => {
      this.scrollOffset = event.target.scrollLeft
      this.freshSegments()
    }, 10)
  }
  handleClick (event, segment) {
    if (!segment.isMerging && segment.hit_count) {
      if (this.isSegmentSelectable(segment)) {
        segment.isSelected = !segment.isSelected
        this.emitValueChange()
      }
    }
  }
  handleMouseMove (event, segment) {
    this.tip.id = segment.id
    this.tip.storage = segment.size_kb > 1024 ? `${segment.size_kb / 1024}MB` : `${segment.size_kb}KB`
    this.tip.startDate = dayjs(segment.dateRangeStart).format('YYYY-MM-DD HH:mm:ss')
    this.tip.endDate = dayjs(segment.dateRangeEnd).format('YYYY-MM-DD HH:mm:ss')
    this.tip.style.left = `${event.offsetX + event.target.offsetLeft - this.scrollOffset}px`
  }
  handleMouseOut () {
    this.tip.id = ''
    this.tip.storage = 0
    this.tip.startDate = ''
    this.tip.endDate = ''
  }
  emitValueChange () {
    const selectedSegmentIds = this.segments
      .filter(segment => segment.isSelected)
      .map(segment => segment.uuid)

    this.$emit('input', selectedSegmentIds)
  }
  async mounted () {
    this.elementWidth = this.$el.clientWidth
    await this.initSegments()
    this.freshSegments()
  }
}
</script>

<style lang="less">
@import '../../../../../../assets/styles/variables.less';

.segment-chart {
  width: 100%;
  background: white;
  .container {
    width: 100%;
    overflow-x: auto;
    overflow-y: hidden;
    white-space: nowrap;
    position: relative;
  }
  .stage {
    height: 152px;
    margin: 20px 0 110px 0;
    border: 1px solid #CFD8DC;
  }
  .segment {
    box-sizing: border-box;
    position: absolute;
    height: 150px;
    cursor: pointer;
    &:hover {
      box-shadow: 0 0 8px 0 #455A64;
    }
    &.selected {
      border: 2px solid #0988DE;
      border-radius: 5px;
    }
  }
  .tick {
    position: absolute;
    width: 1px;
    background: #CFD8DC;
    height: 170px;
  }
  .tick-label {
    position: absolute;
    bottom: -10px;
    transform: translate(-50%, 100%);
  }
  .el-popover {
    position: absolute;
    top: 0;
    transform: translate(-25px, -100%);
    white-space: nowrap;
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
