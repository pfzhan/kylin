<template>
  <div class="date-range-bar">
    <div class="date-range"
      v-for="range in ranges"
      :key="range.startDate"
      :style="getRangeStyle(range)">
      <el-tooltip effect="dark" :content="getDateText(range.startDate)" placement="top">
        <div class="range-point left" :style="getRangePointStyle(range)"></div>
      </el-tooltip>
      <el-tooltip effect="dark" :content="getDateText(range.endDate)" placement="top">
        <div class="range-point right" :style="getRangePointStyle(range)"></div>
      </el-tooltip>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

import { handleAutoGroup, handleCalcRangeSize } from './handle'

@Component({
  props: {
    dateRanges: {
      type: Array,
      default: () => []
    }
  }
})
export default class DateRangeBar extends Vue {
  elementWidth = 0
  elementHeight = 0
  groupMap = {
    minute: 0,
    hour: 0,
    day: 0,
    week: 0,
    month: 0,
    year: 0
  }
  getRangeStyle (range) {
    return {
      width: `${range.width}px`,
      left: `${range.left}px`,
      background: range.color
    }
  }
  getRangePointStyle (range) {
    return {
      background: range.pointColor
    }
  }
  getDateText (dateTime) {
    const date = new Date(dateTime)
    const year = date.getFullYear()
    const month = this.getDoubleDigit(date.getMonth() + 1)
    const day = this.getDoubleDigit(date.getDate())
    return `${year}-${month}-${day}`
  }
  getDoubleDigit (number) {
    return number < 10 ? `0${number}` : `${number}`
  }
  created () {
    this.initRatios()
  }
  get ranges () {
    const dateRanges = JSON.parse(JSON.stringify(this.dateRanges))
    const sortedDateRanges = dateRanges.sort((rangeA, rangeB) => rangeA.startDate > rangeB.startDate ? 1 : -1)
    const ranges = handleAutoGroup(sortedDateRanges, this.groupMap)
    return handleCalcRangeSize(ranges, this.groupMap, this.elementWidth)
  }
  initRatios () {
    this.groupMap.minute = 1000 * 60
    this.groupMap.hour = this.groupMap.minute * 60
    this.groupMap.day = this.groupMap.hour * 24
    this.groupMap.week = this.groupMap.day * 7
    this.groupMap.month = this.groupMap.week * 4
    this.groupMap.year = this.groupMap.month * 12
  }
  freshElementSize () {
    // element table在计算宽度时比较慢，延时200ms来确保获取父元素正常尺寸
    setTimeout(() => {
      this.elementWidth = this.$el.clientWidth
      this.elementHeight = this.$el.clientHeight
    }, 200)
  }
  watchWindowChange () {
    window.addEventListener('resize', this.freshElementSize)
  }
  mounted () {
    this.freshElementSize()
    this.watchWindowChange()
  }
  beforeDestory () {
    window.removeEventListener('resize', this.freshElementSize)
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.date-range-bar {
  position: relative;
  width: 100%;
  height: 8px;
  background: #cfd8dc;
  &:after {
    content: '';
    width: 100%;
    height: 8px;
    display: block;
    background-size: 10px 8px;
    background-image: url('./bg.png');
    background-repeat: repeat-x;
  }
  .date-range {
    position: absolute;
    height: 100%;
  }
  .right {
    right: -6px;
  }
  .left {
    left: -6px;
  }
  .range-point {
    position: absolute;
    top: -2px;
    width: 12px;
    height: 12px;
    border-radius: 50%;
    cursor: pointer;
    &:hover {
      transform: translate(-3px, -3px);
      width: 18px;
      height: 18px;
      box-shadow: 0 0 2px 0 #455A64;
    }
  }
  .range-text {
    white-space: nowrap;
    background: #f7f7f7;
    position: absolute;
    bottom: -24px;
    left: -6px;
  }
}
</style>
