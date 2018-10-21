<template>
  <div class="data-range-bar">
    <div class="background" :style="backgroundStyle">
      <div class="el-slider__button-wrapper left">
        <div class="el-slider__button el-tooltip"></div>
      </div>
      <div class="el-slider__button-wrapper right">
        <div class="el-slider__button el-tooltip"></div>
      </div>
    </div>
    <div class="frontground" ref="frontground">
      <el-slider
        range
        v-model="sliderValue"
        :max="totalTicks"
        :format-tooltip="handleFormatTip"
        :step="86400000"
        @mousedown.native="handleDragStart">
      </el-slider>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import dayjs from 'dayjs'
import { Component, Watch } from 'vue-property-decorator'
import rangeBarBgImg from './bg.png'

const draggableClasses = ['el-slider__button-wrapper', 'el-slider__button']
const SMALL = 0
const LARGE = 1

@Component({
  props: {
    maxRange: {
      type: Array,
      default: () => []
    },
    value: {
      type: Array,
      default: () => []
    },
    disabled: {
      type: Boolean,
      default: () => false
    }
  }
})
export default class DataRangeBar extends Vue {
  isDragging = false
  sliderValue = [0, 0]
  movement = 0
  timer = null
  get totalTicks () {
    const [ min, max ] = this.maxRange
    return max - min
  }
  get isOutOfMin () {
    return this.sliderValue[SMALL] <= 0
  }
  get isOutOfMax () {
    return this.sliderValue[LARGE] >= this.totalTicks
  }
  get backgroundStyle () {
    const backgroundImage = `url(${rangeBarBgImg})`
    const offsetWidth = this.movement < 30 ? this.movement : 30
    const width = `calc(100% + ${offsetWidth}px)`
    const left = `${this.isOutOfMin ? -offsetWidth : 0}px`
    return { backgroundImage, width, left }
  }
  @Watch('value')
  onValueChange () {
    this.fixSliderValue()
  }
  mounted () {
    const barEl = this.$el.querySelector('.el-slider__runway')

    this.fixSliderValue()
    barEl.addEventListener('mouseup', this.handleClick)
    document.addEventListener('mouseup', this.handleDragEnd)
    document.addEventListener('mousemove', this.handleDrag)
  }
  beforeDestroy () {
    const barEl = this.$el.querySelector('.el-slider__runway')

    barEl.removeEventListener('mouseup', this.handleClick)
    document.removeEventListener('mouseup', this.handleDragEnd)
    document.removeEventListener('mousemove', this.handleDrag)
  }
  fixSliderValue () {
    const [ min ] = this.maxRange
    this.sliderValue = [ this.value[SMALL] - min, this.value[LARGE] - min ]
  }
  handleDragEnd (event) {
    if (this.isDragging) {
      this.isDragging = false

      clearInterval(this.timer)
      this.timer = setTimeout(() => this.$emit('click'), 100)
    }
  }
  handleDrag (event) {
    const frontgroundEl = this.$refs['frontground']
    if (this.isDragging && this.isOutOfMin) {
      const targetEl = frontgroundEl.querySelectorAll('.el-slider__button')[SMALL]
      const { left } = targetEl.getBoundingClientRect()
      if (event.pageX <= left) {
        this.movement = left - event.pageX
      }
    } else if (this.isDragging && this.isOutOfMax) {
      const targetEl = frontgroundEl.querySelectorAll('.el-slider__button')[LARGE]
      const { left, width } = targetEl.getBoundingClientRect()
      if (event.pageX >= left + width) {
        this.movement = event.pageX - left - width
      }
    } else if (!this.isOutOfMin && !this.isOutOfMax) {
      this.movement = 0
    }
  }
  handleDragStart (event) {
    const isLeftKey = event.which === 1
    const classList = event.target.className.split(' ')
    const isDraggableEl = draggableClasses.some(draggableClass => classList.includes(draggableClass))
    if (!this.isDragging && isLeftKey && isDraggableEl && !this.disabled) {
      this.isDragging = true
    }
  }
  handleClick (event) {
    clearInterval(this.timer)
    this.timer = setTimeout(() => this.$emit('click'), 100)
  }
  handleFormatTip (val) {
    const currentTime = this.maxRange[SMALL] + val
    return dayjs(currentTime).format('YYYY-MM-DD')
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.data-range-bar {
  position: relative;
  height: 40px;
  .frontground {
    position: absolute;
    width: 100%;
    top: 0;
    left: 0;
  }
  .background {
    position: absolute;
    margin: 16px 0;
    top: 0;
    left: 0;
    height: 8px;
    width: 100%;
    background-color: #D0D8DC;
    background-size: 10px 8px;
    background-repeat: repeat-x;
  }
  .left {
    left: 0;
  }
  .right {
    left: 100%;
  }
  .el-slider__bar {
    background-color: #4CB050;
    height: 8px;
  }
  .el-slider__runway {
    height: 8px;
    background: transparent;
  }
  .el-slider__button {
    background-color: #1A731E;
    border: 0;
    border-radius: 0;
    width: 10px;
    height: 8px;
    position: relative;
    top: -2px;
    &:after {
      content: ' ';
      position: absolute;
      bottom: 0;
      left: 0;
      transform: translateY(100%);
      border-top: 7px solid #1A731E;
      border-left: 5px solid transparent;
      border-right: 5px solid transparent;
      width: 0;
      height: 0;
    }
  }
}
</style>
