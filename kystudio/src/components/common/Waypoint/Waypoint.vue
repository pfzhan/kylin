<template>
  <div class="waypoint">
    <slot></slot>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'

@Component({
  props: {
    scrollableAncestor: {
      type: [HTMLElement, Window],
      default: () => window
    },
    topOffset: {
      type: Number,
      default: 0
    },
    leftOffset: {
      type: Number,
      default: 0
    }
  }
})
export default class Waypoint extends Vue {
  offsetTop = 0
  scrollTop = 0
  offsetLeft = 0
  scrollLeft = 0
  clientWidth = 0
  clientHeight = 0
  isVerticalInview = false
  isHorizontalInview = false
  initialized = false
  timer = null
  @Watch('scrollableAncestor')
  onScrollableAncestorChange (newValue, oldValue) {
    this.initialized = false
    oldValue && this.unbindEventListener(oldValue)
    newValue && this.bindEventListener(newValue)
    newValue && this.handleScroll()
  }
  @Watch('offsetTop')
  @Watch('scrollTop')
  onPageTopChange (newValue, oldValue) {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      if (newValue !== oldValue && this.initialized) {
        if (this.checkPosition('vertical')) {
          if (!this.isVerticalInview) {
            this.$emit('enter')
          }
        } else {
          this.isVerticalInview = false
        }
      }
    }, 200)
  }
  @Watch('offsetLeft')
  @Watch('scrollLeft')
  onPageLeftChange (newValue, oldValue) {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => {
      if (newValue !== oldValue && this.initialized) {
        if (this.checkPosition('horizontal')) {
          if (!this.isHorizontalInview) {
            this.$emit('enter')
          }
        } else {
          this.isHorizontalInview = false
        }
      }
    }, 200)
  }
  mounted () {
    this.scrollableAncestor && this.bindEventListener(this.scrollableAncestor)
  }
  beforeDestroy () {
    this.scrollableAncestor && this.unbindEventListener(this.scrollableAncestor)
  }
  bindEventListener (newElement) {
    newElement.addEventListener('scroll', this.handleScroll)
  }
  unbindEventListener (oldElement) {
    oldElement.removeEventListener('scroll', this.handleScroll)
  }
  handleScroll () {
    this.offsetTop = this.getOffsetTop()
    this.offsetLeft = this.getOffsetLeft()
    this.scrollTop = this.scrollableAncestor.scrollTop
    this.scrollLeft = this.scrollableAncestor.scrollLeft
    this.clientWidth = this.scrollableAncestor.clientWidth
    this.clientHeight = this.scrollableAncestor.clientHeight
    this.initialized = true
  }
  checkPosition (position) {
    const { offsetTop, scrollTop, offsetLeft, scrollLeft, clientWidth, clientHeight } = this

    switch (position) {
      case 'vertical':
        return scrollTop + clientHeight >= offsetTop
      case 'horizontal':
        return scrollLeft + clientWidth >= offsetLeft
    }
  }
  getOffsetTop () {
    try {
      let offsetTop = 0
      let offsetParent = this.$el
      let padding = 0

      while (offsetParent !== this.scrollableAncestor) {
        offsetTop += offsetParent.offsetTop
        offsetParent = offsetParent.offsetParent
        padding += +window.getComputedStyle(offsetParent, null).paddingTop.replace('px', '')
        padding += +window.getComputedStyle(offsetParent, null).paddingBottom.replace('px', '')
      }
      return offsetTop + padding + this.leftOffset
    } catch (e) {
      return 0
    }
  }
  getOffsetLeft () {
    try {
      let offsetLeft = 0
      let offsetParent = this.$el
      let padding = 0

      while (offsetParent !== this.scrollableAncestor) {
        offsetLeft += offsetParent.offsetLeft
        offsetParent = offsetParent.offsetParent
        padding += +window.getComputedStyle(offsetParent, null).paddingLeft.replace('px', '')
        padding += +window.getComputedStyle(offsetParent, null).paddingRight.replace('px', '')
      }
      return offsetLeft + padding + this.leftOffset
    } catch (e) {
      return 0
    }
  }
}
</script>

<style>

</style>
