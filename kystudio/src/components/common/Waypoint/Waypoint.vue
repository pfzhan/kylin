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
      type: HTMLElement,
      default: window
    },
    topOffset: {
      type: Number,
      default: 0
    }
  }
})
export default class Waypoint extends Vue {
  @Watch('scrollableAncestor')
  onScrollableAncestorChange (newValue, oldValue) {
    this.unbindEventListener(oldValue)
    this.bindEventListener(newValue)
  }
  mounted () {
    this.bindEventListener(this.scrollableAncestor)
  }
  beforeDestroy () {
    this.unbindEventListener(this.scrollableAncestor)
  }
  bindEventListener (newElement) {
    newElement.addEventListener('scroll', this.handleScroll)
  }
  unbindEventListener (oldElement) {
    oldElement.removeEventListener('scroll', this.handleScroll)
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
      return offsetTop + padding + this.topOffset
    } catch (e) {
      return 0
    }
  }
  handleScroll () {
    const offsetTop = this.getOffsetTop()
    const { scrollTop, clientHeight } = this.scrollableAncestor

    if (scrollTop + clientHeight === offsetTop) {
      this.$emit('enter')
    }
  }
}
</script>

<style>

</style>
