<template>
  <div class="ksd-slider">
	<el-checkbox v-model="openCollectRange" v-if="!hideCheckbox" @change="changeCollectRange"><span v-if="label">{{label}}</span>
   <slot name="label" v-if="!label"></slot>
  </el-checkbox>
  <div class="ksd-mt-10"><slot name="sliderLabel"></slot></div>
    <el-slider :min="minConfig" :show-stops="showStop" :step="stepConfig" @change="changeBarVal" v-model="staticsRange" :max="maxConfig" :format-tooltip="formatTooltip" ></el-slider> 
    <span v-show="!noShowRange">{{staticsRange}}%</span>
  </div>
</template>
<script>
export default {
  name: 'pager',
  props: ['label', 'step', 'showStops', 'max', 'min', 'show', 'hideCheckbox', 'range', 'noShowRange'],
  data () {
    return {
      stepConfig: this.step || 20,
      openCollectRange: false,
      maxConfig: this.max || 100,
      showStop: this.showStops || true,
      minConfig: this.min || 0,
      staticsRange: this.range,
      currentPage: this.curPage || 1
    }
  },
  methods: {
    formatTooltip (value) {
      return value + '%'
    },
    changeBarVal (val) {
      this.$emit('changeBar', val)
      if (val > 0) {
        this.openCollectRange = true
        this.staticsRange = val
      }
    },
    reset () {
      this.openCollectRange = false
      this.staticsRange = this.range || 0
      this.$emit('changeBar', this.range || 0)
    },
    changeCollectRange () {
      if (this.openCollectRange) {
        this.staticsRange = 100
      } else {
        this.staticsRange = 0
      }
    }
  },
  watch: {
    'show' () {
      this.reset()
    }
  }
}
</script>
<style lang="less">
.ksd-slider {
  .el-slider__stop {
    width: 8px;
    height: 8px;
    top:-2px;
  }
}
</style>
