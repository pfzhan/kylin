<template>
  <div class="ksd-slider">
	<el-checkbox v-model="openCollectRange">{{label}}</el-checkbox>
    <el-slider :min="minConfig" :show-stops="showStop" :step="stepConfig" @change="changeBarVal" v-model="staticsRange" :max="maxConfig" :format-tooltip="formatTooltip" :disabled = '!openCollectRange'></el-slider> <span>{{staticsRange}}%</span>
  </div>
</template>
<script>
export default {
  name: 'pager',
  props: ['label', 'step', 'showStops', 'max', 'min', 'show'],
  data () {
    return {
      stepConfig: this.step || 20,
      openCollectRange: false,
      maxConfig: this.max || 100,
      showStop: this.showStops || true,
      minConfig: this.min || 0,
      staticsRange: 0,
      currentPage: this.curPage || 1
    }
  },
  methods: {
    formatTooltip (value) {
      return value + '%'
    },
    changeBarVal (val) {
      this.$emit('changeBar', val)
      if (val === 0) {
        this.openCollectRange = false
      }
    },
    reset () {
      this.openCollectRange = false
      this.staticsRange = 0
      this.$emit('changeBar', 0)
    }
  },
  watch: {
    'show' () {
      this.reset()
    },
    'openCollectRange' (val) {
      if (val) {
        this.staticsRange = 100
      } else {
        this.staticsRange = 0
      }
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
