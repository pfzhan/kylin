<template>  
  <div class="fake_progess" v-show="percentage">
    <div><slot name="sublabel"></slot></div>
    <el-progress type="circle" :width="w" :percentage="percentage" :stroke-width="wstroke"></el-progress>
    <div><slot name="underlabel"></slot></div>
  </div>
</template>
<script>
export default {
  name: 'fackprogress',
  props: ['speed', 'step', 'width', 'stroke'],
  data () {
    return {
      percentage: 0,
      ST: null,
      w: this.width || 126,
      wstroke: this.stroke || 6
    }
  },
  methods: {
    start () {
      this.ST = setInterval(() => {
        var nextVal = Math.round((this.step || 5) * Math.random() + this.percentage)
        if (nextVal < 99) {
          this.percentage = nextVal
        } else {
          clearInterval(this.ST)
        }
      }, this.speed || 1000)
    },
    stop () {
      clearInterval(this.ST)
      this.percentage = 0
    }
  },
  destoryed () {
    clearInterval(this.ST)
  }
}
</script>
<style lang="less">
  .fake_progess {
    display: inline-block;
  }
</style>
