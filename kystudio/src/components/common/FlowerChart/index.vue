<template>
  <div class="aggregate-chart"></div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { CodeFlower } from '../../../util/code_flower'

@Component({
  props: {
    data: {
      type: Object
    }
  }
})
export default class FlowerChart extends Vue {
  mounted () {
    this.d3data = new CodeFlower(this.$el, this.$el.clientWidth, 638)
    this.d3data.update(this.data, this.$el.clientWidth, 638)
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.aggregate-chart {
  width: 100%;
  .node {
    cursor: pointer;
    stroke: #3182bd;
    stroke-width: 1.5px;
  }
  .link {
    fill: none;
    stroke: #9ecae1;
    stroke-width: 1.5px;
  }
  .el-tooltip__popper {
    padding: 5px;
    background: @text-normal-color;
    color: #fff;
  }
}
</style>
