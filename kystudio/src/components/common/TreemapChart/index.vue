<template>
  <div class="treemap-chart">
    <div id="renderBox" class="project-capacity"></div>
  </div>
</template>

<script>
import Vue from 'vue'
import echarts from 'echarts'
import { Component, Watch } from 'vue-property-decorator'

@Component({
  props: ['data'],
  locales: {
    'en': {
      storage: 'Data Size',
      queryCount: 'Usage',
      MANUAL_AGG: 'Custom(Aggregate Group) ',
      AUTO_AGG: 'Recommended(Aggregate Group)',
      MANUAL_TABLE: 'Custom(Table Index)',
      AUTO_TABLE: 'Recommended(Table Index)'
    },
    'zh-cn': {
      storage: '数据大小',
      queryCount: '使用次数',
      MANUAL_AGG: '自定义聚合索引',
      AUTO_AGG: '系统推荐聚合索引',
      MANUAL_TABLE: '自定义明细索引',
      AUTO_TABLE: '系统推荐明细索引'
    }
  }
})
export default class TreemapChart extends Vue {
  areaWidth = 0
  blockColors = {
    OK: '#006FBB',
    ERROR: '#ff4949',
    TENTATIVE: '#b0bec5'
  }
  projectColors = {}
  ST = null
  myChart = null
  @Watch('data')
  async onDataChange () {
    if (this.data) {
      this.initChart()
    }
  }
  async mounted () {
    if (this.data) {
      this.initChart()
    }
    window.onresize = () => {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        this.$nextTick(() => {
          this.myChart.resize()
        })
      }, 400)
    }
  }
  get renderData () {
    var data = this.data
    if (!data) {
      return
    }
    var result = {
      type: 'treemap',
      data: [{
        name: '',
        children: [...data]
      }]
    }
    return result
  }
  get maxValue () {
    var data = this.data
    if (!data) {
      return
    }
    let value = 0
    data && data.forEach(d => {
      if (d.value > value) {
        value = d.value
      }
    })
    return value
  }
  initChart () {
    if (!this.renderData) {
      return
    }
    this.areaWidth = this.$el.offsetWidth
    this.$nextTick(() => {
      // 渲染正常project
      this.myChart = echarts.init(document.getElementById('renderBox'))
      // 提示框format
      var tooltipConfig = {
        trigger: 'item',
        formatter: (params) => {
          let tooltipContent = ''
          if (/^[0-9]+.?[0-9]*$/.test(params.name)) {
            tooltipContent = tooltipContent + 'Index ID: ' + params.name + '<br/>'
          } else {
            tooltipContent = tooltipContent + this.$t(params.data.name) + '<br/>'
          }
          tooltipContent = tooltipContent + this.$t('storage') + ': ' + Vue.filter('dataSize')(params.data.value)
          if (params.data.usage >= 0) {
            tooltipContent = tooltipContent + '<br/>' + this.$t('queryCount') + ': ' + params.data.usage
          }
          return tooltipContent
        }
      }
      var labelFormat = (params) => {
        this.$set(this.projectColors, params.name, params.color)
      }
      let commonOption = {
        roam: false,
        type: 'treemap',
        width: '100%',
        height: '100%',
        breadcrumb: false,
        itemStyle: {
          gapWidth: 1
        }
      }
      const getLevelOption = [
        {
          itemStyle: {
            normal: {
              borderWidth: 0,
              gapWidth: 1
            }
          }
        },
        {
          color: ['#81c6f4', '#81c6f4', '#81c6f4', '#81c6f4'],
          colorMappingBy: 'id',
          itemStyle: {
            normal: {
              gapWidth: 5
            }
          }
        },
        {
          colorSaturation: [0.9, 0.7],
          itemStyle: {
            normal: {
              gapWidth: 1
            }
          }
        }
      ]
      var option = {
        visualMap: {
          type: 'piecewise',
          min: 0,
          max: this.maxValue,
          show: false,
          itemGap: '2',
          inRange: {}
        },
        tooltip: tooltipConfig,
        series: [Object.assign({}, commonOption, {
          label: {
            normal: {
              position: 'insideTopLeft',
              color: '#191919',
              formatter: labelFormat
            }
          },
          levels: getLevelOption,
          data: this.renderData.data
        })]
      }
      this.myChart.off('click')
      this.myChart.on('click', (a) => {
        this.initChart()
        this.$emit('searchId', a.data.name)
      })
      this.myChart.setOption(option)
    })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.treemap-chart {
  .project-capacity {
    margin-top: 25px;
    height: 400px;
    width: 100%;
  }
}
</style>
