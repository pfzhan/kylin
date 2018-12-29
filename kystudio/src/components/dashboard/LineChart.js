import d3 from 'd3'
import nv from 'nvd3'
import BaseChartMixin from './BaseChartMixin'

export default {
  name: 'LineChart',
  mixins: [BaseChartMixin],
  props: {
    xFormat: {type: [Function, String]},
    yFormat: {type: [Function, String]},
    colors: {type: Array, default: () => ['#15BDF1', '#ddd']}
  },
  mounted () {
    nv.addGraph(() => {
      const chart = nv.models.lineChart()
        .margin({top: 20, right: 40, bottom: 60, left: 35})
        .color(this.colors)
        .showLegend(false)
        .noData(this.$t('kylinLang.common.noData'))

      const xaxis = chart.xAxis.showMaxMin(true)
      if (this.xFormat) {
        if (typeof (this.xFormat) === 'string') {
          xaxis.tickFormat(d3.format(this.xFormat))
        } else {
          xaxis.tickFormat(this.xFormat)
        }
      }

      const yaxis = chart.yAxis.showMaxMin(false)
      if (this.yFormat) {
        if (typeof (this.yFormat) === 'string') {
          yaxis.tickFormat(d3.format(this.yFormat))
        } else {
          yaxis.tickFormat(this.yFormat)
        }
      }

      this.redraw(chart)
      this.chartRef = chart
      nv.utils.windowResize(chart.update)
      return chart
    })
  }
}
