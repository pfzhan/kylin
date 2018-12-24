import nv from 'nvd3'
import * as d3 from 'd3'
import BaseChartMixin from './BaseChartMixin'
import 'nvd3/build/nv.d3.css'

export default {
  name: 'BarChart',
  mixins: [BaseChartMixin],
  props: {
    textField: {type: String, default: 'label'},
    valueField: {type: String, default: 'value'},
    xFormat: {type: [Function, String]},
    yFormat: {type: [Function, String]},
    staggerLabels: {type: Boolean, default: false},
    tooltips: {type: Boolean, default: false},
    showValues: {type: Boolean, default: true},
    colors: {type: Array, default: () => ['#82DFD6', '#ddd']},
    contentGenerator: {type: Function}
  },
  mounted () {
    const textField = this.textField
    const valField = this.valueField

    nv.addGraph(() => {
      const chart = nv.models.discreteBarChart()
        .x(d => d[textField])
        .y(d => d[valField])
        .margin({top: 20, right: 40, bottom: 60, left: 25})
        .staggerLabels(this.staggerLabels)    // Too many bars and not enough room? Try staggering labels.
        .showValues(this.showValues)       // ...instead, show the bar value right on top of each bar.
        .noData(this.$t('kylinLang.common.noData'))

      // Axis settings
      const xaxis = chart.xAxis.showMaxMin(false)
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
      if (this.contentGenerator) {
        chart.tooltip.contentGenerator(this.contentGenerator)
      }

      this.redraw(chart)
      this.chartRef = chart

      nv.utils.windowResize(chart.update)
      return chart
    })
  }
}
