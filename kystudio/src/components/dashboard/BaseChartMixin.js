import * as d3 from 'd3'

export default {
  props: {
    model: {type: Array},
    height: {type: String, default: '325px'},
    id: {type: String}
  },
  data () {
    return {
      chartRef: undefined
    }
  },
  watch: {
    model (value) {
      if (this.chartRef) {
        this.redraw(this.chartRef)
      }
    }
  },
  methods: {
    redraw (chart) {
      d3.select(this.$refs.chart)
        .style('height', this.height)
        .attr('id', this.id)
        .datum(this.model)
        .transition()
        .duration(500)
        .call(chart)
      if (this.id === 'barChart') {
        const chartBar = d3.select(`#${this.id} g.nv-groups`)
        const linearColor = chartBar.append('defs')
          .append('linearGradient')
          .attr('id', 'linear-gradient')
          .attr('x1', '65%')
          .attr('y1', '0%')
          .attr('x2', '35%')
          .attr('y2', '100%')
        linearColor.append('stop')
          .attr('offset', '0%')
          .style('stop-color', '#62E5FF')
          .style('stop-opacity', 1)
        linearColor.append('stop')
          .attr('offset', '100%')
          .style('stop-color', '#019EE5')
          .style('stop-opacity', 1)
        d3.selectAll(`#${this.id} g.nv-group rect`).attr('fill', 'url(#linear-gradient)')
      }
      if (this.id === 'lineChart') {
        const pathArea = d3.select(`#${this.id} g.nv-groups`)
        const linearColor2 = pathArea.append('defs')
          .append('linearGradient')
          .attr('id', 'linear-gradient2')
          .attr('x1', '0%')
          .attr('y1', '0%')
          .attr('x2', '0%')
          .attr('y2', '100%')
        linearColor2.append('stop')
          .attr('offset', '0%')
          .style('stop-color', '#0988DE')
          .style('stop-opacity', 0.2)
        linearColor2.append('stop')
          .attr('offset', '56%')
          .style('stop-color', '#BAE8FA')
          .style('stop-opacity', 0.2)
        linearColor2.append('stop')
          .attr('offset', '100%')
          .style('stop-color', '#fff')
          .style('stop-opacity', 0.2)
        d3.selectAll(`#${this.id} g.nv-group path`).attr('fill', 'url(#linear-gradient2)')
      }
    }
  },
  render (h) {
    return <svg ref="chart">
    </svg>
  }
}
