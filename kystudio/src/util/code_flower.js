import * as d3 from 'd3'

export function CodeFlower (selector, w, h) {
  this.w = w
  this.h = h

  d3.select(selector).selectAll('svg').remove()

  this.svg = d3.select(selector).append('svg:svg')
    .attr('width', w)
    .attr('height', h)

  this.svg.append('svg:rect')
    // .style('stroke', '#999')
    .style('fill', '#fff')
    .attr('width', w)
    .attr('height', h)

  this.force = d3.layout.force()
    .on('tick', this.tick.bind(this))
    // .charge(function (d) { return d._children ? -d.size / 100 : -40 })
    .linkDistance(function (d) { return d.target._children ? 80 : 25 })
    .size([w, h])
}

CodeFlower.prototype.array = function (array, w, h) {
  let splitX = 1
  let splitY = 1

  do {
    splitX < splitY ? splitX++ : splitY++
  } while (splitX * splitY <= array.length)

  const width = w / splitX
  const height = h / splitY

  array.forEach((json, index) => {
    this.update(json, width, height, index, splitX, splitY)
  })
}

CodeFlower.prototype.update = function (json, w, h, index = 0, splitX = 1, splitY = 1) {
  if (json) {
    this.json = json
  }

  if (w) {
    this.w = w
    this.svg.attr('width', w * splitX)
  }
  if (h) {
    this.h = h
    this.svg.attr('height', h * splitY)
  }

  this.json.fixed = true
  this.json.x = this.w / 2 + index * this.w
  this.json.y = this.h / 2 + index * this.h

  var nodes = this.flatten(this.json)
  var links = d3.layout.tree().links(nodes)
  var total = nodes.length || 1
  // var k = Math.sqrt(total / (this.w * this.h))
  // var gravity = Math.sqrt(total / (this.w * this.h))

  // Restart the force layout
  this.force
    // .gravity(100 * k)
    // .charge(-10 / k)
    .nodes(nodes)
    .links(links)
    .start()

  // Update the links
  this.link = this.svg.selectAll('line.link')
    .data(links, function (d) { return d.target.id })

  // Enter any new links
  this.link.enter().insert('svg:line', '.node')
    .attr('class', 'link')
    .attr('x1', function (d) { return d.source.x })
    .attr('y1', function (d) { return d.source.y })
    .attr('x2', function (d) { return d.target.x })
    .attr('y2', function (d) { return d.target.y })

  // Exit any old links.
  this.link.exit().remove()

  // Update the nodes
  this.node = this.svg.selectAll('circle.node')
    .data(nodes, function (d) { return d.name || d.id })
    .classed('collapsed', function (d) { return d._children ? 1 : 0 })

  this.node.transition()
      .attr('r', function (d) { return Math.sqrt(d.size) / 10 })

  // Enter any new nodes
  this.node.enter().append('svg:circle')
    .attr('class', 'node')
    .classed('directory', function (d) { return (d._children || d.children) ? 1 : 0 })
    .attr('cx', function (d) { return d.x })
    .attr('cy', function (d) { return d.y })
    .attr('r', function (d) { return Math.sqrt(d.size) / 10 || 4.5 })
    .style('fill', function color (d) {
      return 'hsl(' + parseInt(360 / total * d.id, 10) + ',90%,70%)'
      // return d._children ? '#3182bd' : d.children ? '#c6dbef' : '#fd8d3c'
    })
    .call(this.force.drag)
    .on('click', this.click.bind(this))
    .on('mouseover', this.mouseover.bind(this))
    .on('mouseout', this.mouseout.bind(this))

  // Exit any old nodes
  this.node.exit().remove()

  return this
}

CodeFlower.prototype.flatten = function (root) {
  var nodes = []
  var i = 0

  function recurse (node) {
    if (node.children) node.children.forEach(recurse)
    if (!node.id) node.id = ++i
    nodes.push(node)
  }

  recurse(root)
  return nodes
}

CodeFlower.prototype.click = function (d) {
  // Toggle children on click.
  if (!d3.event.defaultPrevented) {
    if (d.children) {
      d._children = d.children
      d.children = null
    } else {
      d.children = d._children
      d._children = null
    }
    this.update()
  }
}

CodeFlower.prototype.mouseover = function (d) {
  d.size = d.size ? d.size * 10 : 45
  this.update()

  d3.select('#visualization')
    .append('div')
    .attr('class', 'el-tooltip__popper')
    .html(d.name || d.id)
    .style('left', d.x + 'px')
    .style('top', d.y + 'px')
}

CodeFlower.prototype.mouseout = function (d) {
  d.size = d.size ? d.size / 10 : 4.5
  this.update()
  d3.select('#visualization').selectAll('.el-tooltip__popper').remove()
}

CodeFlower.prototype.tick = function () {
  var w = this.w
  var h = this.h
  this.link.attr('x1', function (d) { return d.source.x })
    .attr('y1', function (d) { return d.source.y })
    .attr('x2', function (d) { return d.target.x })
    .attr('y2', function (d) { return d.target.y })

  this.node.attr('cx', function (d) {
    var radius = Math.sqrt(d.size) / 10 || 4.5
    return Math.max(radius, Math.min(w - radius, d.x))
  })
    .attr('cy', function (d) {
      var radius = Math.sqrt(d.size) / 10 || 4.5
      return Math.max(radius, Math.min(h - radius, d.y))
    })
}

CodeFlower.prototype.cleanup = function () {
  this.update([])
  this.force.stop()
}
