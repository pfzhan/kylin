
 import $ from 'jquery'
 $.Fly = function (element, options) {
  // 默认值
   var defaults = {
     version: '1.0.0',
     autoPlay: true,
     vertex_Rtop: 120, // 默认顶点高度top值
     speed: 1.2,
     start: {}, // top, left, width, height
     end: {},
     onEnd: $.noop
   }
   var self = this
   var $element = $(element)

  /**
   * 初始化组件，new的时候即调用
   */
   self.init = function (options) {
     this.setOptions(options)
     !!this.settings.autoPlay && this.play()
   }

  /**
   * 设置组件参数
   */
   self.setOptions = function (options) {
     this.settings = $.extend(true, {}, defaults, options)
     let settings = this.settings
     let start = settings.start
     let end = settings.end
     $element.css({marginTop: '0px', marginLeft: '0px', position: 'fixed'}).appendTo('body')
    // 运动过程中有改变大小
     if (end.width != null && end.height != null) {
       $.extend(true, start, {
         width: $element.width(),
         height: $element.height()
       })
     }
    // 运动轨迹最高点top值
     var vertexTop = Math.min(start.top, end.top) - Math.abs(start.left - end.left) / 3
     if (vertexTop < settings.vertex_Rtop) {
      // 可能出现起点或者终点就是运动曲线顶点的情况
       vertexTop = Math.min(settings.vertex_Rtop, Math.min(start.top, end.top))
     }

    /**
     * ======================================================
     * 运动轨迹在页面中的top值可以抽象成函数 y = a * x*x + b
     * a = curvature
     * b =  vertexTop
     * ======================================================
     */

     var distance = Math.sqrt(Math.pow(start.top - end.top, 2) + Math.pow(start.left - end.left, 2))
      // 元素移动次数
     let steps = Math.ceil(Math.min(Math.max(Math.log(distance) / 0.05 - 75, 30), 100) / settings.speed)
     let ratio = start.top === vertexTop ? 0 : -Math.sqrt((end.top - vertexTop) / (start.top - vertexTop))
     let vertexLeft = (ratio * start.left - end.left) / (ratio - 1)
      // 特殊情况，出现顶点left==终点left，将曲率设置为0，做直线运动。
     let curvature = end.left === vertexLeft ? 0 : (end.top - vertexTop) / Math.pow(end.left - vertexLeft, 2)

     $.extend(true, settings, {
       count: -1, // 每次重置为-1
       steps: steps,
       vertexLeft: vertexLeft,
       vertexTop: vertexTop,
       curvature: curvature
     })
   }

  /**
   * 开始运动，可自己调用
   */
   self.play = function () {
     this.move()
   }

  /**
   * 按step运动
   */
   self.move = function () {
     var settings = this.settings
     var start = settings.start
     var count = settings.count
     var steps = settings.steps
     var end = settings.end
    // 计算left top值
     var left = start.left + (end.left - start.left) * count / steps
     var top = settings.curvature === 0 ? start.top + (end.top - start.top) * count / steps : settings.curvature * Math.pow(left - settings.vertexLeft, 2) + settings.vertexTop
    // 运动过程中有改变大小
     if (end.width != null && end.height != null) {
       var i = steps / 2
       var width = end.width - (end.width - start.width) * Math.cos(count < i ? 0 : (count - i) / (steps - i) * Math.PI / 2)
       var height = end.height - (end.height - start.height) * Math.cos(count < i ? 0 : (count - i) / (steps - i) * Math.PI / 2)
       $element.css({width: width + 'px', height: height + 'px', 'font-size': Math.min(width, height) + 'px'})
     }
     $element.css({
       left: left + 'px',
       top: top + 'px'
     })
     settings.count++
    // 定时任务
     var time = window.requestAnimationFrame($.proxy(this.move, this))
     if (count === steps) {
       window.cancelAnimationFrame(time)
      // fire callback
       settings.onEnd.apply(this)
     }
   }

  /**
   * 销毁
   */
   self.destroy = function () {
     $element.remove()
   }

   self.init(options)
 }

// add the plugin to the jQuery.fn object
 $.fn.fly = function (options) {
   return this.each(function () {
     if (undefined === $(this).data('fly')) {
       $(this).data('fly', new $.Fly(this, options))
     }
   })
 }
