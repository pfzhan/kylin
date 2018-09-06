import Vue from 'vue'
import $ from 'jquery'
import Scrollbar from 'smooth-scrollbar'
import { stopPropagation } from 'util/event'
Vue.directive('visible', {
  // 当绑定元素插入到 DOM 中。
  inserted: function (el, binding) {
    el.style.visibility = binding.value ? 'visible' : 'hidden'
  },
  update: function (el, binding) {
    el.style.visibility = binding.value ? 'visible' : 'hidden'
    if (el.tagName === 'INPUT') {
      el.focus()
      if (binding.value !== binding.oldValue) {
        el.select()
      }
    } else {
      for (var i = 0; i < el.childNodes.length; i++) {
        if (el.childNodes[i].tagName === 'INPUT') {
          el.childNodes[i].focus()
          if (binding.value !== binding.oldValue) {
            el.childNodes[i].select()
          }
        }
      }
    }
  }
})
Vue.directive('focus', {
  inserted: function (el) {
    $(document).keyup(function (event) {
      if (event.keyCode === 13) {
        el.__vue__ && el.__vue__.$emit('blur')
      }
    })
  },
  update: function (el, binding) {
    if (binding.value) {
      el.__vue__ && el.__vue__.focus()
    }
  },
  unbind: function () {
    $(document).unbind('keyup')
  }
})
Vue.directive('scroll', {
  componentUpdated: function (el, binding) {
    if (el) {
      var isReactive = binding.modifiers.reactive
      // 组件更新后强制重新初始化
      if (isReactive) {
        Scrollbar.init(el)
      } else {
        var instance = Scrollbar.get(el)
        if (!instance) {
          Scrollbar.init(el)
        }
      }
    }
  }
})
// 禁止鼠标双击选中
Vue.directive('unselect', {
  inserted: function (el, binding) {
    el.className += ' unselectable'
  }
})

Vue.directive('event-stop', {
  inserted: function (el, binding) {
    var eventName = binding.arg || 'mousedown'
    el['on' + eventName] = function (e) {
      stopPropagation(e)
    }
  }
})

Vue.directive('search-highlight', function (el, binding) {
  var searchKey = binding.value.hightlight
  var reg = new RegExp('(' + searchKey + ')', 'gi')
  var searchScope = binding.value.scope
  var direcDom = $(el)
  Vue.nextTick(() => {
    direcDom.find(searchScope).each(function () {
      var d = $(this)
      d.html(d.html().replace(/<\/?i.*?>/g, '').replace(new RegExp(reg), '<i class="keywords">$1</i>'))
    })
  })
})

Vue.directive('keyborad-select', {
  unbind: function () {
    $(document).unbind('keyup')
  },
  componentUpdated: function (el, binding) {
    var searchKey = binding.value.searchKey
    var searchScope = binding.value.scope
    var list = null
    Vue.nextTick(() => {
      list = $(el).find(searchScope)
      var index = -1
      selectList()
      if (searchKey) {
        $(document).keyup(function (event) {
          if (event.keyCode === 40 || event.keyCode === 39) {
            index = index + 1 >= list.length ? 0 : index + 1
            selectList(index)
          }
          if (event.keyCode === 37 || event.keyCode === 38) {
            index = index - 1 < 0 ? list.length - 1 : index - 1
            selectList(index)
          }
          if (event.keyCode === 13 && index >= 0) {
            list.eq(index).click()
          }
        })
      }
    })
    function selectList (i) {
      list.removeClass('active')
      if (i >= 0) {
        list.eq(i).addClass('active').focus()
      }
    }
  }
})

Vue.directive('drag', {
  inserted: function (el, binding, vnode) {
    var oDiv = el
    var dragInfo = binding.value
    var limitObj = dragInfo.limit
    var changeOption = binding.modifiers.hasOwnProperty ? binding.modifiers : {}
    var boxDom = null
    var boxW = 0
    var boxH = 0
    var callback = binding.value && binding.value.sizeChangeCb
    if (changeOption.hasOwnProperty('height') || changeOption.hasOwnProperty('width')) {
      el.className += ' ky-resize'
    } else {
      el.className += ' ky-move'
    }
    regainBox()
    // 盒子碰撞检测
    function checkBoxCollision (changeType, size) {
      if (!dragInfo.box && !limitObj) {
        return true
      }
      if (!dragInfo.box) {
        if (changeType === 'top') {
          if (!limitObj['top'] || limitObj['top'][0] <= size && size <= limitObj['top'][1]) {
            return true
          }
        }
        if (changeType === 'height') {
          if (limitObj['height'] === undefined || limitObj['height'][0] <= size && size <= limitObj['height'][1]) {
            return true
          }
        }
        if (changeType === 'left') {
          if (limitObj['left'] === undefined || limitObj['left'][0] <= size && size <= limitObj['left'][1]) {
            return true
          }
        }
        if (changeType === 'right') {
          if (limitObj['right'] === undefined || limitObj['right'][0] <= size && size <= limitObj['right'][1]) {
            return true
          }
        }
        if (changeType === 'width') {
          if (limitObj['wdith'] === undefined || limitObj['wdith'][0] <= size && size <= limitObj['wdith'][1]) {
            return true
          }
        }
        return false
      } else {
        if (changeType === 'top') {
          if (size + dragInfo.height > boxH) {
            dragInfo.top = boxH - dragInfo.height
            return false
          }
          if (size < 0) {
            dragInfo.top = 0
            return false
          }
        }
        if (changeType === 'height') {
          if (size + dragInfo.top > boxH) {
            dragInfo.top = boxH - dragInfo.height
            return false
          }
          if (size < dragInfo.minheight) {
            dragInfo.height = dragInfo.minheight
            return false
          }
        }
        if (changeType === 'right' || changeType === 'left') {
          if (size + dragInfo.width > boxW) {
            dragInfo[changeType] = boxW - dragInfo.width
            return false
          }
          if (size < 0) {
            dragInfo[changeType] = 0
            return false
          }
        }
        return true
      }
    }
    function regainBox () {
      if (dragInfo.box) {
        boxDom = $(el).parents(dragInfo.box).eq(0)
        boxW = boxDom.width()
        boxH = boxDom.height()
      }
    }
    $(window).resize(() => {
      setTimeout(() => {
        regainBox()
        if (checkBoxCollision()) {
          return
        }
        if (dragInfo['right'] || dragInfo['right'] < 0) {
          if (dragInfo['right'] + dragInfo.width > boxW) {
            dragInfo['right'] = 0
          }
          if (dragInfo['right'] < 0) {
            dragInfo['right'] = boxW - dragInfo.width
          }
        }
        if (dragInfo['left'] && dragInfo['left'] + dragInfo.width > boxW) {
          if (dragInfo['left'] + dragInfo.width > boxW) {
            dragInfo['left'] = boxW - dragInfo.width
          }
          if (dragInfo['left'] < 0) {
            dragInfo['left'] = 0
          }
        }
        if (dragInfo['top'] && dragInfo['top'] + dragInfo.height > boxH) {
          if (dragInfo['top'] + dragInfo.height > boxH) {
            dragInfo['top'] = boxH - dragInfo.height
          }
          if (dragInfo['top'] < 0) {
            dragInfo['top'] = 0
          }
        }
        callback && callback()
      }, 2)
    })
    oDiv.onmousedown = function (ev) {
      ev.stopPropagation()
      var offsetX = ev.clientX
      var offsetY = ev.clientY
      regainBox()
      if (changeOption.hasOwnProperty('height') || changeOption.hasOwnProperty('width')) {
        el.className += ' ky-resize-ing'
      } else {
        el.className += ' ky-move-ing'
      }
      document.onmousemove = function (e) {
        var x = e.clientX - offsetX
        var y = e.clientY - offsetY
        offsetX = e.clientX
        offsetY = e.clientY
        if (changeOption) {
          for (var i in changeOption) {
            if (i === 'top') {
              if (checkBoxCollision(i, dragInfo['top'] + y)) {
                dragInfo['top'] += y
              }
            }
            if (i === 'right') {
              if (checkBoxCollision(i, dragInfo['right'] - x)) {
                dragInfo['right'] -= x
              }
            }
            if (i === 'left') {
              if (checkBoxCollision(i, dragInfo['left'] + x)) {
                dragInfo['left'] += x
              }
            }
            if (i === 'height') {
              if (checkBoxCollision(i, dragInfo['height'] + y)) {
                dragInfo['height'] += y
              }
            }
            if (i === 'width') {
              if (checkBoxCollision(i, dragInfo['wdith'] + x)) {
                dragInfo['wdith'] += x
              }
            }
          }
        }
        callback && callback(x, y)
      }
      document.onmouseup = function () {
        el.className = el.className.replace(/ky-[a-z]+-ing/g, '').replace(/\s+$/, '')
        document.onmousemove = null
        document.onmouseup = null
        // $(window).unbind('resize')
      }
    }
  }
})
