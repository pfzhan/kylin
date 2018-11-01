import Vue from 'vue'
import $ from 'jquery'
import Scrollbar from 'smooth-scrollbar'
import { stopPropagation, on } from 'util/event'
const nodeList = []
const ctx = '@@clickoutsideContext'

let startClick
let seed = 0
!Vue.prototype.$isServer && on(document, 'mousedown', e => (startClick = e))

!Vue.prototype.$isServer && on(document, 'mouseup', e => {
  nodeList.forEach(node => node[ctx].documentHandler(e, startClick))
})

function createDocumentHandler (el, binding, vnode) {
  return function (mouseup = {}, mousedown = {}) {
    if (!vnode ||
      !vnode.context ||
      !mouseup.target ||
      !mousedown.target ||
      el.contains(mouseup.target) ||
      el.contains(mousedown.target) ||
      el === mouseup.target ||
      (vnode.context.popperElm &&
      (vnode.context.popperElm.contains(mouseup.target) ||
      vnode.context.popperElm.contains(mousedown.target)))) return

    if (binding.expression &&
      el[ctx].methodName &&
      vnode.context[el[ctx].methodName]) {
      vnode.context[el[ctx].methodName]()
    } else {
      el[ctx].bindingFn && el[ctx].bindingFn()
    }
  }
}
Vue.directive('clickoutside', {
  bind (el, binding, vnode) {
    nodeList.push(el)
    const id = seed++
    el[ctx] = {
      id,
      documentHandler: createDocumentHandler(el, binding, vnode),
      methodName: binding.expression,
      bindingFn: binding.value
    }
  },

  update (el, binding, vnode) {
    el[ctx].documentHandler = createDocumentHandler(el, binding, vnode)
    el[ctx].methodName = binding.expression
    el[ctx].bindingFn = binding.value
  },

  unbind (el) {
    let len = nodeList.length

    for (let i = 0; i < len; i++) {
      if (nodeList[i][ctx].id === el[ctx].id) {
        nodeList.splice(i, 1)
        break
      }
    }
    delete el[ctx]
  }
})
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
  inserted: function (el, binding) {
    if (el) {
      Scrollbar.init(el)
    }
  },
  update: function (el, binding) {
    var isReactive = binding.modifiers.reactive
    if (isReactive) {
      Scrollbar.update(true)
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
var list = null
var scrollInstance = null
Vue.directive('keyborad-select', {
  unbind: function () {
    $(document).unbind('keyup')
  },
  componentUpdated: function (el, binding) {
    var searchScope = binding.value.scope
    list = $(el).find(searchScope)
    scrollInstance = Scrollbar.get(el)
  },
  inserted: function (el, binding) {
    var index = -1
    selectList()
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
    function selectList (i) {
      if (!list) {
        return
      }
      list.removeClass('active')
      if (i >= 0) {
        let height = $(el).height()
        if (scrollInstance) {
          scrollInstance.scrollTo(100, height / list.length * i, 400)
        }
        list.eq(i).addClass('active')
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
      // 无自定义盒子和限制
      if (!dragInfo.box && !limitObj) {
        return true
      }
      if (limitObj) {
        let curCheckProp = limitObj[changeType]
        if (curCheckProp) {
          // 无自定义限制
          if (curCheckProp.length > 0 && undefined !== curCheckProp[0] && size < curCheckProp[0]) {
            dragInfo[changeType] = curCheckProp[0]
            return false
          }
          if (curCheckProp.length > 1 && undefined !== curCheckProp[1] && size > curCheckProp[1]) {
            dragInfo[changeType] = curCheckProp[1]
            return false
          }
        }
      }
      if (dragInfo.box) {
        if (changeType === 'top') {
          if (size + dragInfo.height > boxH) {
            dragInfo.top = boxH - dragInfo.height > 0 ? boxH - dragInfo.height : 0
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
        }
        if (changeType === 'right' || changeType === 'left') {
          if (size + dragInfo.width > boxW) {
            dragInfo[changeType] = boxW - dragInfo.width > 0 ? boxW - dragInfo.width : 0
            return false
          }
          if (size < 0) {
            dragInfo[changeType] = 0
            return false
          }
        }
      }
      return true
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
        $(window).unbind('resize')
      }
    }
  }
})
