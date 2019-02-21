import Vue from 'vue'
import $ from 'jquery'
import Scrollbar from 'smooth-scrollbar'
import store from '../store'
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
Vue.directive('number', {
  update: function (el, binding, vnode) {
    if (binding.value !== binding.oldValue) {
      setTimeout(() => {
        if (binding.value) {
          let newVal = ('' + binding.value).replace(/[^\d]/g, '')
          el.__vue__.$emit('input', +newVal || '0')
        }
      }, 0)
    }
  }
})
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
Vue.directive('timerHide', {
  inserted: function (el, binding) {
    let arg = binding.arg || 1
    let expression = binding.expression
    setTimeout(() => {
      el.style.display = 'none'
      if (typeof expression === 'function') {
        expression()
      }
    }, arg * 1000)
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
// 只适用kyligence-ui里带focus method的组件和原生dom元素
Vue.directive('focus', {
  inserted: function (el) {
    el.__vue__ ? el.__vue__.focus() : el.focus()
  },
  update: function (el, binding) {
    if (binding.value && !binding.oldValue) {
      el.__vue__ ? el.__vue__.focus() : el.focus()
    }
  }
})

Vue.directive('scroll', {
  inserted: function (el, binding, vnode) {
    if (el) {
      let scrollbar = Scrollbar.init(el, {continuousScrolling: true})
      let needObserve = binding.modifiers.observe
      if (needObserve) {
        scrollbar.addListener((status) => {
          if (status.offset.y > status.limit.y - 10) {
            let scrollBottomFunc = vnode.data.on['scroll-bottom']
            scrollBottomFunc && scrollBottomFunc()
          }
        })
      }
    }
  },
  update: function (el, binding) {
    var isReactive = binding.modifiers.reactive
    if (isReactive) {
      let scrollBar = Scrollbar.get(el)
      scrollBar.update(true)
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
let st = null // 定时器全局变量
Vue.directive('keyborad-select', {
  unbind: function () {
    $(document).unbind('keyup')
    clearInterval(st)
  },
  componentUpdated: function (el, binding) {
    var searchScope = binding.value.scope
    list = $(el).find(searchScope)
    scrollInstance = Scrollbar.get(el)
  },
  inserted: function (el, binding) {
    var index = -1
    selectList()
    let handleKey = (event) => {
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
    }
    $(document).keydown((event) => {
      handleKey(event)
      clearInterval(st)
      st = setInterval(() => {
        handleKey(event)
      }, 300)
    })
    $(document).keyup(function (event) {
      clearInterval(st)
    })
    function selectList (i) {
      if (!list) {
        return
      }
      list.removeClass('active')
      if (i >= 0) {
        if (scrollInstance) {
          scrollInstance.scrollTo(100, 32 * i + 5, 400)
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
    let reverse = changeOption.hasOwnProperty('reverse')
    let reverseW = changeOption.hasOwnProperty('reverseW')
    let reverseH = changeOption.hasOwnProperty('reverseH')
    if (changeOption.hasOwnProperty('height') || changeOption.hasOwnProperty('width')) {
      el.className += ' ky-resize'
    } else {
      el.className += ' ky-move'
    }
    regainBox()
    // 盒子碰撞检测
    function checkBoxCollision (changeType, size, rectifyVal) {
      rectifyVal = rectifyVal || 0
      // 无自定义盒子和限制
      if (!dragInfo.box && !limitObj && dragInfo.ignoreEdgeCheck) {
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
      if (dragInfo.box && !dragInfo.ignoreEdgeCheck) {
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
          if (dragInfo.top + rectifyVal < 0) {
            return false
          }
          if (rectifyVal + size + dragInfo.top > boxH) {
            dragInfo.height = boxH - dragInfo.top
            return false
          }
        }
        if (changeType === 'width') {
          if (!isNaN(dragInfo.left)) {
            if (dragInfo.left + rectifyVal < 0) {
              return false
            }
            if (rectifyVal + size + dragInfo.left > boxW) {
              dragInfo.left = boxW - dragInfo.width
              return false
            }
          }
          if (!isNaN(dragInfo.right)) {
            if (dragInfo.right + rectifyVal < 0) {
              return false
            }
            if (size + dragInfo.right - rectifyVal > boxW) {
              dragInfo.width = boxW - dragInfo.right
              return false
            }
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
        if (!isNaN(dragInfo['right']) || dragInfo['right'] < 0) {
          if (dragInfo['right'] + dragInfo.width > boxW) {
            dragInfo['right'] = 0
          }
          if (dragInfo['right'] < 0) {
            dragInfo['right'] = boxW - dragInfo.width
          }
        }
        if (!isNaN(dragInfo['left']) && dragInfo['left'] + dragInfo.width > boxW) {
          if (dragInfo['left'] + dragInfo.width > boxW) {
            dragInfo['left'] = boxW - dragInfo.width
          }
          if (dragInfo['left'] < 0) {
            dragInfo['left'] = 0
          }
        }
        if (!isNaN(dragInfo['top']) && dragInfo['top'] + dragInfo.height > boxH) {
          if (dragInfo['top'] + dragInfo.height > boxH) {
            dragInfo['top'] = boxH - dragInfo.height
          }
          if (dragInfo['top'] < 0) {
            dragInfo['top'] = 0
          }
        }
        callback && callback(0, 0, boxW, boxH, dragInfo)
      }, 2)
    })
    oDiv.onmousedown = function (ev) {
      let zoom = el.getAttribute('data-zoom') || 10
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
        x /= zoom / 10
        y /= zoom / 10
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
              if (reverse || reverseH) {
                if (checkBoxCollision(i, dragInfo['height'] - y, y)) {
                  dragInfo['height'] -= y
                  dragInfo['top'] += y
                }
              } else {
                if (checkBoxCollision(i, dragInfo['height'] + y)) {
                  dragInfo['height'] += y
                }
              }
            }
            if (i === 'width') {
              if (reverse || reverseW) {
                let rectify = 0
                if (!isNaN(dragInfo['left'])) {
                  rectify = x
                }
                if (checkBoxCollision(i, dragInfo['width'] - x, rectify)) {
                  dragInfo['width'] -= x
                  if (!isNaN(dragInfo['left'])) {
                    dragInfo['left'] += x
                  }
                }
              } else {
                let rectify = 0
                if (dragInfo['right']) {
                  rectify = x
                }
                if (checkBoxCollision(i, dragInfo['width'] + x, rectify)) {
                  dragInfo['width'] += x
                  if (!isNaN(dragInfo['right'])) {
                    dragInfo['right'] -= x
                  }
                }
              }
            }
          }
        }
        callback && callback(x, y, boxW, boxH, dragInfo)
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
// 收集guide dom
Vue.directive('guide', {
  // bind: function (el, binding, vnode) {
  //   console.log('----', vnode.key, vnode)
  //   // 设置alone参数 避免虚拟dom重用导致指令生命周期错误
  //   let keys = binding.modifiers
  //   if (binding.arg === 'alone') {
  //     vnode.key = Object.keys(keys).join('-')
  //   }
  // },
  inserted: function (el, binding, vnode) {
    let keys = binding.modifiers
    let storeGuide = store.state.system.guideConfig.targetList
    if (storeGuide) {
      for (let i in keys) {
        storeGuide[i] = el.__vue__ || el
      }
      if (store.state.system.guideConfig.globalMaskVisible && binding.value) {
        storeGuide[binding.value] = el.__vue__ || el
      }
    }
  },
  unbind: function (el, binding) {
    let keys = binding.modifiers
    let storeGuide = store.state.system.guideConfig.targetList
    for (let i in keys) {
      delete storeGuide[i]
    }
  }
})
let keyCodeMap = {
  'esc': 27
}
// 为非聚焦的元素绑定keyup监听
Vue.directive('global-key-event', {
  inserted: function (el, binding) {
    let keys = binding.modifiers
    let keyCodes = []
    Object.keys(keys).forEach((key) => {
      keyCodes.push(keyCodeMap[key])
    })
    document.onkeydown = (e) => {
      var key = (e || window.event).keyCode
      if (keyCodes.indexOf(key) >= 0) {
        typeof binding.value === 'function' && binding.value()
      }
    }
  },
  unbind: function (el, binding) {
    document.onkeydown = null
  }
})

