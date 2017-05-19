import Vue from 'vue'
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

// 禁止鼠标双击选中
Vue.directive('unselect', {
  inserted: function (el, binding) {
    el.className += ' unselectable'
  }
})
