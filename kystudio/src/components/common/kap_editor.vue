<template>
  <div class="smyles_editor_wrap" :style="{width: width? width: '100%'}">
    <editor class="smyles_editor" v-model="editorData" ref="kapEditor" :height="height? height: '100%'" width="100%" :lang="lang" :theme="theme" @change="changeInput" @input="changeInput"></editor>
    <div class="smyles_dragbar"></div>
  </div>
</template>
<script>
import $ from 'jquery'
export default {
  name: 'kap_editor',
  props: ['height', 'lang', 'theme', 'value', 'width', 'dragbar'],
  data () {
    return {
      editorData: this.value,
      dragging: false
    }
  },
  methods: {
    changeInput () {
      this.$emit('input', this.editorData)
    }
  },
  mounted () {
    var editor = this.$refs.kapEditor.editor
    editor.setOption('wrap', 'free')
    var editorWrap = this.$el
    var smylesEditor = this.$el.querySelector('.smyles_editor')
    this.$el.querySelector('.smyles_dragbar').onmousedown = (e) => {
      e.preventDefault()
      this.dragging = true
      var oldTop = 0
      var topOffset = $(smylesEditor).offset().top
      // handle mouse movement
      $(document).mousemove((e) => {
        if (e.pageY - oldTop > 4 || oldTop - e.pageY > 4) {
          oldTop = e.pageY
          var eheight = e.pageY - topOffset
          // Set wrapper height
          editorWrap.style.height = eheight + 'px'
          smylesEditor.style.height = eheight + 'px'
          editor.resize()
        }
      })
    }
    $(document).mouseup((e) => {
      if (this.dragging) {
        $(document).unbind('mousemove')
        // Trigger ace editor resize()
        editor.resize()
        this.dragging = false
      }
    })
  },
  destroyed () {
    $(document).unbind('mouseup')
    $(document).unbind('mousemove')
  },
  watch: {
    value (val) {
      this.editorData = val
    }
  }
}
</script>
<style lang="less">
  .smyles_editor_wrap {
    // background-color: #272822;
    // .smyles_editor {
    //   .ace_gutter {
    //     .ace_layer {
    //       background: #393e53;
    //     }
    //   }
    // }
    .smyles_dragbar {
      width: 100%;
      height: 4px;
      cursor: row-resize;
      opacity: 1;
    }
  }
</style>
