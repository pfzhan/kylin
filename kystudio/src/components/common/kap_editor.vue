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
    },
    setOption (option) {
      var editor = this.$refs.kapEditor.editor
      editor.setOptions(Object.assign({
        wrap: 'free',
        enableBasicAutocompletion: true,
        enableSnippets: true,
        enableLiveAutocompletion: true
      }, option))
    },
    getValue () {
      var editor = this.$refs.kapEditor.editor
      return editor.getValue()
    }
  },
  mounted () {
    var editor = this.$refs.kapEditor.editor
    // editor.setOption('wrap', 'free')
    var editorWrap = this.$el
    var smylesEditor = this.$el.querySelector('.smyles_editor')
    this.$on('setReadOnly', (isReadyOnly) => {
      editor.setReadOnly(true)
    })
    this.setOption()
    this.$on('setOption', (option) => {
      this.setOption(option)
    })
    this.$on('focus', () => {
      editor.focus()
    })
    this.$on('insert', (val) => {
      editor.insert(val)
    })
    this.$on('setAutoCompleteData', (autoCompleteData) => {
      editor.completers.splice(0, editor.completers.length - 3)
      editor.completers.unshift({
        identifierRegexps: [/[.a-zA-Z_0-9]/],
        getCompletions (editor, session, pos, prefix, callback) {
          if (prefix.length === 0) {
            return callback(null, autoCompleteData)
          } else {
            return callback(null, autoCompleteData)
          }
        }
      })
      editor.commands.on('afterExec', function (e, t) {
        if (e.command.name === 'insertstring' && (e.args === ' ' || e.args === '.')) {
          var all = e.editor.completers
          // e.editor.completers = completers;
          e.editor.execCommand('startAutocomplete')
          e.editor.completers = all
        }
      })
    })
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
    .smyles_dragbar {
      width: 100%;
      height: 4px;
      cursor: row-resize;
      opacity: 1;
    }
  }
</style>
