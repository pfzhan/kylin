<template>
  <div class="smyles_editor_wrap" :style="{width: width? width: '100%'}">
    <editor class="smyles_editor" v-model="editorData" ref="kapEditor" :height="height? height: '100%'" :lang="lang" :theme="theme" @change="changeInput" @input="changeInput"></editor>
    <div class="smyles_dragbar"></div>
    <el-popover
      placement="top"
      title=""
      trigger="click"
      v-model="showCopyStatus">
      <i class="el-icon-circle-check"></i> <span>{{$t('kylinLang.common.copySuccess')}}</span>
    </el-popover>
    <el-button size="mini" class="edit-copy-btn" plain
      :class="{'is-show': editorData}"
      v-clipboard:copy="editorData"
      v-clipboard:success="onCopy"
      v-clipboard:error="onError">
      {{$t('kylinLang.common.copy')}}
    </el-button>
  </div>
</template>
<script>
import $ from 'jquery'
import sqlFormatter from 'sql-formatter'
export default {
  name: 'kapEditor',
  props: ['height', 'lang', 'theme', 'value', 'width', 'dragbar', 'isFormatter'],
  data () {
    return {
      editorData: this.isFormatter ? sqlFormatter.format(this.value) : this.value,
      dragging: false,
      showCopyStatus: false
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
    },
    onCopy () {
      this.showCopyStatus = true
      setTimeout(() => {
        this.showCopyStatus = false
      }, 1000)
    },
    onError () {
      this.$message(this.$t('kylinLang.common.copyfail'))
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
    this.$on('setValue', (val) => {
      editor.setValue(val)
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
      this.editorData = this.isFormatter ? sqlFormatter.format(val) : val
    }
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .smyles_editor_wrap {
    position: relative;
    border: 1px solid @text-secondary-color;
    background-color: @aceditor-bg-color;
    .smyles_editor {
      // width: calc(~'100% - 50px') !important;
      border: none;
    }
    .smyles_dragbar {
      width: 100%;
      height: 1px;
      cursor: row-resize;
      opacity: 1;
      position: relative;
      bottom: -1px;
    }
    .edit-copy-btn {
      position: absolute;
      right: 5px;
      top: 5px;
      z-index: 9;
      opacity: 0.45;
      display: none;
      // background-color: rgba(255,255,255,0.2);
      &.is-show {
        display: block;
      }
    }
    &:hover {
      .edit-copy-btn {
        opacity: 1;
      }
    }
    .el-popover {
      right: 40px;
      top: 0px;
      min-width: 80px;
      background-color: transparent;
      border-color: transparent;
      box-shadow: none;
      .el-icon-circle-check {
        color: @normal-color-1;
      }
    }
  }
</style>
