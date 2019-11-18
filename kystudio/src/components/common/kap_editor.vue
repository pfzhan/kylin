<template>
  <div class="smyles_editor_wrap" :style="editorStyle">
    <template v-if="!isAbridge">
      <editor class="smyles_editor" v-model="editorData" ref="kapEditor" :style="{height: editorStyle.height}" :lang="lang" :theme="theme" @change="changeInput" @input="changeInput"></editor>
    </template>
    <template v-else>
      <editor class="smyles_editor" v-model="formatData" ref="kapEditor" :style="{height: editorStyle.height}" :lang="lang" :theme="theme" @change="changeInput" @input="changeInput"></editor>
    </template>
    <div class="smyles_dragbar" v-if="dragable" v-drag:change.height="editorDragData"></div>
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
import { sqlRowsLimit } from '../../config/index'
import { mapState } from 'vuex'
export default {
  name: 'kapEditor',
  props: {
    height: {
      default: 0
    },
    lang: {
      default: ''
    },
    theme: {
      default: ''
    },
    value: {
      default: ''
    },
    width: {
      default: '100%'
    },
    dragable: {
      default: true
    },
    isFormatter: {
      default: false
    },
    readOnly: {
      default: false
    },
    isAbridge: {
      type: Boolean,
      default: false
    }
  },
  data () {
    return {
      editorData: this.isFormatter ? sqlFormatter.format(this.value) : this.value,
      formatData: '',
      dragging: false,
      showCopyStatus: false,
      editorDragData: {
        height: +this.height || 0,
        width: this.width
      }
    }
  },
  computed: {
    ...mapState({
      systemLang: state => state.system.lang
    }),
    editorStyle: function () {
      return {
        height: this.editorDragData.height ? this.editorDragData.height + 'px' : '100%',
        width: this.editorDragData.width ? this.editorDragData.width : '100%'
      }
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
    },
    // 截取前100行sql
    abridgeData () {
      const data = this.editorData.split('\n')
      this.formatData = data.length > sqlRowsLimit ? [...data.slice(0, sqlRowsLimit), `-- ${this.$t('kylinLang.common.sqlLimitTip')}`].join('\n') : this.editorData
    },
    getAbridgeType () {
      this.isAbridge && this.abridgeData()
    }
  },
  mounted () {
    var editor = this.$refs.kapEditor.editor
    // editor.setOption('wrap', 'free')
    // var editorWrap = this.$el
    // var smylesEditor = this.$el.querySelector('.smyles_editor')
    if (this.readOnly) {
      editor.setReadOnly(this.readOnly)
    }
    this.$on('setReadOnly', (isReadyOnly) => {
      editor.setReadOnly(isReadyOnly)
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
    this.getAbridgeType()

    // this.$el.querySelector('.smyles_dragbar').onmousedown = (e) => {
    //   e.preventDefault()
    //   this.dragging = true
    //   var oldTop = 0
    //   var topOffset = $(smylesEditor).offset().top
    //   // handle mouse movement
    //   $(document).mousemove((e) => {
    //     if (e.pageY - oldTop > 4 || oldTop - e.pageY > 4) {
    //       oldTop = e.pageY
    //       var eheight = e.pageY - topOffset
    //       // Set wrapper height
    //       editorWrap.style.height = eheight + 'px'
    //       smylesEditor.style.height = eheight + 'px'
    //       editor.resize()
    //     }
    //   })
    // }
    // $(document).mouseup((e) => {
    //   if (this.dragging) {
    //     $(document).unbind('mousemove')
    //     // Trigger ace editor resize()
    //     editor.resize()
    //     this.dragging = false
    //   }
    // })
  },
  destroyed () {
    $(document).unbind('mouseup')
    $(document).unbind('mousemove')
  },
  watch: {
    value (val) {
      this.editorData = this.isFormatter ? sqlFormatter.format(val) : val
      this.getAbridgeType()
    },
    readOnly (val) {
      if (this.$refs.kapEditor.editor) {
        this.$refs.kapEditor.editor.setReadOnly(val)
      }
    },
    'editorDragData.height' (val) {
      if (val) {
        var editor = this.$refs.kapEditor.editor
        editor.resize()
      }
    },
    systemLang () {
      this.isAbridge && this.abridgeData()
    }
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  .smyles_editor_wrap {
    width: 100%;
    position: relative;
    border: 1px solid @line-border-color;
    background-color: @aceditor-bg-color;
    .ace_print-margin {
      visibility: hidden !important;
    }
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
      opacity: 0;
      display: none;
      width: 48px;
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
      right: 46px;
      top: 0px;
      min-width: 80px;
      text-align: right;
      background-color: transparent;
      border-color: transparent;
      box-shadow: none;
      .el-icon-circle-check {
        color: @normal-color-1;
      }
    }
  }
</style>
