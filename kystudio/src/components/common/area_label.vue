 <template>
 <div class="area_label" :class="changeable">
  <el-select ref="select"
    v-model="selectedL"
    @remove-tag="removeTag"
    @change="change"
    style="width:100%"
    :disabled='disabled'
    multiple
    filterable
    remote
    default-first-option
    size="medium"
    :remote-method="remoteMethod"
    :allow-create='allowcreate'
    :popper-class="changeable"
    :placeholder="placeholder">
    <el-option
      v-for="(item, index) in baseLabel"
      :key="index"
      :label="item.label"
      :value="item.value" >
    </el-option>
  </el-select>
  </div>
</template>
<script>
export default {
  name: 'labelArea',
  props: ['labels', 'refreshInfo', 'selectedlabels', 'placeholder', 'changeable', 'datamap', 'disabled', 'allowcreate', 'ignoreSplitChar', 'validateRegex', 'splitChar'],
  data () {
    return {
      selectedL: this.selectedlabels,
      tags: [],
      query: '',
      validateReg: this.validateRegex
    }
  },
  computed: {
    'baseLabel' () {
      var arr = []
      var len = this.labels && this.labels.length || 0
      for (var k = 0; k < len; k++) {
        if (this.labels[k]) {
          var obj = {
            label: (this.datamap && this.datamap.label) ? this.labels[k][this.datamap.label] : this.labels[k],
            value: (this.datamap && this.datamap.value) ? this.labels[k][this.datamap.value] : this.labels[k]
          }
          arr.push(obj)
        }
      }
      this.$nextTick(() => {
        this.tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
      })
      return arr
    }
  },
  watch: {
    selectedlabels (val) {
      this.selectedL = val
    },
    // 保证组件在外部切换校验类型的时候能够动态切换校验表达式
    validateRegex (val) {
      this.validateReg = val
    }
  },
  methods: {
    remoteMethod (query) {
      this.query = query
    },
    change (e) {
      this.$nextTick(() => {
        this.$emit('change')
        this.tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
        if (this.allowcreate && e.length > 0) {
          let result = this.filterCreateTag(e[e.length - 1])
          var splitChar = this.splitChar || ';'
          var regOfSeparate = new RegExp(splitChar)
          if (!this.ignoreSplitChar && regOfSeparate.test(e[e.length - 1])) {
            if (result && result.length > 0) {
              this.selectedL.splice(this.selectedL.length - 1, 1)
              this.selectedL = this.selectedL.concat(result)
              this.selectedL = [...new Set(this.selectedL)]
            }
          }
          if (result && result.length <= 0) {
            this.selectedL.splice(this.selectedL.length - 1, 1)
          }
        }
        this.selectedL = this.selectedL.map((item) => {
          return item.toLocaleUpperCase()
        })
        this.$emit('refreshData', this.selectedL, this.refreshInfo)
        this.bindTagClick()
      })
    },
    bindTagClick () {
      this.tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
      var arealabel = this.$el.querySelectorAll('.el-select__tags > span')
      if (arealabel.length) {
        arealabel[0].onclick = (e) => {
          var ev = e || window.event
          var target = ev.target || ev.srcElement
          if (target && (target.className.indexOf('el-tag') >= 0 || target.className.indexOf('el-select__tags') || target.className.indexOf('el-select__tags-text') >= 0)) {
            if (e.stopPropagation) {
              e.stopPropagation()
            } else {
              window.event.cancelBubble = true
            }
            this.selectTag(ev)
          }
        }
      }
    },
    removeTag (data) {
      var len = this.selectedL && this.selectedL.length || 0
      for (var k = 0; k < len; k++) {
        if (this.selectedL[k] === data) {
          this.selectedL.splice(k, 1)
          break
        }
      }
      this.$emit('removeTag', data, this.refreshInfo)
    },
    selectTag (e) {
      var ev = e || window.event
      var target = ev.target || ev.srcElement
      if (target && (target.className.indexOf('el-tag') >= 0 || target.className.indexOf('el-select__tags') || target.className.indexOf('el-select__tags-text') >= 0)) {
        this.$emit('checklabel', target.innerText, target)
      }
    },
    filterCreateTag (item) {
      if (!item) {
        return []
      }
      if (this.validateReg) {
        var regExp = new RegExp(this.validateReg)
        if (!regExp.test(item)) {
          this.$emit('validateFail')
          return []
        }
      }
      // 忽略分隔符
      if (this.ignoreSplitChar) {
        return [item]
      }
      var result = []
      // 分隔符
      var splitChar = this.splitChar || ';'
      var regOfSeparate = new RegExp(splitChar)
      if (item && regOfSeparate.test(item)) {
        Array.prototype.push.apply(result, item.split(regOfSeparate))
      } else if (item) {
        result.push(item)
      }
      result = result.map((item) => {
        return item.replace(/^\s+|\s+$/g, '')
      })
      // result = result.filter((item) => {
      //   return item
      // })
      return result
    }
  },
  mounted () {
    if (this.allowcreate) {
      this.$refs.select.$refs.input.onkeydown = (ev) => {
        ev = ev || window.event
        if (ev.keyCode !== 13) {
          return
        }
        // 处理单独录入的情况 start
        if (this.allowcreate && this.query) {
          var result = this.filterCreateTag(this.query)
          if (result && result.length > 0) {
            this.selectedL = this.selectedL.concat(result)
            this.selectedL = [...new Set(this.selectedL)]
            this.selectedL = this.selectedL.map((item) => {
              return item.toLocaleUpperCase()
            })
            this.$emit('refreshData', this.selectedL, this.refreshInfo)
          }
          if (this.$refs.select.$refs.input) {
            this.$refs.select.$refs.input.value = ''
            this.$refs.select.$refs.input.click()
            setTimeout(() => {
              this.$refs.select.$refs.input.focus()
            }, 0)
          }
        }
        // 处理单独录入的情况end
      }
    }
    this.bindTagClick()
    this.$nextTick(() => {
      this.tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
      this.$emit('loadComplete', this.refreshInfo)
    })
  }
}
</script>
<style lang="less">

.unchange{
    display:none;
}
.area_label {
  overflow: hidden;
}
.area_label.unchange{
  display: block;
}
.unchange{
  .el-select-dropdown__empty{
     display:none;
  }
  .el-tag{
    cursor: pointer;
  }
  .el-select .el-input{
    .el-input__icon{
      display:none
    }
  }

}
.area_label{
  // .el-tag__close{
  //   position: absolute;
  //   right: 0;
  //   top: 2px;
  // }
  // .el-tag{
  //   max-width:100%;
  //   overflow:hidden;
  //   position: relative;
  //   padding-right: 20px;
  //   float: left;
  //   .el-select__tags-text{
  //     display: block;
  //     height: 22px;
  //     line-height: 22px;
  //   }
  // }
  .el-select__tags {
    overflow-y: auto;
    overflow-x: auto;
  }
  .el-select__input{
    float:left;
  }
  .el-select__input:after{
    content:'.';
    clear:both;
    height:0;
    visibility:hidden;
    font-size:0;
    line-height:0;
  }
}

</style>
