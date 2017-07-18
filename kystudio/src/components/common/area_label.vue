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
  props: ['labels', 'refreshInfo', 'selectedlabels', 'placeholder', 'changeable', 'datamap', 'disabled', 'allowcreate'],
  data () {
    return {
      selectedL: this.selectedlabels,
      tags: []
    }
  },
  computed: {
    'baseLabel' () {
      var arr = []
      for (var k = 0; this.labels && k < this.labels.length || 0; k++) {
        if (this.labels[k]) {
          var obj = {
            label: (this.datamap && this.datamap.label) ? this.labels[k][this.datamap.label] : this.labels[k],
            value: (this.datamap && this.datamap.value) ? this.labels[k][this.datamap.value] : this.labels[k]
          }
          arr.push(obj)
        }
      }
      return arr
    }
  },
  watch: {
    selectedlabels (val) {
      this.selectedL = val
    }
  },
  methods: {
    change (e) {
      this.$nextTick(() => {
        this.tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
        this.$emit('change')
        // 处理单独录入的情况 start
        if (this.allowcreate) {
          var result = []
          // 分隔符
          var regOfSeparate = /[,;$|]/
          var refOfAllowChar = /[^\w.,;$|]/g
          // 只有分隔符
          var regJustSeparate = /(^[,;$|]+$)|([,;$|]+$)|(^[,;$|]+)/g
          var needRefresh = false
          e.forEach((item) => {
            item = item.replace(refOfAllowChar, '').replace(regJustSeparate, '')
            if (item && regOfSeparate.test(item)) {
              needRefresh = true
              Array.prototype.push.apply(result, item.split(regOfSeparate))
            } else if (item) {
              result.push(item)
            } else {
              needRefresh = true
            }
          })
          // 添加默认的datasource
          result = result.map((table) => {
            if (!/^\w+\.\w+$/.test(table)) {
              needRefresh = true
              return 'default.' + table
            }
            return table
          })
          if (needRefresh) {
            this.selectedL = result
          }
        }
        // 处理单独录入的情况end
        this.$emit('refreshData', this.selectedL, this.refreshInfo)
        this.bindTagClick()
      })
    },
    bindTagClick () {
      this.tags = Array.prototype.slice.call(this.$el.querySelectorAll('.el-tag'))
      this.tags.forEach(tag => {
        // tag.removeEventListener('click', e => {
        //   e.stopPropagation()
        //   this.selectTag(e)
        // })
        tag.addEventListener('click', e => {
          e.stopPropagation()
          this.selectTag(e)
        })
      })
    },
    removeTag (data) {
      for (var k = 0; k < (this.selectedL && this.selectedL.length || 0); k++) {
        if (this.selectedL[k] === data.value) {
          this.selectedL.splice(k, 1)
          break
        }
      }
      this.$emit('removeTag', data.value)
    },
    selectTag (e) {
      var ev = ev || window.event
      var target = ev.target || ev.srcElement
      if (target && (target.className.indexOf('el-tag') >= 0 || target.className.indexOf('el-select__tags') || target.className.indexOf('el-select__tags-text') >= 0)) {
        this.$emit('checklabel', target.innerText, target)
      }
    }
  },
  mounted () {
    this.bindTagClick()
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
  .el-tag__close{
    position: absolute;
    right: 0;
    top: 2px;
  }
  .el-tag{
    max-width:100%;
    overflow:hidden;
    position: relative;
    padding-right: 20px;
  }
}

</style>
