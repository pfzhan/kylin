 <template>
 <div class="area_label" :class="changeable">
  <el-select ref="select"
    v-model="selectedL"
    @remove-tag="removeTag"
    @change="change"
    style="width:100%"
    :disabled='disabled'
    multiple
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
  props: ['labels', 'refreshInfo', 'selectedlabels', 'placeholder', 'changeable', 'datamap', 'disabled'],
  data () {
    return {
      selectedL: this.selectedlabels
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
      // console.log(e, 9999)
      // var ev = ev || window.event
      // var target = ev.target || ev.srcElement
      this.$emit('change')
      this.$emit('refreshData', this.selectedL, this.refreshInfo)
      // this.refreshData = this.selectedL
      // Object.assign(this.refreshData, [], this.selectedL)
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
      if (target && (target.className.indexOf('el-tag') >= 0 || target.className.indexOf('el-select__tags-text') >= 0)) {
        this.$emit('checklabel', target.innerText, target)
      }
    }
  },
  mounted () {
    var _this = this
    this.$refs.select.$refs.tags.onclick = function (e) {
      _this.selectTag(e)
      if (e && e.stopPropagation) {
      // W3C取消冒泡事件
        e.stopPropagation()
      } else {
      // IE取消冒泡事件
        window.event.cancelBubble = true
      }
    }
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
