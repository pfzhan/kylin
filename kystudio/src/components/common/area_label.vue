 <template>
 <div class="area_label" :class="changeable">
  <el-select ref="select"
    v-model="selectedlabels"
    @remove-tag="removeTag"
    @change="change"
    style="width:100%"
    multiple
    :popper-class="changeable"
    :placeholder="placeholder">
    <el-option
      v-for="item in baseLabel"
      :label="item.label"
      :value="item.value" >
    </el-option>
  </el-select>
  </div>
</template>
<script>
export default {
  name: 'labelArea',
  props: ['labels', 'selectedlabels', 'placeholder', 'changeable', 'datamap'],
  computed: {
    'baseLabel' () {
      var arr = []
      for (var k = 0; this.labels && k < this.labels.length || 0; k++) {
        var obj = {
          label: (this.datamap && this.datamap.label) ? this.labels[k][this.datamap.label] : this.labels[k].label,
          value: (this.datamap && this.datamap.value) ? this.labels[k][this.datamap.value] : this.labels[k].value
        }
        arr.push(obj)
      }
      console.log(arr, 'arr')
      return arr
    }
  },
  methods: {
    change (e) {
      var ev = ev || window.event
      var target = ev.target || ev.srcElement
      this.$emit('change', target.innerText, target)
    },
    removeTag (data) {
      for (var k = 0; this.selectedlabels && k < this.selectedlabels.length || 0; k++) {
        if (this.selectedlabels[k] === data.value) {
          this.selectedlabels.splice(k, 1)
          break
        }
      }
    },
    selectTag (e) {
      var ev = ev || window.event
      var target = ev.target || ev.srcElement
      if (target.className.indexOf('el-tag') >= 0 || target.className.indexOf('el-select__tags-text') >= 0) {
        this.$emit('checklabel', target.innerText, target)
      }
    }
  },
  mounted () {
    var _this = this
    this.$refs.select.$refs.tags.onclick = function (e) {
      _this.selectTag(e)
    }
  }
}
</script>
<style lang="less">

.unchange{
    display:none;
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

</style>
