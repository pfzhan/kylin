 <template>
 <div class="area_label" :class="changeable">
  <el-select ref="select"
    v-model="selectedL"
    @remove-tag="removeTag"
    @change="change"
    style="width:100%"
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
  props: ['labels', 'selectedlabels', 'placeholder', 'changeable', 'datamap'],
  data () {
    return {
      selectedL: this.selectedlabels
    }
  },
  computed: {
    'baseLabel' () {
      var arr = []
      for (var k = 0; this.labels && k < this.labels.length || 0; k++) {
        var obj = {
          label: (this.datamap && this.datamap.label) ? this.labels[k][this.datamap.label] : this.labels[k],
          value: (this.datamap && this.datamap.value) ? this.labels[k][this.datamap.value] : this.labels[k]
        }
        arr.push(obj)
      }
      return arr
    }
  },
  methods: {
    change (e) {
      console.log(this.selectedlabels, 'seee')
      var ev = ev || window.event
      var target = ev.target || ev.srcElement
      this.$emit('change', target.innerText, target)
    },
    removeTag (data) {
      for (var k = 0; k < (this.selectedlabels && this.selectedlabels.length || 0); k++) {
        if (this.selectedlabels[k] === data.value) {
          this.selectedlabels.splice(k, 1)
          break
        }
      }
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

</style>
