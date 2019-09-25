<template>
  <el-select
    ref="selecter"
    v-model="filterVal"
    filterable
    remote
    size="medium"
    :placeholder="$t(placeholder)"
    @change="changeSelect"
    @handleOptionClick = "clickSelect"
    :disabled="disabled"
    :multiple="multiple"
    :popper-append-to-body="false"
    :loading="loading">
    <el-option
      v-for="item in drawData"
      :key="item[datamap['value']]"
      :label="item[datamap['label']]"
      :value="item[datamap['value']]">
      <slot name="select" :item="item">
      </slot>
    </el-option>
  </el-select>
</template>
<script>
export default {
  name: 'filterSelect',
  props: ['list', 'size', 'placeholder', 'dataMap', 'value', 'disabled', 'asyn', 'delay', 'multiple'],
  data () {
    return {
      loading: false,
      datamap: this.dataMap ? this.dataMap : {label: 'label', value: 'value'},
      filterList: [],
      filterVal: this.value,
      ST: null
    }
  },
  methods: {
    remoteMethod (query) {
      this.filterVal = query
      this.loading = true
      if (this.asyn) {
        clearTimeout(this.ST)
        this.ST = setTimeout(() => {
          this.$emit('req', query)
        }, this.delay || 1000)
        return
      }
      if (query) {
        this.filterList = this.list.filter(item => {
          return item[this.datamap['label']].toLowerCase()
          .indexOf(query.toLowerCase()) > -1
        })
      } else {
        this.filterList = []
      }
    },
    changeSelect (event) {
      this.$emit('input', this.filterVal)
    },
    clickSelect (event) {
      this.$emit('select', this.filterVal)
    }
  },
  watch: {
    value (val) {
      this.filterVal = val
    }
  },
  created () {
  },
  mounted () {
    // 解决用户快速输入还未出下拉菜单时候值消失的问题
    var refrence = this.$refs.selecter && this.$refs.selecter.$refs.reference
    if (refrence && refrence.$el.children[0]) {
      refrence.$el.children[0].oninput = (e) => {
        e = e || window.event
        this.$emit('change', e.target.value)
        this.$emit('input', e.target.value)
        this.filterVal = e.target.value
        this.remoteMethod(this.filterVal)
      }
      refrence.$el.children[0].onblur = (e) => {
        this.$emit('blur', e.target.value)
      }
    }
  },
  computed: {
    drawData () {
      var result = []
      if (this.asyn) {
        result = this.list
      } else {
        result = this.filterVal ? this.filterList.slice(0, this.size) ? this.filterList.slice(0, this.size) : this.list.slice(0, this.size) : this.list.slice(0, this.size)
      }
      this.loading = false
      return result
    }
  }
}
</script>
<style>
</style>
