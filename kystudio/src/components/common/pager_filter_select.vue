<template>
  <el-select
    v-model="filterVal"
    filterable
    remote
    :placeholder="$t(placeholder)"
    :remote-method="remoteMethod"
    @change="changeSelect"
    @input="changeSelect"
    :disabled="disabled"
    :loading="loading">
    <el-option
      v-for="item in drawData"
      :key="item[datamap['value']]"
      :label="item[datamap['label']]"
      :value="item[datamap['value']]">
    </el-option>
  </el-select>
</template>
<script>
export default {
  name: 'filterSelect',
  props: ['list', 'size', 'placeholder', 'dataMap', 'value', 'disabled', 'asyn', 'delay'],
  data () {
    return {
      loading: false,
      datamap: this.dataMap ? this.dataMap : {label: 'label', value: 'value'},
      filterList: [],
      filterVal: this.value,
      filter: '',
      ST: null
    }
  },
  methods: {
    remoteMethod (query) {
      this.filter = query
      this.$emit('input', query)
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
      this.$emit('input', this.value)
    }
  },
  watch: {
    value (val) {
      this.filterVal = val
    }
  },
  created () {
  },
  computed: {
    drawData () {
      this.loading = false
      if (this.asyn) {
        return this.list
      }
      return this.filter ? this.filterList.slice(0, this.size) ? this.filterList.slice(0, this.size) : this.list.slice(0, this.size) : this.list.slice(0, this.size)
    }
  }
}
</script>
<style>
</style>
