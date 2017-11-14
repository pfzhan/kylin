<template>
  <el-select
    v-model="value"
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
  props: ['list', 'size', 'placeholder', 'dataMap', 'value', 'disabled'],
  data () {
    return {
      loading: false,
      datamap: this.dataMap ? this.dataMap : {label: 'label', value: 'value'},
      filterList: [],
      filter: ''
    }
  },
  methods: {
    remoteMethod (query) {
      this.filter = query
      if (query) {
        this.filter = query
        this.loading = true
        this.filterList = this.list.filter(item => {
          return item[this.datamap['label']].toLowerCase()
          .indexOf(query.toLowerCase()) > -1
        })
      } else {
        this.filterList = []
      }
      this.$emit('input', query)
    },
    changeSelect (event) {
      this.$emit('input', this.value)
    }
  },
  created () {
  },
  computed: {
    drawData () {
      this.loading = false
      return this.filter ? this.filterList.slice(0, this.size) ? this.filterList.slice(0, this.size) : this.list.slice(0, this.size) : this.list.slice(0, this.size)
    }
  }
}
</script>
<style>
</style>
