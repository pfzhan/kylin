<template>
  <div>
    <ul v-if="measure.function.expression !== 'TOP_N' && measure.function.expression !== 'EXTENDED_COLUMN'">
      <li>
        Value:<b>{{ measure.function.parameter.value }}</b>, Type:<b>{{measure.function.parameter.type }}</b>    
      </li>  
      <li v-for="(item,key) in nextParaList">
        Value:<b>{{item}}</b>, Type:<b>column</b>    
      </li>
    </ul>  
    <ul v-if="measure.function.expression === 'EXTENDED_COLUMN'">
      <li>Host Column:<b>{{ measure.function.parameter.value }}</b></li>
      <li>Extended Column:<b><span>{{ measure.function.parameter.next_parameter.value}}.</span></b></li>      
    </ul>
    <ul v-if="measure.function.expression === 'TOP_N'">
      <li>SUM|ORDER BY:<b>{{ measure.function.parameter.value }}</b></li>
      <li>
        Group By:
        <b v-for="(item,key) in nextParaList">
          <span>{{ item }}</span>
          <span v-if="key < nextParaList.length-1">,</span>
          <span v-else></span>
        </b>
      </li>
    </ul>
  </div>
</template>

<script>
export default {
  name: 'parameterTree',
  props: ['measure'],
  methods: {
    recursion: function (parameter, list) {
      let _this = this
      list.push(parameter.value)
      if (parameter.next_parameter) {
        _this.recursion(parameter.next_parameter, list)
      } else {
        return
      }
    }
  },
  computed: {
    nextParaList: function () {
      let _this = this
      let paralist = []
      if (_this.measure.function.parameter.next_parameter) {
        _this.recursion(_this.measure.function.parameter.next_parameter, paralist)
      }
      return paralist
    }
  }
}
</script>
<style scoped>
 li {
  list-style:none
 }
</style>
