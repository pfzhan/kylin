<template>
  <el-form label-position="left" >
    <el-form-item :label="$t('name')">
      <el-input  v-model="measure.name"></el-input>
    </el-form-item>
    <el-form-item :label="$t('expression')">
      <el-select v-model="measure.function.expression">
        <el-option
          v-for="item in expressionsConf"
          :label="item"
          :value="item">
        </el-option>
      </el-select>
    </el-form-item>
    <el-form-item :label="$t('paramType')" >
      <el-tag>{{measure.function.parameter.type}}</el-tag>
    </el-form-item>
    <el-form-item :label="$t('paramValue')" >
      <el-select v-model="measure.function.parameter.value">
        <el-option
          v-for="item in expressionsConf"
          :label="item"
          :value="item">
        </el-option>
      </el-select>
    </el-form-item>   
    <el-form-item :label="$t('returnType')" >
      <el-tag>{{measure.function.returntype}}</el-tag>
    </el-form-item>          
  </el-form>
</template>
<script>
export default {
  name: 'add_measure',
  props: ['measureDesc', 'modelDesc'],
  data () {
    return {
      measure: Object.assign({}, this.measureDesc),
      expressionsConf: ['SUM', 'MIN', 'MAX', 'COUNT', 'COUNT_DISTINCT', 'TOP_N', 'RAW', 'EXTENDED_COLUMN', 'PERCENTILE']
    }
  },
  methods: {
    removeProperty (index) {
      console.log('322')
    },
    addNewProperty () {
      console.log('222')
    }
  },
  watch: {
    measureDesc (measureDesc) {
      this.measure = Object.assign({}, this.measureDesc)
    }
  },
  created () {
    console.log(this.measure)
  },
  locales: {
    'en': {name: 'Name', expression: 'Expression', paramType: 'Param Type', paramValue: 'Param Value', returnType: 'Return Type', includeDimensions: 'Include Dimensions'},
    'zh-cn': {name: '名称', expression: '表达式', paramType: '参数类型', paramValue: '参数值', returnType: '返回类型', includeDimensions: '包含维度'}
  }
}
</script>
<style scoped="">

</style>
