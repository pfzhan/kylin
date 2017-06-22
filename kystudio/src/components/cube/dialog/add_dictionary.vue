<template>
<div>
  <el-form  :model="newDictionary" label-position="left" ref="cloneCubeForm" label-width="180px">
    <el-form-item :label="$t('column')" >
      <el-select v-model="newDictionary.column" >
        <el-option v-for="(column, index) in factColumns"
        :label="column" :value="column">
      </el-option>
      </el-select>
    </el-form-item> 
    <el-form-item >
      <el-radio-group v-model="isReuse">
        <el-radio-button label="false">{{$t('builderClass')}}</el-radio-button>
        <el-radio-button label="true">{{$t('reuse')}}</el-radio-button>
      </el-radio-group>
    </el-form-item>
    <el-form-item v-if="isReuse === 'false'" :label="$t('builderClass')" >
      <el-select v-model="newDictionary.builder" >
        <el-option v-for="(dictionary, index) in buildDictionaries"
        :label="dictionary.name" :value="dictionary.value">
      </el-option>
      </el-select>
    </el-form-item>
    <el-form-item v-else :label="$t('reuse')" >
      <el-select v-model="newDictionary.reuse" >
        <el-option v-for="(column, index) in factColumns"
        :label="column" :value="column">
      </el-option>
      </el-select>
    </el-form-item>
 </el-form> 
</div>     
</template>
<script>
export default {
  name: 'add_dictionary',
  props: ['cubeDesc', 'dictionaryDesc'],
  data () {
    return {
      newDictionary: Object.assign({}, this.dictionaryDesc),
      buildDictionaries: [{name: 'Global Dictionary', value: 'org.apache.kylin.dict.GlobalDictionaryBuilder'}],
      isReuse: 'false',
      factColumns: []
    }
  },
  methods: {
    getFactColumns: function () {
      let _this = this
      _this.cubeDesc.dimensions.forEach(function (dimension, index) {
        if (dimension.column && dimension.derived === null) {
          _this.factColumns.push(dimension.table + '.' + dimension.column)
        } else {
          dimension.derived.forEach(function (derived) {
            _this.factColumns.push(dimension.table + '.' + derived)
          })
        }
      })
      _this.cubeDesc.measures.forEach(function (measure) {
        if (measure.function.parameter.type === 'column' && _this.factColumns.indexOf(measure.function.parameter.value) === -1) {
          _this.factColumns.push(measure.function.parameter.value)
        }
      })
    }
  },
  watch: {
    dictionaryDesc (dictionaryDesc) {
      this.newDictionary = Object.assign({}, this.dictionaryDesc)
    },
    cubeDesc (cubeDesc) {
      this.factColumns = []
      this.getFactColumns()
    }
  },
  created () {
    let _this = this
    this.getFactColumns()
    this.$on('editDictionaryFormValid', (t) => {
      let dictionary = {}
      if (_this.isReuse === 'true') {
        dictionary = {column: _this.newDictionary.column, reuse: _this.newDictionary.reuse}
      } else {
        dictionary = {column: _this.newDictionary.column, builder: _this.newDictionary.builder}
      }
      _this.$emit('validSuccess', dictionary)
    })
  },
  locales: {
    'en': {column: 'Column', builderClass: 'Builder Class', reuse: 'Reuse'},
    'zh-cn': {column: '列', builderClass: '构造类', reuse: '复用'}
  }
}
</script>
<style scoped="">
</style>
