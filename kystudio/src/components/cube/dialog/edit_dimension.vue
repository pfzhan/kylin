<template>
<el-row :gutter="20">
  <el-col :span="14">
    <el-form  :model="dimension" :rules="rules" label-position="right"  label-width="180px" ref="editDimensionForm">
      <el-form-item :label="$t('tableAlias')" >
        <el-tag>{{dimension.table}}</el-tag>
      </el-form-item>
      <el-form-item :label="$t('columnName')" >
        <el-tag v-if="dimension.column">{{dimension.column}}</el-tag>
        <el-tag v-else>{{dimension.derived}}</el-tag>
      </el-form-item>      
      <el-alert  type="info" show-icon :title="$t('tip')" v-if="modelDesc.factTables.indexOf(dimension.table) ===-1">
      </el-alert>      
      <el-form-item :label="$t('type')" v-if="modelDesc.factTables.indexOf(dimension.table) ===-1">
        <el-radio-group v-model="type">
          <el-radio-button label="normal">{{$t('normal')}}</el-radio-button>
          <el-radio-button label="derived">{{$t('derived')}}</el-radio-button>
        </el-radio-group>
      </el-form-item>        
      <el-form-item :label="$t('name')" prop="name">
        <el-input v-model="dimension.name"></el-input>
      </el-form-item> 
   </el-form>
  </el-col>   
  <el-col :span="10">
    <el-tag>{{$t('tips')}}</el-tag>
    <el-row>
      <el-col :span="24">{{$t('tips1')}}</el-col>
    </el-row>
    <el-row>
      <el-col :span="24">{{$t('tips2')}}</el-col>
    </el-row>
    <el-row>
      <el-col :span="24">{{$t('tips3')}}</el-col>
    </el-row>
     <el-row>
      <el-col :span="24">{{$t('tips4')}}</el-col>
    </el-row>
    <el-row>
      <el-col :span="24">{{$t('tips5')}}</el-col>
    </el-row>               
  </el-col>  
</el-row>  
</template>
<script>
export default {
  name: 'edit_dimension',
  props: ['dimensionDesc', 'modelDesc'],
  data () {
    return {
      dimension: Object.assign({}, this.dimensionDesc),
      type: this.dimensionDesc.column ? 'normal' : 'derived',
      rules: {
        name: [
            { required: true, message: '', trigger: 'blur' }
        ]
      }
    }
  },
  watch: {
    dimensionDesc (dimensionDesc) {
      this.dimension = Object.assign({}, this.dimensionDesc)
    }
  },
  created () {
    let _this = this
    this.$on('editDimensionFormValid', (t) => {
      _this.$refs['editDimensionForm'].validate((valid) => {
        if (valid) {
          _this.$emit('validSuccess', {type: this.type, name: this.dimension.name})
        }
      })
    })
  },
  locales: {
    'en': {tableAlias: 'Table Alias', columnName: 'Column Name', type: 'Type', name: 'Name', normal: 'Normal', derived: 'Derived', tip: 'Derived dimension only comes from lookup table.', tips: 'tip', tips1: '1.Type in any input box for auto suggestion', tips2: '2.Pick up Fact Table from Star Schema Tables first', tips3: '3.Data Type should match with Hive Table\'s Data Type', tips4: '4.Join Type have to be same as will be used in query', tips5: '5.Using Derived for One-One relationship between columns, like ID and Name'},
    'zh-cn': {tableAlias: '表别名', columnName: '列名', type: '类型', name: '名称', normal: '普通维度', derived: '可推导维度', tip: '可推导(Derived)维度需要来自维度表.', tips: '提示', tips1: '1.在输入框中输入, 系统会自动提示', tips2: '2.请先从星型模型中选择事实表', tips3: '3.数据类型需要与Hive表中的类型相一致', tips4: '4.连接类型需跟SQL查询时的连接关系相同', tips5: '5.对于ID, Name这种一一对应的列, 请添加维度时选择可推导(Derived)类型的维度'}
  }
}
</script>
<style scoped="">

</style>
