<template>
  <div>
    <el-form label-position="top" :model="measureDesc" :rules="rules" ref="projectForm">
    <el-form-item :label="$t('')" prop="name">
      <el-input v-model="measureDesc.name" auto-complete="off"></el-input>
    </el-form-item>
  <el-form-item>
    <el-button @click="addNewProperty">{{$t('property')}}</el-button>
  </el-form-item>    
  </el-form>
</div>
</template>
<script>
export default {
  name: 'add_measures',
  props: ['measureDesc'],
  data () {
    return {
      convertedProperties: []
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
  created () {
    console.log('11')
  },
  locales: {
    'en': {name: 'Name', expression: 'Expression', paramType: 'Param Type', paramValue: 'Param Value', returnType: 'Return Type', includeDimensions: 'Include Dimensions'},
    'zh-cn': {name: '', expression: '', paramType: '', paramValue: '', returnType: '', includeDimensions: ''}
  }
}
</script>
<style scoped="">

</style>
