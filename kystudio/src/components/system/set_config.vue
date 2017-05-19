<template>
<div>
  <el-form  :model="newConfig" :rules="rules" label-position="top" ref="setConfigForm">
    <el-form-item :label="$t('key')" prop="key">
      <el-input v-model="newConfig.key" placeholder="Config Key..."></el-input>
    </el-form-item>
        <el-form-item :label="$t('value')" prop="value">
      <el-input v-model="newConfig.value" placeholder="Config Value..."></el-input>
    </el-form-item>  
 </el-form> 
</div>     
</template>
<script>
export default {
  name: 'set_config',
  data () {
    return {
      newConfig: {
        key: '',
        value: ''
      },
      rules: {
        key: [
            { required: true, message: '', trigger: 'blur' }
        ],
        value: [
            { required: true, message: '', trigger: 'blur' }
        ]
      }
    }
  },
  created () {
    let _this = this
    this.$on('setConfigFormValid', (t) => {
      _this.$refs['setConfigForm'].validate((valid) => {
        if (valid) {
          _this.$emit('validSuccess', this.newConfig)
        }
      })
    })
  },
  locales: {
    'en': {key: 'Key', value: 'Value'},
    'zh-cn': {key: '配置项', value: '配置值'}
  }
}
</script>
<style scoped>
</style>
