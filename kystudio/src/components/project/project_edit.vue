<template>
  <div>
    <el-form label-position="top" :model="projectDesc" :rules="rules" ref="projectForm">
    <el-form-item :label="$t('projectName')" prop="name">
      <el-input v-model="projectDesc.name" auto-complete="off"></el-input>
    </el-form-item>
    <el-form-item :label="$t('description')" >
      <el-input v-model="projectDesc.description" auto-complete="off"></el-input>
    </el-form-item>
    <el-form-item
    :label="$t('projectConfig')">
    <el-row :gutter="20"  v-for="(property,index) in convertedProperties ">
      <el-col :span="10"><el-input v-model="property.key"></el-input></el-col>
      <el-col :span="10"><el-input v-model="property.value"></el-input></el-col>
      <el-col :span="4"><el-button @click.prevent="removeProperty(index)">{{$t('delete')}}</el-button></el-col>
    </el-row>    
  </el-form-item> 
  <el-form-item>
    <el-button @click="addNewProperty">{{$t('propertye')}}</el-button>
  </el-form-item>    
  </el-form>
</div>
</template>
<script>
import { fromObjToArr, fromArrToObj } from '../../util/index'
export default {
  name: 'project_edit',
  props: ['project'],
  data () {
    return {
      convertedProperties: fromObjToArr(this.project.override_kylin_properties),
      projectDesc: Object.assign({}, this.project),
      rules: {
        name: [
          { required: true, message: this.$t('inputTip'), trigger: 'blur' }
        ]
      }
    }
  },
  methods: {
    removeProperty (index) {
      this.convertedProperties.splice(index, 1)
    },
    addNewProperty () {
      this.convertedProperties.push({key: '', value: ''})
    }
  },
  watch: {
    project (project) {
      this.projectDesc = Object.assign({}, this.project)
      this.convertedProperties = fromObjToArr(this.projectDesc.override_kylin_properties)
    }
  },
  created () {
    let _this = this
    this.$on('projectFormValid', (t) => {
      _this.$refs['projectForm'].validate((valid) => {
        if (valid) {
          _this.projectDesc.override_kylin_properties = fromArrToObj(this.convertedProperties)
          _this.$emit('validSuccess', _this.projectDesc)
        } else {
          _this.$emit('validFailed')
          return false
        }
      })
    })
  },
  locales: {
    'en': {projectName: 'Project Name', description: 'Description', projectConfig: 'Project Config', delete: 'Delete', property: 'Property', inputTip: 'The project name is required.'},
    'zh-cn': {projectName: '项目名称', description: '描述', projectConfig: '项目配置', delete: '删除', property: '配置', inputTip: '项目名不能为空'}
  }
}
</script>
<style scoped="">

</style>
