<template>
  <div>
    <el-form label-position="top" :model="projectDesc" :rules="rules" ref="projectForm">
    <el-form-item label="Project Name" prop="name">
      <el-input v-model="projectDesc.name" auto-complete="off"></el-input>
    </el-form-item>
    <el-form-item label="Description" >
      <el-input v-model="projectDesc.description" auto-complete="off"></el-input>
    </el-form-item>
    <el-form-item
    label="Project Config">
    <el-row :gutter="20"  v-for="(property,index) in convertedProperties ">
      <el-col :span="10"><el-input v-model="property.key"></el-input></el-col>
      <el-col :span="10"><el-input v-model="property.value"></el-input></el-col>
      <el-col :span="4"><el-button @click.prevent="removeProperty(index)">删除</el-button></el-col>
    </el-row>    
  </el-form-item> 
  <el-form-item>
    <el-button @click="addNewProperty">Property</el-button>
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
          { required: true, message: '请输入项目名称', trigger: 'blur' }
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
  }
}
</script>
<style scoped="">

</style>
