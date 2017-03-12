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
    v-for=" (value, key, index) in projectDesc.override_kylin_properties"
    label="Project Config">
    <el-row :gutter="20">
      <el-col :span="10"><el-input v-model="convertedProperties[index].key"></el-input></el-col>
      <el-col :span="10"><el-input v-model="convertedProperties[index].value"></el-input></el-col>
      <el-col :span="4"><el-button @click.prevent="removeProperty(property)">删除</el-button></el-col>
    </el-row>    
  </el-form-item> 
  <el-form-item>
    <el-button @click="addNewProperty">Property</el-button>
  </el-form-item>    
  </el-form>
</div>
</template>
<script>
export default {
  name: 'project_edit',
  props: ['project'],
  data () {
    return {
      convertedProperties: [ ],
      projectDesc: Object.assign({}, this.project),
      rules: {
        name: [
          { required: true, message: '请输入项目名称', trigger: 'blur' }
        ]
      }
    }
  },
  watch: {
    project (project) {
      this.projectDesc = Object.assign({}, project)
    }
  },
  methods: {
    removeProperty (item) {
      console.log(item)
    },
    addNewProperty () {
      this.project.override_kylin_properties[' '] = ' '
    }
  },
  created () {
    var _this = this
    this.$on('project_update', (t) => {
      _this.$refs['projectForm'].validate((valid) => {
        if (valid) {
          _this.$emit('validSuccess', _this.projectDesc)
        } else {
          _this.$emit('validFailed')
          return false
        }
      })
    })
    this.$on('project_save', () => {
      _this.saveProject(_this.projectDesc)
    })
  }
}
</script>
<style scoped="">

</style>
