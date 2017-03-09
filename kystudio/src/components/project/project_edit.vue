<template>
  <div>
    <el-form label-position="top">
    <el-form-item label="Project Name" >
      <el-input v-model="project.name" auto-complete="off"></el-input>
    </el-form-item>
    <el-form-item label="Description" >
      <el-input v-model="project.description" auto-complete="off"></el-input>
    </el-form-item>
    <el-form-item
    v-for=" (value, key, index) in project.override_kylin_properties"
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
  <el-form-item>
   <el-button type="primary" @click="updateProject (project) ">确定</el-button>   
   <el-button @click="showTip = false">取消</el-button>
  </el-form-item>     
  </el-form>
</div>
</template>
<script>
export default {
  name: 'project_edit',
  props: ['project', 'showTip'],
  data () {
    return {
      convertedProperties: [ ]
    }
  },
  methods: {
    updateProject (project) {
      this.showTip = false
//      this.updateProject(project)
      this.$dispatch('updateProject', this.showTip)
    },
    saveProject (project) {
      this.showTip = false
//      this.saveProject(project)
    },
    removeProperty (item) {
//      var index = this.dynamicValidateForm.domains.indexOf(item)
//      if (index !== -1) {
//        this.dynamicValidateForm.domains.splice(index, 1)
//      }
      console.log(item)
    },
    addNewProperty () {
      this.project.override_kylin_properties[' '] = ' '
      // this.dynamicValidateForm.domains.push({
      //   value: '',
      //   key: Date.now()
      // });
      console.log(2)
    }
  }
}
</script>
<style scoped="">

</style>
