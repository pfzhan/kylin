<template>
  <div>
    <el-form label-position="top">
    <el-form-item label="Project Name" >
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
import { mapActions } from 'vuex'
export default {
  name: 'project_edit',
  props: ['project'],
  data () {
    return {
      convertedProperties: [ ],
      projectDesc: Object.assign({}, this.project)
    }
  },
  watch: {
    project (project) {
      this.projectDesc = Object.assign({}, project)
    }
  },
  methods: {
    ...mapActions({
      updateProject: 'UPDATE_PROJECT',
      saveProject: 'SAVE_PROJECT'
    }),
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
  },
  created () {
    var _this = this
    this.$on('project_update', () => {
      _this.updateProject(_this.projectDesc)
    })
    this.$on('project_save', () => {
      _this.saveProject(_this.projectDesc)
    })
  }
}
</script>
<style scoped="">

</style>
