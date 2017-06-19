<template>
  <div class="project_edit">
    <el-form label-position="top" :model="projectDesc" :rules="rules" ref="projectForm">
      <el-form-item :label="$t('projectName')" prop="name">
        <el-input v-model="projectDesc.name" :placeholder="$t('projectPlace')" auto-complete="off"></el-input>
      </el-form-item>
      <el-form-item :label="$t('description')" >
        <el-input type="textarea" :placeholder="$t('projectDescription')" v-model="projectDesc.description" auto-complete="off"></el-input>
      </el-form-item>
      <el-col class="project-config">{{$t('projectConfig')}}</el-col>
      <el-row :gutter="20" class="ksd-mb-6"  v-for="(property,index) in convertedProperties " :key="index">
        <el-col :span="10">
          <el-form-item prop="key">
            <el-input v-model="property.key" placeholder="key"></el-input>
          </el-form-item>
        </el-col>
        <el-col :span="10">
          <el-form-item prop="value">
            <el-input v-model="property.value" placeholder="value"></el-input>
          </el-form-item>
        </el-col>
        <el-col :span="4"><el-button type="danger" class="ksd-mt-12" @click.prevent="removeProperty(index)">{{$t('delete')}}</el-button></el-col>
    </el-row>    
    <el-form-item>
      <el-button @click="addNewProperty" icon="close">{{$t('property')}}</el-button>
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
          { trigger: 'blur', validator: this.validateProjectName }
        ]
      }
    }
  },
  methods: {
    validateProjectName (rule, value, callback) {
      if (value === '') {
        callback(new Error(this.$t('noProject')))
      } else if (!/^\w+$/.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    },
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
    this.$on('projectFormValid', (t) => {
      this.$refs['projectForm'].validate((valid) => {
        if (valid) {
          this.projectDesc.override_kylin_properties = fromArrToObj(this.convertedProperties)
          this.$emit('validSuccess', this.projectDesc)
        } else {
          this.$emit('validFailed')
          return false
        }
      })
    })
  },
  locales: {
    'en': {projectName: 'Project Name', description: 'Description', projectConfig: 'Project Config', delete: 'Delete', property: 'Property', inputTip: 'The project name is required.', projectDescription: 'Project description...', projectPlace: 'You can use letters, numbers, and underscore characters "_"', noProject: 'Please enter the project name'},
    'zh-cn': {projectName: '项目名称', description: '描述', projectConfig: '项目配置', delete: '删除', property: '配置', inputTip: '项目名不能为空', projectDescription: '项目描述...', projectPlace: '可以使用字母、数字以及下划线', noProject: '请输入project名称'}
  }
}
</script>
<style>
.project_edit .project-config {
  height: 30px;
  line-height: 30px;
}
.project_edit .el-form-item .el-icon-close {
  font-weight: 700;
  transform: rotate(45deg) scale(0.6);
}
</style>
