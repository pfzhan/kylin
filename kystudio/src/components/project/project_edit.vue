<template>
  <div class="project_edit">
    <el-form label-position="top" :model="projectDesc" :rules="rules" ref="projectForm">
      <el-form-item :label="$t('projectName')" prop="name">
        <el-input v-model="projectDesc.name" :placeholder="$t('projectPlace')" auto-complete="off" :disabled="isEdit"></el-input>
      </el-form-item>
      <el-form-item :label="$t('description')" prop="description">
        <el-input type="textarea" :placeholder="$t('projectDescription')" v-model="projectDesc.description" auto-complete="off" :disabled="isEdit"></el-input>
      </el-form-item>
      <div class="line-primary"></div>
      <el-form-item :label="$t('projectConfig')" prop="configuration" class="project-config">
        <el-row :gutter="20"  v-for="(property,index) in convertedProperties " :key="index">
          <el-col :span="10">
            <el-form-item prop="key">
              <el-input v-model="property.key" placeholder="Key"></el-input>
            </el-form-item>
          </el-col>
          <el-col :span="10">
            <el-form-item prop="value">
              <el-input v-model="property.value" placeholder="Value"></el-input>
            </el-form-item>
          </el-col>
          <el-col :span="3"><el-button size="small" type="danger" icon="minus" @click.prevent="removeProperty(index)"></el-button></el-col>
      </el-row>
    </el-form-item>
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
  props: ['project', 'visible', 'isEdit'],
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
    },
    checkProperty: function () {
      let alertMessage = false
      for (let i = 0; i < this.convertedProperties.length; i++) {
        if (this.convertedProperties[i].key === '') {
          this.$message({
            showClose: true,
            duration: 0,
            message: this.$t('checkCOKey'),
            type: 'error'
          })
          alertMessage = true
          break
        }
        if (this.convertedProperties[i].value === '') {
          this.$message({
            showClose: true,
            duration: 0,
            message: this.$t('checkCOValue'),
            type: 'error'
          })
          alertMessage = true
          break
        }
      }
      return alertMessage
    }
  },
  watch: {
    visible (visible) {
      this.projectDesc = Object.assign({}, this.project)
      this.convertedProperties = fromObjToArr(this.projectDesc.override_kylin_properties)
    }
  },
  created () {
    this.$on('projectFormValid', (t) => {
      this.$refs['projectForm'].validate((valid) => {
        if (valid) {
          if (!this.checkProperty()) {
            this.projectDesc.override_kylin_properties = fromArrToObj(this.convertedProperties)
            this.$emit('validSuccess', this.projectDesc)
          }
        } else {
          this.$emit('validFailed')
          return false
        }
      })
    })
  },
  locales: {
    'en': {projectName: 'Project Name', description: 'Description', projectConfig: 'Project Config', delete: 'Delete', property: 'Property', inputTip: 'The project name is required.', projectDescription: 'Project description...', projectPlace: 'You can use letters, numbers, and underscore characters "_"', noProject: 'Please enter the project name', checkCOKey: 'Project Config name is required!', checkCOValue: 'Project Config value is required!'},
    'zh-cn': {projectName: '项目名称', description: '描述', projectConfig: '项目配置', delete: '删除', property: '配置', inputTip: '项目名不能为空', projectDescription: '项目描述...', projectPlace: '可以使用字母、数字以及下划线', noProject: '请输入project名称', checkCOKey: '项目配置名不能为空!', checkCOValue: '项目配置值不能为空!'}
  }
}
</script>
<style lang="less">
  @import url(../../less/config.less);
  .project_edit {
    .line-primary {
      margin: 0px 0px 15px 0px;
    }
    .el-row {
      .el-col {
        height: 32px;
        margin-bottom: 10px;
        .el-input {
          height: 32px;
          line-height: 32px;
          .el-input__inner {
            height: 32px;
            line-height: 32px;
          }
        }
        .el-button--danger {
          height: 32px;
          line-height: 32px;
          padding: 0px 9px 0px 9px;
          .el-icon-minus {
            line-height: 32px;
          }
        }
      }
    }
    .el-form-item {
      margin-bottom: 15px;
      .el-form-item__label {
        font-size:12px;
        color:#ffffff;
        letter-spacing:0;
        line-height:14px;
        text-align:left;
      }
      .el-button--default {
        height: 32px;
        padding: 8px 15px 8px 15px;
        font-size:12px;
        color:#ffffff;
        .el-icon-close {
          color:#ffffff;
        }
      }
      .el-form-item__content::after {
        display: none;
      }
      .el-input__inner {
        font-size: 12px;
        color:#cdcfdd;
      }
    }
    .project-config {
      margin-bottom: 0px;
    }
    .el-input.is-disabled {
      .el-input__inner {
        background:#454b62;
        border:1px solid #51576f;
        border-radius:4px;
        width:348px;
        height:32px;
        font-size: 12px;
        color:#cdcfdd;
        letter-spacing:0;
      }
    }
    .el-textarea.is-disabled {
      background:#454b62;
      border:1px solid #51576f;
      border-radius:4px;
      width:348px;
      color:#cdcfdd;
      letter-spacing:0;
      .el-textarea__inner {
        border: none;
      }
    }
    .el-textarea__inner {
      font-size: 12px;
      color:#cdcfdd;
    }
  }
  .project_edit .el-form-item .el-icon-close {
    font-weight: 700;
    transform: rotate(45deg) scale(0.6);
  }
</style>
