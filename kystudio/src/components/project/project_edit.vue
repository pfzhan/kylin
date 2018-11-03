<template>
  <div class="project_edit">
    <el-form label-position="top" label-width="110px" :model="projectDesc" :rules="rules" ref="projectForm">
      <el-form-item :label="$t('projectName')" prop="name">
        <el-input v-model="projectDesc.name" :placeholder="$t('projectPlace')" auto-complete="off" size="small"></el-input>
      </el-form-item>
      <el-form-item :label="$t('description')" prop="description">
        <el-input type="textarea" size="small" :placeholder="$t('projectDescription')" v-model="projectDesc.description" auto-complete="off" :disabled="isEdit"></el-input>
      </el-form-item>
      <!-- <div class="line-primary"></div> -->
      <el-form-item :label="$t('projectConfig')" prop="configuration" class="project-config" v-if="isEdit">
        <el-button type="primary" plain size="small" @click="addNewProperty" icon="el-icon-plus">{{$t('property')}}</el-button>
        <el-row :gutter="20"  v-for="(property,index) in convertedProperties " :key="index">
          <el-col :span="11">
            <el-form-item prop="key">
              <el-input v-model="property.key" placeholder="Key" :disabled="isProjectDefaultKey(index)" @blur="trimInput(property, 'key')"></el-input>
            </el-form-item>
          </el-col>
          <el-col :span="11">
            <el-form-item prop="value">
              <el-input v-model="property.value" placeholder="Value" :disabled="isProjectDefaultKey(index)" @blur="trimInput(property, 'value')"></el-input>
            </el-form-item>
          </el-col>
          <el-col :span="2"><el-button size="small" icon="el-icon-delete" @click.prevent="removeProperty(index)" v-if="!isProjectDefaultKey(index)"></el-button></el-col>
        </el-row>
      </el-form-item>
    <el-form-item>

    </el-form-item>
  </el-form>
</div>
</template>
<script>
import { mapActions } from 'vuex'
import { projectCfgs } from '../../config'
import { fromObjToArr, fromArrToObj } from '../../util/index'

const { constProperties } = projectCfgs

export default {
  name: 'project_edit',
  props: ['project', 'visible', 'isEdit'],
  data () {
    return {
      convertedProperties: fromObjToArr(this.project.override_kylin_properties),
      projectDesc: Object.assign({}, this.project),
      rules: {
        name: [
          { required: true, trigger: 'blur', validator: this.validateProjectName }
        ]
      }
    }
  },
  methods: {
    ...mapActions({
      loadConfig: 'LOAD_DEFAULT_CONFIG'
    }),
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
      this.convertedProperties.push({key: '', value: '', isNewProperty: true})
    },
    checkProperty () {
      let alertMessage = false

      for (let i = 0; i < this.convertedProperties.length; i++) {
        const currentProperty = this.convertedProperties[i]

        if (currentProperty.key === '') {
          this.alertMessage('checkCOKey')
          alertMessage = true
          break
        }
        if (currentProperty.value === '') {
          this.alertMessage('checkCOValue')
          alertMessage = true
          break
        }
        if (constProperties.includes(currentProperty.key) && currentProperty.isNewProperty) {
          this.alertMessage('checkCOConstProperty', { keyName: currentProperty.key })
          alertMessage = true
          break
        }

        // [暂定]property key禁止重复输入功能
        // const isPropertyDuplicate = this.convertedProperties.find(
        //   (item, itemIndex) => item.key === currentProperty.key && itemIndex !== i
        // )

        // if (isPropertyDuplicate) {
        //   this.alertMessage('checkCODuplicate')
        //   alertMessage = true
        //   break
        // }
      }
      return alertMessage
    },
    initProperty: function () {
      let defaultConfigs = fromObjToArr(this.$store.state.config.defaultConfig.project)
      defaultConfigs.forEach((config) => {
        this.convertedProperties.push({key: config.key, value: config.value})
      })
    },

    alertMessage (msg, fields) {
      this.$message({
        showClose: true,
        duration: 0,
        message: this.$t(msg, fields),
        type: 'error'
      })
    },

    isProjectDefaultKey (index) {
      const defaultPropertyIndex = this.convertedProperties.findIndex(
        item => constProperties.includes(item.key) && !item.isNewProperty
      )
      return defaultPropertyIndex === index
    },

    /**
     * Prevent user input value with space
     */
    trimInput (property, key) {
      setTimeout(() => {
        property[key] = property[key].trim()
      })
    }
  },
  watch: {
    visible (visible) {
      this.projectDesc = Object.assign({}, this.project)
      this.convertedProperties = fromObjToArr(this.projectDesc.override_kylin_properties)
      if (!this.isEdit) {
        // for newten
        // this.loadConfig('project').then(() => {
        //   this.initProperty()
        // })
      }
    }
  },
  created () {
    if (!this.isEdit) {
      // for newten
      // this.loadConfig('project').then(() => {
      //   this.initProperty()
      // })
    }
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
    'en': {projectName: 'Project Name', description: 'Description', projectConfig: 'Project Config', delete: 'Delete', property: 'Property', inputTip: 'The project name is required.', projectDescription: 'Project description...', projectPlace: 'You can use letters, numbers, and underscore characters "_"', noProject: 'Please enter the project name', checkCOKey: 'Project Config name is required.', checkCOValue: 'Project Config value is required.', noSuite: 'Please select the business unit', checkCOConstProperty: '[{keyName}] cannot be edited'},
    'zh-cn': {projectName: '项目名称', description: '描述', projectConfig: '项目配置', delete: '删除', property: '配置', inputTip: '项目名不能为空', projectDescription: '项目描述...', projectPlace: '可以使用字母、数字以及下划线', noProject: '请输入项目名称', checkCOKey: '项目配置名不能为空', checkCOValue: '项目配置值不能为空', noSuite: '请选择业务单元', checkCOConstProperty: '[{keyName}] 不可被修改'}
  }
}
</script>
<style lang="less">
  @import "../../assets/styles/variables.less";
  .project_edit {
    .line-primary {
      margin: 0px 0px 15px 0px;
      background: #4A5070;
      height:2px;
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
        .el-button--small {
          padding: 7px 9px;
        }
      }
    }
    .el-form-item {
      margin-bottom: 15px;
      .el-form-item__label {
        letter-spacing:0;
      }
      .el-button--default {
        height: 32px;
        font-size:12px;
      }
      .el-form-item__content::after {
        display: none;
      }
      .el-input__inner {
        font-size: 12px;
        color: @color-text-primary;
      }
    }
    .project-config {
      margin-bottom: 0px;
    }
  }
  .project_edit .el-form-item .el-icon-close {
    font-weight: 700;
    transform: rotate(45deg) scale(0.6);
  }
</style>
