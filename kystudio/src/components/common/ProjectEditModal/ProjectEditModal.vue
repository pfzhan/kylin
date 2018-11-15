<template>
  <el-dialog class="project-edit-modal" :width="modalWidth"
    :title="$t(modalTitle)"
    :visible="isShow"
    @close="isShow && closeHandler(false)">
    
    <el-form :model="form" :label-position="labelPosition" :rules="rules" ref="form" v-if="isFormShow" label-width="110px">
      <!-- 表单：项目名 -->
      <el-form-item :label="$t('projectName')" prop="name" v-if="isFieldShow('name')">
        <el-input
          size="small"
          auto-complete="off"
          :value="form.name"
          :placeholder="$t('projectPlace')"
          @input="value => inputHandler('name', value)">
        </el-input>
      </el-form-item>
      <div class="el-form-item is-required" style="padding: 10px 0;" v-if="isFieldShow('type')">
        <label for="name" class="el-form-item__label" style="width: 110px;line-height: 1;">{{$t('projectType')}}</label>
        <div class="el-form-item__content clearfix" style="margin-left: 110px;">
          <!-- project type 2 -->
          <div class="project-type clearfix" :class="{ active: form.type === 'MANUAL_MAINTAIN' }">
            <!-- <div class="project-type-status">
              <span class="el-icon-ksd-good_health"></span>
            </div> -->
            <div class="project-type-button">
              <div class="project-type-icon" @click="inputHandler('type', 'MANUAL_MAINTAIN')">
                <i class="el-icon-ksd-model_designer"></i>
              </div>
              <div class="project-type-text">
                <span class="font-medium">{{$t('projectType2')}}</span>
              </div>
            </div>
          </div>
          <!-- project type 4 -->
          <div class="project-type clearfix" :class="{ active: form.type === 'AUTO_MAINTAIN' }">
            <!-- <div class="project-type-status">
              <span class="el-icon-ksd-good_health"></span>
            </div> -->
            <div class="project-type-button">
              <div class="project-type-icon" @click="inputHandler('type', 'AUTO_MAINTAIN')">
                <i class="el-icon-ksd-sql_acceleration"></i>
              </div>
              <div class="project-type-text">
                <span class="font-medium">{{$t('projectType4')}}</span>
              </div>
            </div>
          </div>
        </div>
      </div>
      <!-- 表单：项目描述 -->
      <el-form-item :label="$t('description')" prop="description" v-if="isFieldShow('description')">
        <el-input
          size="small"
          type="textarea"
          auto-complete="off"
          :value="form.description"
          :disabled="editType !== 'new'"
          :placeholder="$t('projectDescription')"
          @input="value => inputHandler('description', value)">
        </el-input>
      </el-form-item>
      <!-- 表单：项目配置 -->
      <div class="project-config" v-if="isFieldShow('configuration')">
        <label class="el-form-item__label">{{$t('projectConfig')}}</label>
        <div>
          <el-button
            plain
            class="add-property"
            size="small"
            type="primary"
            icon="el-icon-plus"
            @click="addProperty">
            {{$t('property')}}
          </el-button>
        </div>
        <!-- 表单：配置项键 -->
        <el-row :gutter="20" v-for="(property, index) in form.properties" :key="index">
          <el-col :span="11">
            <el-form-item prop="properties.key">
              <el-input
                size="small"
                placeholder="Key"
                :value="property.key"
                :disabled="isPropertyDisabled(index)"
                @input="value => propertyHandler('input', 'key', index, value)"
                @blur="propertyHandler('blur', 'key', index)">
              </el-input>
            </el-form-item>
          </el-col>
          <!-- 表单：配置项值 -->
          <el-col :span="11">
            <el-form-item prop="properties.value">
              <el-input
                size="small"
                placeholder="Value"
                :value="property.value"
                :disabled="isPropertyDisabled(index)"
                @input="value => propertyHandler('input', 'value', index, value)"
                @blur="propertyHandler('blur', 'value', index)">
              </el-input>
            </el-form-item>
          </el-col>
          <!-- 表单：配置项删除按钮 -->
          <el-col :span="2">
            <el-button
              size="small"
              icon="el-icon-delete"
              @click.prevent="removeProperty(index)"
              v-if="!isPropertyDisabled(index)">
            </el-button>
          </el-col>
        </el-row>
      </div>
    </el-form>

    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="closeHandler(false)">{{$t('cancel')}}</el-button>
      <el-button size="medium" plain type="primary" @click="submit">{{$t('kylinLang.common.submit')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapMutations, mapActions } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { fieldVisiableMaps, titleMaps, getSubmitData, disabledProperties } from './handler'
import { validate, validateTypes, handleError, fromObjToArr, handleSuccessAsync } from '../../../util'

const { PROJECT_NAME } = validateTypes

vuex.registerModule(['modals', 'ProjectEditModal'], store)

@Component({
  computed: {
    ...mapState({
      defaultProperties: state => fromObjToArr(state.config.defaultConfig.project)
    }),
    // Store数据注入
    ...mapState('ProjectEditModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('ProjectEditModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      updateProject: 'UPDATE_PROJECT',
      saveProject: 'SAVE_PROJECT',
      loadConfig: 'LOAD_DEFAULT_CONFIG'
    })
  },
  locales
})
export default class ProjectEditModal extends Vue {
  // Data: 用来销毁el-form
  isFormShow = false
  // Data: el-form表单验证规则
  rules = {
    name: [{
      validator: this.validate(PROJECT_NAME), trigger: 'blur', required: true
    }]
  }
  get labelPosition () {
    return this.editType === 'new' ? 'left' : 'top'
  }
  // Computed: Modal宽度
  get modalWidth () {
    return '660px'
  }
  // Computed: Modal标题
  get modalTitle () {
    return titleMaps[this.editType]
  }
  // Computed Method: 计算每个Form的field是否显示
  isFieldShow (fieldName) {
    return fieldVisiableMaps[this.editType].includes(fieldName)
  }
  // Computed Method: 计算是否属性是被禁止修改
  isPropertyDisabled (propertyIdx) {
    const properties = JSON.parse(JSON.stringify(this.form.properties))
    const property = properties[propertyIdx]

    return !property.isNew && disabledProperties.includes(property.key)
  }
  // Watcher: 监视销毁上一次elForm
  @Watch('isShow')
  async onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
    } else {
      setTimeout(() => {
        this.isFormShow = false
      }, 300)
    }
  }
  // Action: 模态框关闭函数
  closeHandler (isSubmit) {
    this.hideModal()

    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 200)
  }
  // Action: 修改Form函数
  inputHandler (key, value) {
    this.setModalForm({[key]: value})
  }
  // Action: 修改Form中的properties
  propertyHandler (action, type, propertyIdx, value) {
    const properties = JSON.parse(JSON.stringify(this.form.properties))
    let shouldUpdate = false

    const property = properties[propertyIdx]

    if (action === 'input') {
      property[type] = value
      property.isNew = true
      shouldUpdate = true
    }
    if (action === 'blur' && property[type] !== property[type].trim()) {
      property[type] = property[type].trim()
      property.isNew = true
      shouldUpdate = true
    }
    shouldUpdate && this.setModalForm({ properties })
  }
  // Action: 新添加一个project property
  addProperty () {
    const properties = JSON.parse(JSON.stringify(this.form.properties))

    properties.push({key: '', value: '', isNew: true})
    this.setModalForm({ properties })
  }
  // Action: 删除一个project property
  removeProperty (propertyIdx) {
    const properties = JSON.parse(JSON.stringify(this.form.properties))

    properties.splice(propertyIdx, 1)
    this.setModalForm({ properties })
  }
  // Action: Form递交函数
  async submit () {
    try {
      const isInvaild = this.validateProperties()

      if (!isInvaild) {
        let res
        // 获取Form格式化后的递交数据
        const data = getSubmitData(this)
        // 验证表单
        await this.$refs['form'].validate()
        // 针对不同的模式，发送不同的请求
        switch (this.editType) {
          case 'new':
            res = await handleSuccessAsync(await this.saveProject(data))
            break
          case 'edit':
            res = await handleSuccessAsync(await this.updateProject(data))
            break
        }
        this.$message({
          type: 'success',
          message: this.$t('saveSuccessful')
        })
        this.closeHandler(res)
      } else {
        this.$message({ showClose: true, duration: 0, message: isInvaild, type: 'error' })
      }
    } catch (e) {
      // 异常处理
      e && handleError(e)
    }
  }
  // Helper: 给el-form用的验证函数
  validate (type) {
    // TODO: 这里的this是vue的实例，而data却是class的实例
    return validate[type].bind(this)
  }
  // Helper: project属性验证
  validateProperties () {
    const duplicateProperty = this.form.properties.find(property => disabledProperties.includes(property.key) && property.isNew)
    const hasEmptyKeyProperty = this.form.properties.some(property => !property.key)
    const emptyValueProperty = this.form.properties.find(property => !property.value)

    if (duplicateProperty) {
      return this.$t('propertyCannotChange', { keyName: duplicateProperty.key })
    } else if (hasEmptyKeyProperty) {
      return this.$t('propertyEmptyKey')
    } else if (emptyValueProperty) {
      return this.$t('propertyEmptyValue')
    }
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.project-edit-modal {
  .el-form-item {
    margin-bottom: 15px;
  }
  .project-config .add-property {
    margin: 3px 0 10px 0;
  }
  .project-config .el-input__inner {
    height: 32px;
    line-height: 32px;
  }
  .project-config .el-col {
    height: 32px;
    margin-bottom: 10px;
  }
  .project-type,
  .project-type-status,
  .project-type-button {
    display: block;
    float: left;
    height: 125px;
    position: relative;
  }
  .project-type-status .el-icon-ksd-good_health {
    vertical-align: top;
  }
  .project-type-status {
    margin-right: 10px;
  }
  .project-type {
    margin-right: 25px;
  }
  .project-type:not(.active) {
    color: @text-disabled-color;
  }
  .project-type-icon {
    width: 90px;
    height: 90px;
    border: 1px solid @text-secondary-color;
    text-align: center;
    line-height: 90px;
    border-radius: 6px;
    font-size: 65px;
    overflow: hidden;
    color: @base-color;
    cursor: pointer;
    margin: 0 auto 15px auto;
  }
  .project-type-text {
    line-height: 1;
    text-align: center;
    white-space: nowrap;
  }
  .project-type.active .project-type-text {
    color: @text-title-color;
  }
  .project-type.active .project-type-icon {
    border-color: @base-color;
    background: @base-color;
    color: @fff;
  }
  .project-type.active .el-icon-ksd-good_health {
    color: @btn-success-normal;
  }
  .project-type-icon:hover {
    border-color: @base-color;
  }
  .el-form-item__label {
    font-weight: 500;
  }
}
</style>
