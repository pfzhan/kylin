<template>
  <el-dialog class="user-edit-modal" :width="modalWidth"
    :title="$t(modalTitle)"
    :visible="isShow"
    :close-on-press-escape="false"
    :close-on-click-modal="false"
    @close="isShow && closeHandler(false)">

    <el-form :model="form" label-position="top" :rules="rules" ref="form" v-if="isFormShow">
       <!-- 避免浏览器自动填充 -->
      <input name="username" type="text" style="display:none"/>
      <input name="password" type="password" style="width:1px;height:0;border:none;position:absolute"/>
      <!-- 表单：用户名 -->
      <el-form-item :label="$t('username')" prop="username" v-if="isFieldShow('username')">
        <el-input
          size="medium"
          :value="form.username"
          @input="value => inputHandler('username', value)"
          :disabled="editType !== 'new'">
          </el-input>
      </el-form-item>
      <!-- 表单：密码 -->
      <el-form-item :label="$t('password')" prop="password" v-if="isFieldShow('password')">
        <el-input
          size="medium"
          type="password"
          :value="form.password"
          @input="value => inputHandler('password', value)">
          </el-input>
      </el-form-item>
      <!-- 表单：旧密码（ 面向非管理员 -->
      <el-form-item :label="$t('oldPassword')" prop="oldPassword" v-if="!isAdminRole && isFieldShow('confirmPassword')">
        <el-input
          size="medium"
          type="password"
          :value="form.oldPassword"
          @input="value => inputHandler('oldPassword', value)">
          </el-input>
      </el-form-item>
      <!-- 表单：新密码 -->
      <el-form-item :label="$t('newPassword')" prop="newPassword" v-if="isFieldShow('newPassword')">
        <el-input
          size="medium"
          type="password"
          :value="form.newPassword"
          @input="value => inputHandler('newPassword', value)">
          </el-input>
      </el-form-item>
      <!-- 表单：确认密码 -->
      <el-form-item :label="$t('confirmNewPassword')" prop="confirmPassword" v-if="isFieldShow('confirmPassword')">
        <el-input
          size="medium"
          type="password"
          :value="form.confirmPassword"
          @input="value => inputHandler('confirmPassword', value)">
          </el-input>
      </el-form-item>
      <!-- 表单：角色 -->
      <el-form-item :label="$t('role')" v-if="isFieldShow('admin')">
        <el-radio-group :value="form.admin" @input="value => inputHandler('admin', value)">
          <el-radio :label="true">{{$t('admin')}}</el-radio>
          <el-radio :label="false">{{$t('user')}}</el-radio>
        </el-radio-group>
      </el-form-item>
      <!-- 表单：分组 -->
      <el-form-item v-if="isFieldShow('group')">
        <el-transfer
          filterable
          :data="totalGroups"
          :value="form.authorities"
          :titles="[$t('willCheckGroup'), $t('checkedGroup')]"
          @change="value => inputHandler('authorities', value)">
          </el-transfer>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="closeHandler(false)">{{$t('cancel')}}</el-button><el-button
      size="medium" plain type="primary" @click="submit" :loading="isLoading">{{$t('ok')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { fieldVisiableMaps, titleMaps, getSubmitData } from './handler'
import { validate, validateTypes, handleError } from '../../../util'

const { USERNAME, PASSWORD, CONFIRM_PASSWORD } = validateTypes

vuex.registerModule(['modals', 'UserEditModal'], store)

@Component({
  computed: {
    // 全局getter注入
    ...mapGetters([
      'isAdminRole',
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('UserEditModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback,
      totalGroups: state => state.totalGroups
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('UserEditModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      saveUser: 'SAVE_USER',
      editRole: 'EDIT_ROLE',
      resetPassword: 'RESET_PASSWORD',
      getGroupList: 'GET_GROUP_LIST',
      addGroupToUsers: 'ADD_GROUPS_TO_USER'
    })
  },
  locales
})
export default class UserEditModal extends Vue {
  // Data: 用来销毁el-form
  isFormShow = false
  isLoading = false
  // Data: el-form表单验证规则
  rules = {
    username: [{
      validator: this.validate(USERNAME), trigger: 'blur', required: true
    }],
    password: [{
      validator: this.validate(PASSWORD), trigger: 'blur', required: true
    }],
    newPassword: [{
      validator: this.validate(PASSWORD), trigger: 'blur', required: true
    }],
    confirmPassword: [{
      validator: this.validate(CONFIRM_PASSWORD), trigger: 'blur', required: true
    }]
  }

  // Computed: Modal宽度
  get modalWidth () {
    return this.editType === 'group'
      ? '660px'
      : '440px'
  }

  // Computed: Modal标题
  get modalTitle () {
    return titleMaps[this.editType]
  }

  // Computed Method: 计算每个Form的field是否显示
  isFieldShow (fieldName) {
    return fieldVisiableMaps[this.editType].includes(fieldName)
  }

  // Watcher: 监视销毁上一次elForm
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
      this.editType === 'group' && this.fetchUserGroups()
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

  // Action: Form递交函数
  async submit () {
    this.isLoading = true
    try {
      // 获取Form格式化后的递交数据
      const data = getSubmitData(this)
      // 验证表单
      await this.$refs['form'].validate()
      // 针对不同的模式，发送不同的请求
      this.editType === 'new' && await this.saveUser(data)
      this.editType === 'edit' && await this.editRole(data)
      this.editType === 'group' && await this.addGroupToUsers(data)
      this.editType === 'password' && await this.resetPassword(data)
      // 成功提示
      this.$message({
        type: 'success',
        message: this.editType !== 'password'
          ? this.$t('kylinLang.common.saveSuccess')
          : this.$t('kylinLang.common.updateSuccess')
      })
      // 关闭模态框，通知父组件成功
      this.closeHandler(true)
    } catch (e) {
      // 异常处理
      e && handleError(e)
    }
    this.isLoading = false
  }

  // Helper: 给el-form用的验证函数
  validate (type) {
    // TODO: 这里的this是vue的实例，而data却是class的实例
    return validate[type].bind(this)
  }

  // Helper: 从后台获取用户组
  async fetchUserGroups () {
    const project = this.currentSelectedProject
    const { data: { data: totalGroups } } = await this.getGroupList({ project })

    this.setModal({ totalGroups })
  }
}
</script>

<style lang="less">
.user-edit-modal {
  .el-transfer-panel {
    width: 250px;
  }
}
</style>
