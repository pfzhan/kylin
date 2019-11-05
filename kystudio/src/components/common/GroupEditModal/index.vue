<template>
  <el-dialog class="group-edit-modal" :width="modalWidth"
    :title="$t(modalTitle)"
    :visible="isShow"
    limited-area
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    @close="isShow && closeHandler(false)">
    <el-form :model="form" :rules="rules" ref="form" v-if="isFormShow">
      <!-- 表单：组名 -->
      <el-form-item :label="$t('kylinLang.common.groupName')" prop="groupName" v-if="isFieldShow('groupName')">
        <el-input auto-complete="off" @input="value => inputHandler('groupName', value.trim())" :value="form.groupName"></el-input>
      </el-form-item>
      <!-- 表单：分配用户 -->
      <el-form-item v-if="isFieldShow('users')">
        <el-transfer
          filterable
          :data="totalUsers"
          :value="form.selectedUsers"
          :before-query="queryHandler"
          :total-elements="totalSizes"
          :titles="[$t('willCheckGroup'), $t('checkedGroup')]"
          @change="value => inputHandler('selectedUsers', value)">
            <div class="users-over-size-tip" slot="left-panel-bottom_content" v-if="isOverSizeTip">{{$t('overSizeTip')}}</div>
        </el-transfer>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="closeHandler(false)">{{$t('kylinLang.common.cancel')}}</el-button><el-button
      size="medium" plain type="primary" @click="submit">{{$t('kylinLang.common.save')}}</el-button>
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

const { GROUP_NAME } = validateTypes

vuex.registerModule(['modals', 'GroupEditModal'], store)

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('GroupEditModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback,
      totalUsers: state => state.totalUsers
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('GroupEditModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      saveGroup: 'ADD_GROUP',
      loadUsersList: 'LOAD_USERS_LIST',
      addUserToGroup: 'ADD_USERS_TO_GROUP'
    })
  },
  locales
})
export default class GroupEditModal extends Vue {
  // Data: 用来销毁el-form
  isFormShow = false
  // Data: el-form表单验证规则
  rules = {
    groupName: [{
      validator: this.validate(GROUP_NAME), trigger: 'blur', required: true
    }]
  }

  // 是否显示提示文案（接口size数大于显示数）
  isOverSizeTip = false
  // 返回的数据总数
  totalSizes = [0]

  // Computed: Modal宽度
  get modalWidth () {
    return this.editType === 'assign'
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
      this.editType === 'assign' && this.fetchUsers()
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

  async queryHandler (title, query) {
    if (title === this.$t('willCheckGroup')) {
      await this.fetchUsers(query)
    }
  }

  // Action: 修改Form函数
  inputHandler (key, value) {
    this.setModalForm({[key]: value})
  }

  // Action: Form递交函数
  async submit () {
    try {
      // 获取Form格式化后的递交数据
      const data = getSubmitData(this)
      // 验证表单
      await this.$refs['form'].validate()
      // 针对不同的模式，发送不同的请求
      this.editType === 'new' && await this.saveGroup(data)
      this.editType === 'assign' && await this.addUserToGroup(data)
      // 成功提示
      this.$message({
        type: 'success',
        message: this.$t('kylinLang.common.saveSuccess')
      })
      // 关闭模态框，通知父组件成功
      this.closeHandler(true)
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

  // Helper: 从后台获取用户组
  async fetchUsers (value) {
    const pageSize = 1000
    const { data: { data } } = await this.loadUsersList({
      pageSize,
      pageOffset: 0,
      project: this.currentSelectedProject,
      name: value
    })

    const remoteUsers = data.users
      .map(user => ({ key: user.username, value: user.username }))

    const selectedUsersNotInRemote = this.form.selectedUsers
      .map(sItem => ({key: sItem, value: sItem}))
      .filter(sItem => !remoteUsers.some(user => user.key === sItem.key))

    this.totalSizes = [data.size]

    if (data.size > pageSize) {
      this.isOverSizeTip = true
    }

    this.setModal({
      totalUsers: [ ...selectedUsersNotInRemote, ...remoteUsers ]
    })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.group-edit-modal {
  .el-transfer-panel {
    width: 250px;
  }
  .users-over-size-tip {
    color: @text-normal-color;
    font-size: @text-assist-size;
    text-align: center;
  }
}
</style>
