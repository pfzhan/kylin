<template>
<div class="user-list">
  <el-row>
    <el-col :span="1">
      <el-button type="primary" v-if="$store.state.system.securityProfile === 'testing' && (hasAdminProjectPermission() || isAdmin)" icon="plus" size="small" @click="addUser">{{$t('user')}}</el-button>
    </el-col>
  </el-row>
  <el-alert v-if="$store.state.system.securityProfile !== 'testing'" class="ksd-mt-20"
    :title="$t('securityProfileTip')"
    :closable="false"
    type="info">
  </el-alert>
  <el-table
    :data="usersList"
    border
    class="table_margin"
    style="width: 100%"
    :default-sort = "{prop: 'username', order: 'descending'}">
    <el-table-column
      :label="$t('userName')"
      sortable
      prop="username">
    </el-table-column>
    <el-table-column
      :label="$t('admin')">
      <template scope="scope">
        <i class="el-icon-check" v-if="scope.row.admin"></i>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('status')">
      <template scope="scope">
        <el-tag type="primary" v-if="scope.row.disabled">Disabled</el-tag>
        <el-tag type="success" v-else>Enabled</el-tag>
      </template>
    </el-table-column>
    <el-table-column v-if="$store.state.system.securityProfile === 'testing' && (hasAdminProjectPermission() || isAdmin)"
      :label="$t('action')">
      <template scope="scope">
        <el-dropdown trigger="click" >
          <el-button class="el-dropdown-link">
            <i class="el-icon-more"></i>
          </el-button >
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item v-show="scope.row.username!=='ADMIN'" @click.native="edit(scope.row)">{{$t('editRole')}}</el-dropdown-item>
            <el-dropdown-item @click.native="reset(scope.row)">{{$t('resetPassword')}}</el-dropdown-item>
            <el-dropdown-item v-show="scope.row.username!=='ADMIN'" @click.native="drop(scope.row.username, scope.row.$index)">{{$t('drop')}}</el-dropdown-item>
            <el-dropdown-item v-show="scope.row.username!=='ADMIN'" @click.native="changeStatus(scope.row)" v-if="scope.row.disabled">{{$t('enable')}}</el-dropdown-item>
            <el-dropdown-item v-show="scope.row.username!=='ADMIN'" @click.native="changeStatus(scope.row)" v-else>{{$t('disable')}}</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </template>
    </el-table-column>
  </el-table>

  <pager class="ksd-center" ref="pager" :totalSize="usersListSize"  v-on:handleCurrentChange='pageCurrentChange' ></pager>
  <el-dialog @close="closeAddUser" :title="$t('addUser')" v-model="addUserFormVisible">
    <add_user :newUser="selected_user" ref="addUser" v-on:validSuccess="addUserValidSuccess"></add_user>
    <div slot="footer" class="dialog-footer">
      <el-button  @click="addUserFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" @click="checkAddUserForm">{{$t('yes')}}</el-button>
    </div>
  </el-dialog>

  <el-dialog @close="closeEditRole" :title="$t('editRole')" v-model="editRoleFormVisible">
    <edit_role :userDetail="selected_user" ref="editRole" v-on:validSuccess="editRoleValidSuccess"></edit_role>
    <div slot="footer" class="dialog-footer">
      <el-button @click="editRoleFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" @click="checkEditRoleForm">{{$t('yes')}}</el-button>
    </div>
  </el-dialog>

  <el-dialog @close="closeResetPassword" :title="$t('resetPassword')" v-model="resetPasswordFormVisible">
    <reset_password  :curUser="selected_user" ref="resetPassword" v-on:validSuccess="resetPasswordValidSuccess"></reset_password>
    <div slot="footer" class="dialog-footer">
      <el-button @click="resetPasswordFormVisible = false">{{$t('cancel')}}</el-button>
      <el-button type="primary" @click="checkResetPasswordForm">{{$t('yes')}}</el-button>
    </div>
  </el-dialog>

  <el-dialog :title="$t('resetPassword')" v-model="resetAdmin" :show-close="false" :close-on-click-modal="false" :close-on-press-escape="false">
    <el-alert style="margin-bottom:10px;"
      :title="$t('refinePassword')"
      type="info"
      :closable="false"
      show-icon>
    </el-alert>
    <reset_password  :curUser="adminSetting" ref="resetPassword" v-on:validSuccess="resetPasswordValidSuccess">
    </reset_password>
    <div slot="footer" class="dialog-footer">
      <el-button type="primary" @click="checkResetPasswordForm">{{$t('yes')}}</el-button>
    </div>
  </el-dialog>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import { handleError, hasPermission, hasRole } from '../../util/business'
import addUser from './add_user'
import editRole from './edit_role'
import resetPassword from './reset_password'
import { pageCount, permissions } from '../../config'
export default {
  name: 'userslist',
  data () {
    return {
      selected_user: {},
      addUserFormVisible: false,
      editRoleFormVisible: false,
      resetPasswordFormVisible: false,
      currentPage: 1,
      adminSetting: {
        username: 'ADMIN',
        password: '',
        disabled: false,
        admin: true,
        modeler: false,
        analyst: false,
        confirmPassword: ''
      }
    }
  },
  components: {
    'add_user': addUser,
    'edit_role': editRole,
    'reset_password': resetPassword
  },
  methods: {
    ...mapActions({
      loadUsersList: 'LOAD_USERS_LIST',
      updateStatus: 'UPDATE_STATUS',
      saveUser: 'SAVE_USER',
      editRole: 'EDIT_ROLE',
      resetPassword: 'RESET_PASSWORD',
      removeUser: 'REMOVE_USER'
    }),
    drop: function (userName, index) {
      this.removeUser(userName).then((result) => {
        this.loadUsersList({pageSize: this.$refs['pager'].pageSize, pageOffset: this.currentPage - 1})
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
      }).catch((result) => {
        handleError(result)
      })
    },
    getProjectIdByName (pname) {
      var projectList = this.$store.state.project.allProject
      var len = projectList && projectList.length || 0
      var projectId = ''
      for (var s = 0; s < len; s++) {
        if (projectList[s].name === pname) {
          projectId = projectList[s].uuid
        }
      }
      return projectId
    },
    hasAdminProjectPermission () {
      return hasPermission(this, this.getProjectIdByName(localStorage.getItem('selected_project')), permissions.ADMINISTRATION.mask)
    },
    pageCurrentChange (pager) {
      this.currentPage = pager
      this.loadUsersList({pageSize: this.$refs['pager'].pageSize, pageOffset: pager - 1})
    },
    changeStatus: function (user) {
      let userStatus = {name: user.username, disabled: !user.disabled}
      this.updateStatus(userStatus).then((result) => {
        this.loadUsersList({pageSize: this.$refs['pager'].pageSize, pageOffset: this.currentPage - 1})
      }).catch((result) => {
        handleError(result)
      })
    },
    addUser: function () {
      this.selected_user = {
        username: '',
        password: '',
        disabled: false,
        admin: false,
        modeler: false,
        analyst: true,
        confirmPassword: ''
      }
      this.addUserFormVisible = true
    },
    closeAddUser: function () {
      this.$refs['addUser'].$refs['addUserForm'].resetFields()
    },
    checkAddUserForm: function () {
      this.$refs['addUser'].$emit('addUserFormValid')
    },
    addUserValidSuccess: function (data) {
      let user = {
        name: data.username,
        detail: {
          username: data.username,
          password: data.password,
          disabled: data.disabled,
          authorities: []
        }
      }
      if (data.admin) {
        user.detail.authorities.push('ROLE_ADMIN')
      }
      if (data.modeler) {
        user.detail.authorities.push('ROLE_MODELER')
      }
      if (data.analyst) {
        user.detail.authorities.push('ROLE_ANALYST')
      }
      this.saveUser(user).then((result) => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.saveSuccess')
        })
        this.loadUsersList({pageSize: this.$refs['pager'].pageSize, pageOffset: this.currentPage - 1})
      }).catch((result) => {
        handleError(result)
      })
      this.$refs['addUser'].$refs['addUserForm'].resetFields()
      this.addUserFormVisible = false
    },
    closeEditRole: function () {
      this.$refs['editRole'].$refs['editRoleForm'].resetFields()
    },
    edit: function (userDetail) {
      this.selected_user = userDetail
      this.editRoleFormVisible = true
    },
    checkEditRoleForm: function () {
      this.$refs['editRole'].$emit('editRoleFormValid')
    },
    editRoleValidSuccess: function (data) {
      let user = {
        name: data.username,
        detail: {
          defaultPassword: data.defaultPassword,
          username: data.username,
          password: data.password,
          disabled: data.disabled,
          authorities: []
        }
      }
      if (data.admin) {
        user.detail.authorities.push('ROLE_ADMIN')
      }
      if (data.modeler) {
        user.detail.authorities.push('ROLE_MODELER')
      }
      if (data.analyst) {
        user.detail.authorities.push('ROLE_ANALYST')
      }
      this.editRole(user).then((result) => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.saveSuccess')
        })
      }).catch((result) => {
        this.$message({
          type: 'error',
          message: result.statusText
        })
      })
      this.editRoleFormVisible = false
    },
    closeResetPassword: function () {
      if (this.$refs['resetPassword'].$refs['resetPasswordForm']) {
        this.$refs['resetPassword'].$refs['resetPasswordForm'].resetFields()
      }
    },
    reset: function (userDetail) {
      this.selected_user = userDetail
      this.resetPasswordFormVisible = true
    },
    checkResetPasswordForm: function () {
      this.$refs['resetPassword'].$emit('resetPasswordFormValid')
    },
    resetPasswordValidSuccess: function (data) {
      let userPassword = {
        username: data.username,
        password: data.oldPassword,
        newPassword: data.password
      }
      this.resetPassword(userPassword).then((result) => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.updateSuccess')
        })
        this.resetPasswordFormVisible = false
        this.$store.state.system.needReset = false
      }).catch((result) => {
        handleError(result)
      })
    },
    closeResetWindow: function (done) {
    }
  },
  computed: {
    resetAdmin () {
      if (this.$store.state.system.needReset) {
        return true
      }
      return false
    },
    usersListSize () {
      return this.$store.state.user.usersSize
    },
    securityProfile () {
      return this.$store.state.system.securityProfile
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    },
    usersList () {
      let userData = []
      this.$store.state.user.usersList.forEach(function (user) {
        let newUser = {
          username: user.username,
          disabled: user.disabled,
          admin: false,
          modeler: false,
          analyst: false,
          defaultPassword: user.defaultPassword
        }
        user.authorities.forEach(function (role) {
          if (role.authority === 'ROLE_ADMIN') {
            newUser.admin = true
          }
          if (role.authority === 'ROLE_MODELER') {
            newUser.modeler = true
          }
          if (role.authority === 'ROLE_ANALYST') {
            newUser.analyst = true
          }
        })
        userData.push(newUser)
      })
      return userData
    }
  },
  created () {
    this.loadUsersList({pageSize: pageCount, pageOffset: this.currentPage - 1})
  },
  locales: {
    'en': {user: 'User', userName: 'User Name', admin: 'System Admin', modeler: 'Modeler', analyst: 'Analyst', status: 'Status', action: 'Actions', editRole: 'Edit Role', resetPassword: 'Reset Password', drop: 'Drop', disable: 'Disable', enable: 'Enable', addUser: 'Add User', yes: 'Yes', cancel: 'Cancel', securityProfileTip: 'User management does not apply to the current security configuration, go to the correct permissions management page for editing.', refinePassword: ' Please redefine ADMIN\'s password while you login at the very first time.'},
    'zh-cn': {user: '用户', userName: '用户名', admin: '系统管理员', modeler: '建模人员', analyst: '分析人员', status: '状态', action: '操作', editRole: '编辑角色', resetPassword: '重置密码', drop: '删除', disable: '禁用', enable: '启用', addUser: '添加用户', yes: '确定', cancel: '取消', securityProfileTip: '用户管理不适用于当前安全配置，请前往正确的权限管理页面编辑。', refinePassword: '为了保证系统安全，首次登录后请重置ADMIN密码。'}
  }
}
</script>
<style lang="less">

.user-list{
  margin: 0 30px 0 30px;
  .table_margin {
    margin-top: 20px;
    margin-bottom: 20px;
  }
  .el-icon-check {
    color: #13ce66;
  }
  .el-tag {
    // color: #fff;
  }
  .el-tag--danger {
      background-color: #ff4949;
  }
  .el-tag--success {
    background-color: #13ce66;
    color: #fff;
  }
  td {
    height: 50px;
  }
}
</style>
