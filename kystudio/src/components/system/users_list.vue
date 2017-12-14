<template>
<div class="user-list">
  <el-row>
    <el-col :span="24">
      <el-button type="trans" class="radius" :class="{'ksd-mt-20':extraoption && extraoption.groupName}" v-if="$store.state.system.securityProfile === 'testing' && (hasAdminProjectPermission() || isAdmin)" icon="plus"  @click="addUser">{{$t('user')}}</el-button>
      <div style="width:200px;" class="ksd-mb-10  ksd-fright" :class="{'ksd-mt-20':extraoption && extraoption.groupName}" >
          <el-input :placeholder="$t('userName')" icon="search" v-model="serarchChar" @change="filterUserList" class="show-search-btn" >
          </el-input>
        </div>
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
    style="width: 100%">
    <el-table-column
      :label="$t('userName')"
      sortable
      prop="username" :width="220">
      <template slot-scope="scope">
        <icon name="user-o"  style="color: #d4d7e3;position: absolute;top:15px;" scale="0.8"></icon><span class="ksd-ml-20">{{scope.row.username}}</span>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('kylinLang.common.group')"
      sortable>
      <template slot-scope="scope">
        <common-tip :content="scope.row.groups && scope.row.groups.join('<br/>')" placement="top-start">
            <p>{{scope.row.groups && scope.row.groups.join(',')}}</p>
        </common-tip>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('admin')" :width="120">
      <template slot-scope="scope">
        <i class="el-icon-check" v-if="scope.row.admin"></i>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('status')" :width="120">
      <template slot-scope="scope">
        <el-tag type="primary" v-if="scope.row.disabled">Disabled</el-tag>
        <el-tag type="success" v-else>Enabled</el-tag>
      </template>
    </el-table-column>
    <el-table-column v-if="$store.state.system.securityProfile === 'testing' && (hasAdminProjectPermission() || isAdmin)"
      :label="$t('action')" :width="100">
      <template slot-scope="scope">
        <el-dropdown trigger="click" >
          <el-button class="el-dropdown-link">
            <i class="el-icon-more"></i>
          </el-button >
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item v-show="scope.row.username!=='ADMIN'" @click.native="pushGroup(scope.row)">{{$t('groupMembership')}}</el-dropdown-item>
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


  <el-dialog :title="$t('kylinLang.common.group')" :visible.sync="addGroupDialog"  size="small" @close="closeDialog" :close-on-press-escape="false" :close-on-click-modal="false">
      <el-form>
        <el-form-item >
          <el-transfer filterable :titles="[$t('willcheck'), $t('haschecked')]" v-model="selectedGroupList" :data="groupList"></el-transfer>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="addGroupDialog = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="saveBtnLoad"  :disabled="selectedGroupList.length<=0" @click="saveGroupToUser">{{$t('kylinLang.common.save')}}</el-button>
      </div>
  </el-dialog>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import { handleError, handleSuccess, hasPermission, hasRole, kapConfirm } from 'util/business'
import addUser from './add_user'
import editRole from './edit_role'
import resetPassword from './reset_password'
import { pageCount, permissions } from 'config'
export default {
  name: 'userslist',
  props: ['extraoption'],
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
      },
      addGroupDialog: false,
      groupList: [],
      selectedGroupList: [],
      defaultGroup: 'ALL_USERS',
      saveBtnLoad: false,
      serarchChar: '',
      ST: null,
      userData: [],
      totalSize: 0
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
      loadUserListByGroupName: 'GET_USERS_BY_GROUPNAME',
      updateStatus: 'UPDATE_STATUS',
      saveUser: 'SAVE_USER',
      editRole: 'EDIT_ROLE',
      resetPassword: 'RESET_PASSWORD',
      removeUser: 'REMOVE_USER',
      getGroupList: 'GET_GROUP_LIST',
      addGroupToUsers: 'ADD_GROUPS_TO_USER'
    }),
    closeDialog () {

    },
    saveGroupToUser () {
      this.selected_user.authorities = this.selectedGroupList
      this.addGroupToUsers(this.selected_user).then((res) => {
        handleSuccess(res, (data) => {
          this.addGroupDialog = false
          this.loadUsers()
        })
      }, (res) => {
        handleError(res)
      })
    },
    pushGroup (data) {
      this.selected_user = data
      this.selectedGroupList = []
      this.groupList = []
      this.getGroupList({project: localStorage.getItem('selected_project')}).then((res) => {
        handleSuccess(res, (groups) => {
          this.selectedGroupList = this.selectedGroupList.concat(this.selected_user.groups)
          groups.forEach((x) => {
            if (x !== this.defaultGroup) {
              this.groupList.push({
                key: x,
                label: x,
                disabled: false
              })
            }
          })
          // 确保ALL_USER 组排在第一
          this.groupList.unshift({
            key: this.defaultGroup,
            label: this.defaultGroup,
            disabled: true
          })
        })
      })
      this.addGroupDialog = true
    },
    loadUsers () {
      var action = 'loadUsersList'
      var parameter = {pageSize: pageCount, pageOffset: this.currentPage - 1, project: localStorage.getItem('selected_project')}
      if (this.extraoption && this.extraoption.groupName) {
        parameter.groupName = this.extraoption.groupName
        action = 'loadUserListByGroupName'
      }
      if (this.serarchChar) {
        parameter.name = this.serarchChar
      }
      this[action](parameter).then((res) => {
        handleSuccess(res, (data) => {
          if (data) {
            this.userData = data.users || data.groupMembers || []
            this.totalSize = data.size
          }
        })
      }, (res) => {
        handleError(res)
      })
    },
    filterUserList () {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        this.loadUsers()
      }, 1500)
    },
    drop: function (userName, index) {
      kapConfirm(this.$t('cofirmDelUser')).then(() => {
        this.removeUser(userName).then((result) => {
          this.loadUsers()
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.delSuccess')
          })
        }, (result) => {
          handleError(result)
        })
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
      return hasPermission(this, permissions.ADMINISTRATION.mask)
    },
    pageCurrentChange (pager) {
      this.currentPage = pager
      this.loadUsers()
    },
    changeStatus: function (user) {
      let userStatus = {name: user.username, disabled: !user.disabled}
      this.updateStatus(userStatus).then((result) => {
        this.loadUsers()
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
          authorities: ['ALL_USERS']
        }
      }
      if (data.admin) {
        user.detail.authorities.push('ROLE_ADMIN')
      }
      if (this.extraoption && this.extraoption.groupName) {
        user.detail.authorities.push(this.extraoption.groupName)
      }
      this.saveUser(user).then((result) => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.saveSuccess')
        })
        this.loadUsers()
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
          authorities: data.authorities.map((au) => {
            return au.authority
          })
        }
      }
      var indexOfAdmin = user.detail.authorities.indexOf('ROLE_ADMIN')
      if (data.admin) {
        if (indexOfAdmin < 0) {
          user.detail.authorities.push('ROLE_ADMIN')
        }
      } else {
        if (indexOfAdmin >= 0) {
          user.detail.authorities.splice(indexOfAdmin, 1)
        }
      }
      this.editRole(user).then((result) => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.saveSuccess')
        })
        this.loadUsers()
      }, (result) => {
        handleError(result)
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
      }, (result) => {
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
      return this.totalSize
    },
    securityProfile () {
      return this.$store.state.system.securityProfile
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    },
    usersList () {
      let userDataRender = []
      this.userData.forEach((user) => {
        let newUser = {
          username: user.username,
          disabled: user.disabled,
          admin: false,
          modeler: false,
          analyst: false,
          defaultPassword: user.defaultPassword,
          authorities: user.authorities
        }
        var group = []
        user.authorities.forEach((role) => {
          if (role.authority === 'ROLE_ADMIN') {
            newUser.admin = true
          }
          // if (role.authority === 'ROLE_MODELER') {
          //   newUser.modeler = true
          // }
          // if (role.authority === 'ROLE_ANALYST') {
          //   newUser.analyst = true
          // }
          group.push(role.authority)
        })
        newUser.groups = group
        userDataRender.push(newUser)
      })
      return userDataRender
    }
  },
  created () {
    this.loadUsers()
  },
  locales: {
    'en': {user: 'User', userName: 'User Name', admin: 'System Admin', modeler: 'Modeler', analyst: 'Analyst', status: 'Status', action: 'Actions', editRole: 'Edit Role', resetPassword: 'Reset Password', drop: 'Drop', disable: 'Disable', enable: 'Enable', addUser: 'Add User', yes: 'Yes', cancel: 'Cancel', securityProfileTip: 'User management does not apply to the current security configuration, go to the correct permissions management page for editing.', refinePassword: ' Please redefine ADMIN\'s password while you login at the very first time.', 'groupMembership': 'Group Membership', willcheck: 'Group to be selected', haschecked: 'Checked Groups', cofirmDelUser: 'Are you sure to delete this user?'},
    'zh-cn': {user: '用户', userName: '用户名', admin: '系统管理员', modeler: '建模人员', analyst: '分析人员', status: '状态', action: '操作', editRole: '编辑角色', resetPassword: '重置密码', drop: '删除', disable: '禁用', enable: '启用', addUser: '添加用户', yes: '确定', cancel: '取消', securityProfileTip: '用户管理不适用于当前安全配置，请前往正确的权限管理页面编辑。', refinePassword: '为了保证系统安全，首次登录后请重置ADMIN密码。', 'groupMembership': '分组', willcheck: '待选择组', haschecked: '已选择组', cofirmDelUser: '确认删除该用户吗？'}
  }
}
</script>
<style lang="less">
@import '../../less/config.less';
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

  .el-transfer{
    text-align: center;
  }
  .el-transfer-panel__item{
    &:hover{
      background: none;
      color:@base-color;
    }
  }
  .el-transfer-panel__filter{
    margin-top: 0;
    &.el-input{
     .el-input__inner{
      width: 90%;
     }
    }
  }
  .el-transfer__buttons{
    .el-button+.el-button{
      margin-top: -80px;
      margin-left: 0;
    }
  }
  .el-transfer-panel {
    width: 40%;
    background-color: #20222e;
    border:solid 1px #20222e;
    text-align: left;
    .el-transfer-panel__header{
      background-color: #393e53;
      border-bottom: none;
      color:#fff;
      text-align: left;
    }
    .el-transfer-panel__footer{
      background-color: #393e53;
      border-top: none;
      color:#fff;
      text-align: left;
    }
  }
  .el-transfer-panel:first-child{
    float: left;
  }
  .el-transfer-panel:last-child{
    float: right;
  }
  .el-transfer__buttons{
    margin-top: 80px;
  }
}
</style>
