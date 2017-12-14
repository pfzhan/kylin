<template>
<div class="group-list">
  <el-row>
    <el-col :span="1">
      <el-button type="trans" class="radius" v-if="mixAccess" icon="plus" @click="addGroup">{{$t('kylinLang.common.group')}}</el-button>
    </el-col>
  </el-row>
  <el-table
    :data="groupUsersList"
    border
    class="table_margin"
    style="width: 100%"
    >
    <el-table-column
      :label="$t('kylinLang.common.name')"
      sortable
      show-overflow-tooltip
      prop="first">
      <template slot-scope="scope">
        <icon name="group" style="color: #d4d7e3;position: absolute;top:15px;" scale="0.8"></icon>
        <span @click="addTab(scope.row.first, scope.row.second)"  style="cursor:pointer;" class="ksd-ml-20 underline">{{scope.row.first}}</span>
      </template>
    </el-table-column>
    <el-table-column
      :label="$t('usersCount')"
      show-overflow-tooltip
      sortable>
      <template slot-scope="scope">
        {{scope.row.second && scope.row.second.length || 0}}
      </template>
    </el-table-column>
    <el-table-column v-if="$store.state.system.securityProfile === 'testing' && (hasAdminProjectPermission() || isAdmin)"
      :label="$t('kylinLang.common.action')" :width="100">
      <template slot-scope="scope">
        <el-dropdown trigger="click" >
          <el-button class="el-dropdown-link">
            <i class="el-icon-more"></i>
          </el-button >
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item  @click.native="assignUsers(scope.row)">{{$t('assignUsers')}}</el-dropdown-item>
            <el-dropdown-item v-show="(scope.row.first!=='ROLE_ADMIN' && scope.row.first!=='ALL_USERS')" @click.native="dropGroup(scope.row.first)">{{$t('kylinLang.common.drop')}}</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </template>
    </el-table-column>
  </el-table>

  <pager class="ksd-center" ref="pager" :totalSize="groupUsersListSize"  v-on:handleCurrentChange='pageCurrentChange' ></pager>
  <el-dialog :title="$t('newGroup')" v-model="addGroupFormVisible" size="tiny" :close-on-press-escape="false" :close-on-click-modal="false">
      <el-form :model="addGroupMeta" :rules="addGroupFormRule" ref="addGroupForm">
        <el-form-item :label="$t('kylinLang.common.groupName')" prop="newName">
          <el-input v-model="addGroupMeta.newName" auto-complete="off"></el-input>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="addGroupFormVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="btnLoading" @click="saveGroup">{{$t('kylinLang.common.save')}}</el-button>
      </div>
    </el-dialog>

     <el-dialog :title="$t('kylinLang.common.user')" :visible.sync="addUserToGroupDialog"  size="small" :close-on-press-escape="false" :close-on-click-modal="false">
      <el-form>
        <el-form-item >
          <el-transfer filterable :titles="[$t('willcheck'), $t('haschecked')]" ref="transfer" v-model="selectedUserList" :data="userList"></el-transfer>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer">
        <el-button @click="addUserToGroupDialog = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :loading="btnLoading"  :disabled="selectedUserList.length<=0" @click="saveUserToGroup">{{$t('kylinLang.common.save')}}</el-button>
      </div>
  </el-dialog>
</div>
</template>

<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, hasPermission, hasRole, kapConfirm } from 'util/business'
import addUser from './add_user'
import editRole from './edit_role'
import resetPassword from './reset_password'
import $ from 'jquery'
import { pageCount, permissions, NamedRegex } from 'config'
export default {
  name: 'grouplist',
  data () {
    return {
      addGroupFormVisible: false,
      addUserToGroupDialog: false,
      addGroupMeta: {
        newName: ''
      },
      addGroupFormRule: {
        newName: [
          {required: true, message: this.$t('kylinLang.common.pleaseInput'), trigger: 'blur'},
          {validator: this.checkName, trigger: 'blur'}]
      },
      currentPage: 1,
      btnLoading: false,
      defaultGroup: 'All Users',
      selectedUserList: [],
      userList: [],
      currentSelectedGroup: '',
      ST: null
    }
  },
  components: {
    'add_user': addUser,
    'edit_role': editRole,
    'reset_password': resetPassword
  },
  methods: {
    ...mapActions({
      loadGroupUsersList: 'GET_GROUP_USERS_LIST',
      saveNewGroup: 'ADD_GROUP',
      delGroup: 'DEL_GROUP',
      loadUsersList: 'LOAD_USERS_LIST',
      addUserToGroup: 'ADD_USERS_TO_GROUP'
    }),
    addTab (groupName, userList) {
      this.$emit('addtab', groupName, userList)
    },
    saveUserToGroup () {
      this.addUserToGroup({
        groupName: this.currentSelectedGroup,
        data: this.selectedUserList
      }).then((res) => {
        handleSuccess(res, () => {
          this.addUserToGroupDialog = false
          this.loadGroupUsers()
        })
      }, (res) => {
        handleError(res)
      })
    },
    loadUsers (filterName) {
      var para = {pageSize: 1000, pageOffset: 0, project: localStorage.getItem('selected_project')}
      if (filterName) {
        para.name = filterName
      }
      return this.loadUsersList(para)
    },
    filterUser (val) {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        val = val || ''
        this.loadUsers(val).then(() => {
          var userList = []
          for (var k = 0; k < this.userListData.length; k++) {
            var curUser = this.userListData[k]
            if (this.selectedUserList.indexOf(curUser.username) >= 0) {
              continue
            }
            userList.push({
              key: curUser.username,
              label: curUser.username
            })
          }
          this.selectedUserList.forEach((user) => {
            userList.push({
              key: user,
              label: user
            })
          })
          this.userList = userList
        })
      }, 500)
    },
    assignUsers (group) {
      this.addUserToGroupDialog = true
      this.selectedUserList = group.second
      this.currentSelectedGroup = group.first
      this.filterUser()
      this.$nextTick(() => {
        var filterInputDom = $(this.$refs['transfer'].$el.querySelectorAll('.el-transfer-panel__filter input')[0])
        filterInputDom.val('')
        filterInputDom.unbind('input propertychange').bind('input propertychange', () => {
          var val = filterInputDom.val()
          this.filterUser(val)
        })
      })
    },
    dropGroup (groupName) {
      kapConfirm(this.$t('confirmDelGroup')).then(() => {
        this.delGroup({groupName: groupName}).then((res) => {
          handleSuccess(res, () => {
            this.loadGroupUsers()
          })
        }, (res) => {
          handleError(res)
        })
      })
    },
    addGroup () {
      this.addGroupFormVisible = true
    },
    saveGroup () {
      this.btnLoading = true
      this.$refs['addGroupForm'].validate((valid) => {
        if (valid) {
          this.saveNewGroup({
            groupName: this.addGroupMeta.newName
          }).then((res) => {
            handleSuccess(res, (data) => {
              this.btnLoading = false
              this.addGroupFormVisible = false
              this.resetAddGroupForm()
              this.loadGroupUsers()
            })
          }, (res) => {
            this.btnLoading = false
            handleError(res)
          })
        }
      })
    },
    resetAddGroupForm: function () {
      this.$refs['addGroupForm'].resetFields()
    },
    checkName (rule, value, callback) {
      if (!NamedRegex.test(value)) {
        callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
      } else {
        callback()
      }
    },
    loadGroupUsers () {
      this.loadGroupUsersList({pageSize: pageCount, pageOffset: this.currentPage - 1})
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
      this.loadGroupUsers()
    }
  },
  computed: {
    groupUsersListSize () {
      return this.$store.state.user.usersGroupSize
    },
    userListData () {
      // 大量用户测试用例
      // var list = []
      // for (var i = 0; i < 100000; i++) {
      //   list.push({username: 'user_' + i})
      // }
      // return list
      return this.$store.state.user.usersList
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    },
    groupUsersList () {
      let userData = []
      this.$store.state.user.usersGroupList.forEach(function (group) {
        userData.push(group)
      })
      return userData
    },
    mixAccess () {
      return this.$store.state.system.securityProfile === 'testing' && (this.hasAdminProjectPermission() || this.isAdmin)
    }
  },
  created () {
    this.loadGroupUsers()
    if (this.mixAccess) {
      this.loadUsers()
    }
  },
  locales: {
    'en': {newGroup: 'New Group', assignUsers: 'Assign Users', willcheck: 'Unassigned Users', haschecked: 'Assigned Users', confirmDelGroup: 'Are you sure to delete this group?', usersCount: 'User Number'},
    'zh-cn': {newGroup: '新建组', assignUsers: '分配用户', willcheck: '未分配的用户', haschecked: '已分配组用户', confirmDelGroup: '确认删除该组吗？', usersCount: '用户数'}
  }
}
</script>
<style lang="less">
@import '../../less/config.less';
.group-list{
  .underline{
    &:hover{
      text-decoration: underline;
    }
  }
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
