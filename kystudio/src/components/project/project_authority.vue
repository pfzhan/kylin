<template>
  <div id="projectAuth">
    <div class="ksd-title-label">{{$t('projectTitle', {projectName: currentProject})}}</div>
    <el-row class="ksd-mt-10 ksd-mb-10">
      <el-col :span="24">
        <div class="ksd-fleft ky-no-br-space">
          <el-button plain type="primary" icon="el-icon-ksd-back" @click="$router.push('/admin/project')">{{$t('back')}}</el-button>
          <el-button plain type="primary" icon="el-icon-ksd-add_2" @click="authorUser()">{{$t('userAccess')}}</el-button>
          <!-- <el-button v-if="accessView == 'user'" plain type="primary" @click="toggleView('table')">{{$t('toggleTableView')}}</el-button>
          <el-button v-if="accessView == 'table'" plain type="primary" @click="toggleView('user')">{{$t('toggleUserView')}}</el-button> -->
        </div>
        <div style="width:200px;" class="ksd-fright" v-if="accessView == 'user'">
          <el-input class="show-search-btn"
            size="medium"
            v-model="serarchChar"
            :placeholder="$t('userNameOrGroup')"
            @input="inputFilter">
            <i slot="prefix" class="el-input__icon" :class="{'el-icon-search': !searchLoading, 'el-icon-loading': searchLoading}"></i>
          </el-input>
        </div>
      </el-col>
    </el-row>
    <div v-if="accessView === 'user'">
      <el-table :data="userAccessList" class="user-access-table" border key="user">
        <el-table-column type="expand">
          <template slot-scope="props">
            <user_access :roleOrName="props.row.roleOrName" :projectName="currentProject" :type="props.row.type"></user_access>
          </template>
        </el-table-column>
        <el-table-column :label="$t('userOrGroup')" prop="roleOrName">
          <template slot-scope="props">
            <i :class="{'el-icon-ksd-table_admin': props.row.type === 'User', 'el-icon-ksd-table_group': props.row.type === 'Group'}"></i>
            <span>{{props.row.roleOrName}}</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('type')" prop="type"></el-table-column>
        <el-table-column :label="$t('accessType')" prop="promission"></el-table-column>
        <el-table-column :label="$t('kylinLang.common.action')" :width="87">
          <template slot-scope="scope">
            <el-tooltip :content="$t('kylinLang.common.edit')" effect="dark" placement="top">
              <i class="el-icon-ksd-table_edit ksd-mr-10 ksd-fs-14" @click="editAuthorUser(scope.row)"></i>
            </el-tooltip><span>
            </span><el-tooltip :content="$t('kylinLang.common.delete')" effect="dark" placement="top">
              <i class="el-icon-ksd-table_delete ksd-fs-14" @click="removeAccess(scope.row.id, scope.row.roleOrName, scope.row.promission, !scope.row.sid.grantedAuthority)"></i>
            </el-tooltip>
          </template>
        </el-table-column>
      </el-table>
      <kap-pager
        class="ksd-center ksd-mtb-10" ref="pager"
        :totalSize="totalSize"
        @handleCurrentChange="handleCurrentChange">
      </kap-pager>
    </div>
    <div v-if="accessView === 'table'">
      <el-table :data="tableAccessList" class="table-access-table" border key="table">
        <el-table-column type="expand">
          <template slot-scope="props">
            <table_access :users="props.row.users"></table_access>
          </template>
        </el-table-column>
        <el-table-column :label="$t('tableName')" prop="name"></el-table-column>
        <el-table-column :label="$t('datasourceType')" prop="type"></el-table-column>
        <el-table-column :label="$t('kylinLang.common.action')" :width="87">
          <template slot-scope="scope">
            <el-tooltip :content="$t('kylinLang.common.delete')" effect="dark" placement="top">
              <i class="el-icon-ksd-table_delete ksd-fs-14" @click=""></i>
            </el-tooltip>
          </template>
        </el-table-column>
      </el-table>
      <kap-pager
        class="ksd-center ksd-mtb-10" ref="pager"
        :totalSize="tableTotalSize"
        @handleCurrentChange="handleCurrentChange1">
      </kap-pager>
    </div>

    <el-dialog :title="authorTitle" width="960px" class="author_dialog" :close-on-press-escape="false" :close-on-click-modal="false" :visible.sync="authorizationVisible" @close="initAccessData">
      <el-alert :title="$t('authorTips')" class="ksd-mb-20" show-icon :closable="false" :show-background="false" type="info" v-if="!isEditAuthor"></el-alert>
      <div class="ksd-title-label-small">{{$t('selectUserAccess')}}</div>
      <div v-for="(accessMeta, index) in accessMetas" :key="index" class="user-group-select ksd-mt-10 ky-no-br-space">
        <el-select placeholder="Type" v-model="accessMeta.principal" :disabled="isEditAuthor" @change="changeUserType(index)" size="medium" class="user-select">
          <el-option label="user" :value="true"></el-option>
          <el-option label="group" :value="false"></el-option>
        </el-select>
        <!-- <kap-filter-select class="name-select" :asyn="true" @req="filterUser" v-model="accessMeta.sids" :disabled="isEditAuthor" multiple :list="renderUserList" placeholder="kylinLang.common.pleaseInputUserName" :size="100" v-if="accessMeta.principal"></kap-filter-select> -->
        <el-select
          class="name-select"
          v-model="accessMeta.sids"
          :disabled="isEditAuthor"
          multiple
          filterable
          remote
          :placeholder="$t('kylinLang.common.pleaseInputUserName')"
          :remote-method="filterUser"
          v-if="accessMeta.principal">
          <el-option
            v-for="item in renderUserList"
            :key="item.value"
            :label="item.label"
            :value="item.value">
          </el-option>
        </el-select>
        <!-- <kap-filter-select class="name-select" :asyn="true" @req="filterGroup" v-model="accessMeta.sids" :disabled="isEditAuthor" multiple :list="renderGroupList"  placeholder="kylinLang.common.pleaseInputUserGroup" :size="100" v-else></kap-filter-select> -->
        <el-select
          class="name-select"
          v-model="accessMeta.sids"
          :disabled="isEditAuthor"
          multiple
          filterable
          remote
          :placeholder="$t('kylinLang.common.pleaseInputUserGroup')"
          :remote-method="filterGroup"
          v-else>
          <el-option
            v-for="item in renderGroupList"
            :key="item.value"
            :label="item.label"
            :value="item.value">
          </el-option>
        </el-select>
        <el-select class="type-select" :placeholder="$t('access')" v-model="accessMeta.permission" size="medium">
          <el-option :label="item.key" :value="item.value" :key="item.value" v-for="item in showMaskByOrder"></el-option>
        </el-select>
        <span class="ky-no-br-space ksd-ml-10" v-if="!isEditAuthor">
          <el-button type="primary" icon="el-icon-ksd-add_2" plain circle size="mini" @click="addAccessMetas" v-if="index==0"></el-button>
          <el-button icon="el-icon-minus" class="ksd-ml-5" plain circle size="mini" :disabled="index==0&&accessMetas.length==1" @click="removeAccessMetas(index)"></el-button>
        </span>
      </div>
      <span slot="footer" class="dialog-footer ky-no-br-space">
        <el-button @click="cancelAuthor" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="submitAuthor" :loading="submitLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { objectClone } from '../../util'
import { handleSuccess, handleError, kapConfirm, hasRole, hasPermissionOfProjectAccess } from '../../util/business'
import { mapActions } from 'vuex'
import { pageCount, permissions } from 'config'
import userAccess from './user_access'
import tableAccess from './table_access'
@Component({
  methods: {
    ...mapActions({
      getUserAndGroups: 'GET_USER_AND_GROUPS',
      getProjectAccess: 'GET_PROJECT_ACCESS',
      delProjectAccess: 'DEL_PROJECT_ACCESS',
      getAvailableUserOrGroupList: 'ACCESS_AVAILABLE_USER_OR_GROUP',
      saveProjectAccess: 'SAVE_PROJECT_ACCESS',
      editProjectAccess: 'EDIT_PROJECT_ACCESS',
      getUserAccessByProject: 'USER_ACCESS'
    })
  },
  components: {
    'user_access': userAccess,
    'table_access': tableAccess
  },
  locales: {
    'en': {
      projectTitle: '{projectName} Authorization',
      back: 'Back',
      userAccess: 'User / Group',
      selectUserAccess: 'Select User / Group',
      userNameOrGroup: 'Filter by user or group name',
      toggleTableView: 'Swtich to Table Operation',
      toggleUserView: 'Swtich to User Operation',
      userOrGroup: 'Name',
      type: 'Type',
      accessType: 'Role',
      accessTables: 'Access Tables',
      author: 'Add User / Group',
      editAuthor: 'Edit User / Group',
      authorTips: 'By default, a user/user group will be automatically granted all access permissions on all tables in this project after added into this project.',
      selectUserOrUserGroups: 'Please select or input the user / user group name.',
      userGroups: 'User Groups',
      users: 'Users',
      tableName: 'Table Name',
      datasourceType: 'Data Source',
      tableType: 'Table',
      deleteAccessTip: 'Please confirm whethter to delete the authorization of {userName} in this project?',
      access: 'Role',
      deleteAccessTitle: 'Delete Access'
    },
    'zh-cn': {
      projectTitle: '{projectName} 权限',
      back: '返回',
      userAccess: '用户/用户组',
      selectUserAccess: '选择用户/用户组',
      userNameOrGroup: '搜索用户或用户组名称',
      toggleTableView: '切换至表操作界面',
      toggleUserView: '切换至用户操作界面',
      userOrGroup: '名称',
      type: '类型',
      accessType: '权限',
      accessTables: '有权限的表',
      author: '添加用户/用户组',
      editAuthor: '编辑用户/用户组',
      authorTips: '默认情况下，用户/用户组被添加至项目后，将自动授予该项目下的所有表及行列的访问权限。',
      selectUserOrUserGroups: '请选择或者输入用户/用户组名称',
      userGroups: '用户组',
      users: '用户',
      tableName: '表名',
      datasourceType: '数据源',
      tableType: '表',
      deleteAccessTip: '请确认是否删除 {userName} 在当前项目的所有访问权限？',
      access: '权限',
      deleteAccessTitle: '删除权限'
    }
  }
})
export default class ProjectAuthority extends Vue {
  userTimer = null
  groupTimer = null
  accessView = 'user'
  userAccessList = []
  tableAccessList = [{name: 'Table A', type: 'Hive', users: []}]
  totalSize = 1
  tableTotalSize = 1
  filterTimer = null
  serarchChar = ''
  searchLoading = false
  pagination = {
    pageSize: pageCount,
    pageOffset: 0
  }
  pagination1 = {
    pageSize: pageCount,
    pageOffset: 0
  }
  authorizationVisible = false
  authorForm = {name: [], editName: '', role: 'Admin'}
  isEditAuthor = false
  authorOptions = [{
    label: this.$t('userGroups'),
    options: []
  }, {
    label: this.$t('users'),
    options: []
  }]
  showMask = {
    1: 'Query',
    16: 'Admin',
    32: 'Management',
    64: 'Operation'
  }
  mask = {
    1: 'READ',
    16: 'ADMINISTRATION',
    32: 'MANAGEMENT',
    64: 'OPERATION'
  }
  accessMetas = [{permission: 16, principal: true, sids: []}]
  userList = []
  groupList = []
  showMaskByOrder = [
    // { key: 'Query', value: 1 },
    // { key: 'Operation', value: 64 },
    // { key: 'Management', value: 32 },
    { key: 'Admin', value: 16 }
  ]
  submitLoading = false
  projectAccess = null
  get hasProjectAdminPermission () {
    return hasPermissionOfProjectAccess(this, this.projectAccess, permissions.ADMINISTRATION.mask)
  }
  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }
  get currentProject () {
    return this.$route.params.projectName
  }
  get authorTitle () {
    return this.isEditAuthor ? this.$t('editAuthor') : this.$t('author')
  }
  get currentProjectId () {
    return this.$route.query.projectId
  }
  get renderUserList () {
    var result = this.userList.filter((user) => {
      let isSelected = false
      for (let i = 0; i < this.accessMetas.length; i++) {
        if (this.accessMetas[i].principal && this.accessMetas[i].sids.indexOf(user) !== -1) {
          isSelected = true
          break
        }
      }
      return !isSelected
    })
    result = result.map((u) => {
      return {label: u, value: u}
    })
    return result
  }
  get renderGroupList () {
    var result = this.groupList.filter((user) => {
      let isSelected = false
      for (let i = 0; i < this.accessMetas.length; i++) {
        if (!this.accessMetas[i].principal && this.accessMetas[i].sids.indexOf(user) !== -1) {
          isSelected = true
          break
        }
      }
      return !isSelected
    })
    result = result.map((u) => {
      return {label: u, value: u}
    })
    return result
  }
  inputFilter () {
    clearInterval(this.filterTimer)
    this.filterTimer = setTimeout(() => {
      this.searchLoading = true
      this.loadAccess().then(() => {
        this.searchLoading = false
      }, () => {
        this.searchLoading = false
      })
    }, 500)
  }
  handleCurrentChange (pager, pageSize) {
    this.pagination.pageOffset = pager
    this.pagination.pageSize = pageSize
    this.loadAccess()
  }
  handleCurrentChange1 (pager, pageSize) {
    this.pagination1.pageOffset = pager
    this.pagination1.pageSize = pageSize
    // this.loadUsers(this.currentGroup)
  }
  removeAccess (id, username, promission, principal) {
    kapConfirm(this.$t('deleteAccessTip', {userName: username}), null, this.$t('deleteAccessTitle')).then(() => {
      this.delProjectAccess({id: this.currentProjectId, aid: id, userName: username, principal: principal}).then((res) => {
        this.initAccessData()
        this.loadAccess()
        this.reloadAvaliableUserAndGroup()
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
      }, (res) => {
        handleError(res)
      })
    })
  }
  toggleView (view) {
    this.accessView = view
  }
  loadUserOrGroup (filterUserName, type) {
    var para = {data: {pageSize: 100, pageOffset: 0, project: this.projectName}}
    if (filterUserName) {
      para.data.name = filterUserName
    }
    para.uuid = this.currentProjectId
    para.type = type
    return this.getAvailableUserOrGroupList(para)
  }
  addAccessMetas () {
    this.accessMetas.unshift({permission: 16, principal: true, sids: []})
  }
  removeAccessMetas (index) {
    this.accessMetas.splice(index, 1)
  }
  filterUser (filterUserName) {
    window.clearTimeout(this.userTimer)
    this.userTimer = setTimeout(() => {
      this.loadUserOrGroup(filterUserName, 'user').then((res) => {
        handleSuccess(res, (data) => {
          this.userList = data.sids
        })
      }, (res) => {
        handleError(res)
      })
    }, 500)
  }
  filterGroup (filterUserName) {
    window.clearTimeout(this.groupTimer)
    this.groupTimer = setTimeout(() => {
      this.loadUserOrGroup(filterUserName, 'group').then((res) => {
        handleSuccess(res, (data) => {
          this.groupList = data.sids
        })
      }, (res) => {
        handleError(res)
      })
    }, 500)
  }
  changeUserType (index) {
    if (this.accessMetas[index].sids.length && !this.isEditAuthor) {
      this.accessMetas[index].sids = []
    }
  }
  authorUser () {
    this.isEditAuthor = false
    this.authorizationVisible = true
  }
  cancelAuthor () {
    this.authorizationVisible = false
  }
  editAuthorUser (row) {
    this.isEditAuthor = true
    this.authorizationVisible = true
    const sids = row.sid.grantedAuthority ? [row.sid.grantedAuthority] : [row.sid.principal]
    this.accessMetas = [{permission: row.permission.mask, principal: row.type === 'User', sids: sids, accessEntryId: row.id}]
  }
  submitAuthor () {
    const accessMetas = objectClone(this.accessMetas)
    accessMetas.filter((acc) => {
      return acc.sids.length && acc.permission
    }).forEach((access) => {
      access.permission = this.mask[access.permission]
    })
    this.submitLoading = true
    let accessData = null
    if (this.isEditAuthor) {
      accessData = accessMetas[0]
      accessData.sid = accessData.sids[0]
      delete accessData.sids
    } else {
      accessData = accessMetas
    }
    const actionType = this.isEditAuthor ? 'editProjectAccess' : 'saveProjectAccess'
    this[actionType]({accessData: accessData, id: this.currentProjectId}).then((res) => {
      handleSuccess(res, (data) => {
        this.submitLoading = false
        this.authorizationVisible = false
        this.initAccessData()
        this.loadAccess()
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.saveSuccess')
        })
        !this.isEditAuthor && this.reloadAvaliableUserAndGroup()
      })
    }, (res) => {
      handleError(res)
      this.submitLoading = false
      this.authorizationVisible = false
    })
  }
  initAccessData () {
    this.accessMetas = [{permission: 16, principal: true, sids: []}]
  }
  reloadAvaliableUserAndGroup () {
    this.filterUser()
    this.filterGroup()
  }
  loadAccess () {
    const para = {
      data: this.pagination,
      projectId: this.currentProjectId
    }
    para.data.name = this.serarchChar
    return this.getProjectAccess(para).then((res) => {
      handleSuccess(res, (data) => {
        this.userAccessList = data.sids
        this.totalSize = data.size
        this.settleAccessList = this.userAccessList && this.userAccessList.map((access) => {
          access.roleOrName = access.sid.grantedAuthority || access.sid.principal
          access.type = access.sid.principal ? 'User' : 'Group'
          access.promission = this.showMask[access.permission.mask]
          access.accessDetails = []
          return access
        }) || []
      })
    }, (res) => {
      handleError(res)
    })
  }
  created () {
    this.loadAccess()
    this.getUserAccessByProject({
      project: this.currentProject,
      notCache: true
    }).then((res) => {
      handleSuccess(res, (data) => {
        this.projectAccess = data
        if (this.hasProjectAdminPermission || this.isAdmin) {
          this.reloadAvaliableUserAndGroup()
        }
      })
    }, (res) => {
      handleError(res)
    })
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  #projectAuth {
    padding: 20px;
    .user-access-table {
      .el-icon-ksd-table_edit,
      .el-icon-ksd-table_delete {
        &:hover {
          color: @base-color;
        }
      }
    }
  }
  .author_dialog {
    .el-alert {
      padding: 0;
      .el-alert__title {
        font-size: 14px;
      }
    }
    .user-group-select {
      .user-select {
        width: 128px;
      }
      .name-select {
        width: 570px;
        margin-left: 5px;
      }
      .type-select {
        width: 150px;
        margin-left: 5px;
      }
    }
  }
  .user-access-block,
  .table-access-block {
    .access-card {
      border: 1px solid @line-border-color;
      background-color: @fff;
      height: 370px;
      .access-title {
        background-color: @background-disabled-color;
        border-bottom: 1px solid @line-border-color;
        height: 36px;
        line-height: 36px;
        color: @text-title-color;
        font-size: 14px;
        font-weight: bold;
        padding: 0 10px;
        .el-checkbox__label {
          color: @text-title-color;
          font-weight: bold;
        }
      }
      .access-search {
        height: 32px;
        line-height: 32px;
        padding: 0 10px;
        border-bottom: 1px solid @line-border-color;
      }
      .access-tips {
        height: 24px;
        line-height: 24px;
        color: @text-title-color;
        background-color: @background-disabled-color;
        padding: 0 10px;
        i {
          color: @text-disabled-color;
        }
      }
      .access-content {
        height: 264px;
        overflow-y: auto;
        position: relative;
        &.all-tips {
          height: 240px;
        }
        &.tree-content {
          height: 300px;
          &.all-tips {
            height: 276px;
          }
        }
        .view-all-tips {
          margin: 0 auto;
          margin-top: 100px;
          font-size: 12px;
          color: @text-title-color;
          width: 80%;
          text-align: center;
        }
        ul {
          overflow-y: auto;
          li {
            height: 30px;
            line-height: 30px;
            padding: 0 10px;
            box-sizing: border-box;
            &.row-list {
              display: table;
              width: 100%;
              border-bottom: 1px solid @line-border-color;
              span {
                word-break: break-all;
                &:first-child {
                  word-break: keep-all;
                }
                &:nth-child(3) {
                  line-height: 1.5;
                  max-height: 55px;
                  overflow: auto;
                  padding-top: 5px;
                }
              }
            }
            &:hover {
              background-color: @base-color-9;
            }
          }
        }
      }
      &.column-card,
      &.row-card {
        margin-left: -1px;
        margin-top: 36px;
        height: 334px;
      }
      &.column-card {
        .el-checkbox__input.is-checked+.el-checkbox__label {
          color: @text-title-color;
        }
      }
      &.row-card {
        .access-content {
          ul li {
            .el-row {
              position: relative;
            }
            .el-col-21 {
              width: calc(~'100% - 42px');
              display: flex;
            }
            .el-col-3 {
              width: 42px;
              height: 18px;
              position: absolute;
              right: 0;
              top: calc(~'50% - 9px');
            }
            .btn-icons {
              text-align: right;
            }
          }
        }
      }
    }
    .expand-footer {
      border-top: 1px solid @line-border-color;
      margin: 15px -15px 0 -15px;
      padding: 10px 15px 0;
    }
  }
</style>
