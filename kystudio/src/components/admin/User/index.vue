<template>
  <div class="security-user">
    <div class="ksd-title-label ksd-mt-20">{{$t('userList')}}</div>
    <!-- 新建/过滤用户 -->
    <el-row class="ksd-mt-10 ksd-mb-10">
      <el-col :span="24">
        <el-button plain type="primary"
          size="medium"
          icon="el-icon-ksd-back"
          v-if="currentGroup"
          @click="$router.push('/admin/group')">
          {{$t('back')}}
        </el-button><el-button
          plain type="primary"
          size="medium"
          icon="el-icon-ksd-add_2"
          v-if="userActions.includes('addUser')"
          @click="editUser('new')">
          {{$t('user')}}
        </el-button>
        <div style="width:200px;" class="ksd-fright">
          <el-input class="show-search-btn"
            size="medium"
            prefix-icon="el-icon-search"
            :placeholder="$t('userName')"
            @input="inputFilter">
          </el-input>
        </div>
      </el-col>
    </el-row>

    <el-alert class="ksd-mb-20" type="info"
      v-if="!isTestingSecurityProfile"
      :title="$t('securityProfileTip')"
      :closable="false">
    </el-alert>

    <el-table :data="usersList" class="user-table" border>
      <!-- 表：username列 -->
      <el-table-column sortable :label="$t('userName')" prop="username" :width="220">
        <template slot-scope="scope">
          <i class="el-icon-ksd-table_admin ksd-fs-14" style="cursor: default;"></i>
          <span>{{scope.row.username}}</span>
        </template>
      </el-table-column>
      <!-- 表：group列 -->
      <el-table-column :label="$t('kylinLang.common.group')">
        <template slot-scope="scope">
          <common-tip :content="scope.row.groups && scope.row.groups.join('<br/>')" placement="top">
              <span>{{scope.row.groups && scope.row.groups.join(',')}}</span>
          </common-tip>
        </template>
      </el-table-column>
      <!-- 表：是否系统管理员列 -->
      <el-table-column :label="$t('admin')" :width="120">
        <template slot-scope="scope">
          <i class="el-icon-ksd-good_health admin-svg" v-if="scope.row.admin"></i>
        </template>
      </el-table-column>
      <!-- 表：status列 -->
      <el-table-column :label="$t('status')" :width="120">
        <template slot-scope="scope">
          <el-tag size="small" type="info" v-if="scope.row.disabled">Disabled</el-tag>
          <el-tag size="small" type="success" v-else>Enabled</el-tag>
        </template>
      </el-table-column>
      <!-- 表：action列 -->
      <el-table-column v-if="isActionShow" :label="$t('action')" :width="87">
        <template slot-scope="scope">
          <el-tooltip :content="$t('resetPassword')" effect="dark" placement="top">
            <i class="el-icon-ksd-table_reset_password ksd-fs-14 ksd-mr-10" v-show="userActions.includes('changePassword') || scope.row.uuid === currentUser.uuid" @click="editUser('password', scope.row)"></i>
          </el-tooltip><span>
          </span><el-tooltip :content="$t('groupMembership')" effect="dark" placement="top">
            <i class="el-icon-ksd-table_group ksd-fs-14 ksd-mr-10" v-show="userActions.includes('assignGroup')" @click="editUser('group', scope.row)"></i>
          </el-tooltip><span>
          </span><common-tip :content="$t('kylinLang.common.moreActions')"><el-dropdown trigger="click">
            <i class="el-icon-ksd-table_others" v-show="isMoreActionShow"></i>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item v-if="userActions.includes('editUser')" @click.native="editUser('edit', scope.row)">{{$t('editRole')}}</el-dropdown-item>
              <el-dropdown-item v-if="userActions.includes('deleteUser')" @click.native="dropUser(scope.row)">{{$t('drop')}}</el-dropdown-item>
              <el-dropdown-item v-if="userActions.includes('disableUser') && scope.row.disabled" @click.native="changeStatus(scope.row)">{{$t('enable')}}</el-dropdown-item>
              <el-dropdown-item v-if="userActions.includes('disableUser') && !scope.row.disabled" @click.native="changeStatus(scope.row)">{{$t('disable')}}</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
          </common-tip>
        </template>
      </el-table-column>
    </el-table>

    <kap-pager
      class="ksd-center ksd-mtb-10" ref="pager"
      :totalSize="totalSize"
      @handleCurrentChange="handleCurrentChange">
    </kap-pager>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapState, mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { pageCount } from 'config'
import { handleError, kapConfirm } from '../../../util'

@Component({
  computed: {
    ...mapState({
      currentUser: (state) => state.user.currentUser
    }),
    ...mapGetters([
      'userActions',
      'currentSelectedProject',
      'isTestingSecurityProfile'
    ])
  },
  methods: {
    ...mapActions({
      removeUser: 'REMOVE_USER',
      loadUsersList: 'LOAD_USERS_LIST',
      loadUserListByGroupName: 'GET_USERS_BY_GROUPNAME',
      updateStatus: 'UPDATE_STATUS'
    }),
    ...mapActions('UserEditModal', {
      callUserEditModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class SecurityUser extends Vue {
  userData = []
  totalSize = 0
  filterTimer = null
  pagination = {
    pageSize: pageCount,
    pageOffset: 0
  }
  get currentGroup () {
    return this.$route.params.groupName
  }
  get isActionShow () {
    return this.userActions.filter(action => ['addUser'].includes(action)).length
  }
  get isMoreActionShow () {
    return this.userActions.filter(action => ['resetPassword', 'addUser'].includes(action)).length
  }
  get usersList () {
    return this.userData.map(user => ({
      username: user.username,
      disabled: user.disabled,
      admin: user.authorities.some(role => role.authority === 'ROLE_ADMIN'),
      modeler: user.authorities.some(role => role.authority === 'ROLE_MODELER'),
      analyst: user.authorities.some(role => role.authority === 'ROLE_ANALYST'),
      defaultPassword: user.defaultPassword,
      authorities: user.authorities,
      groups: user.authorities.map(role => role.authority),
      uuid: user.uuid
    }))
  }

  inputFilter (value) {
    clearInterval(this.filterTimer)

    this.filterTimer = setTimeout(() => {
      this.loadUsers(value)
    }, 1500)
  }

  handleCurrentChange (pager, pageSize) {
    this.pagination.pageOffset = pager
    this.pagination.pageSize = pageSize
    this.loadUsers(this.currentGroup)
  }

  async loadUsers (name) {
    try {
      const parameter = {
        ...this.pagination,
        project: this.currentSelectedProject,
        name,
        groupName: this.currentGroup
      }

      const { data: { data } } = !this.currentGroup
        ? await this.loadUsersList(parameter)
        : await this.loadUserListByGroupName(parameter)

      this.userData = data.users || data.groupMembers || []
      this.totalSize = data.size
    } catch (e) {
      handleError(e)
    }
  }

  async editUser (editType, userDetail) {
    const isSubmit = await this.callUserEditModal({ editType, userDetail })
    isSubmit && this.loadUsers(this.currentGroup)
  }

  async dropUser (userDetail) {
    try {
      const { username } = userDetail

      await kapConfirm(this.$t('cofirmDelUser', {userName: username}), null, this.$t('delUserTitle'))
      await this.removeUser(username)

      this.loadUsers()
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
    } catch (e) {
      e !== 'cancel' && handleError(e)
    }
  }

  async changeStatus (userDetail) {
    try {
      await this.updateStatus({
        uuid: userDetail.uuid,
        username: userDetail.username,
        disabled: !userDetail.disabled
      })
      this.loadUsers()
    } catch (e) {
      handleError(e)
    }
  }

  mounted () {
    this.loadUsers(this.currentGroup)
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.security-user {
  padding: 0 20px;
  .el-icon-ksd-good_health {
    color: @color-success;
    cursor: default;
  }
  .user-table {
    .el-icon-ksd-table_reset_password:hover,
    .el-icon-ksd-table_group:hover,
    .el-icon-ksd-table_others:hover {
      color: @base-color;
    }
  }
}
</style>
