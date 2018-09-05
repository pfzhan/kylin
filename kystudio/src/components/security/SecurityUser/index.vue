<template>
  <div class="security-user">
    <!-- 从Group页面跳至User页面的返回按钮 -->
    <div v-if="currentGroup">
      <router-link class="el-icon-ksd-more_04" to="/security/group">
        {{$t('backToGroupList')}}
      </router-link>
    </div>
    <!-- 新建/过滤用户 -->
    <el-row class="ksd-mt-10 ksd-mb-14">
      <el-col :span="24">
        <el-button plain type="primary"
          size="medium"
          icon="el-icon-plus"
          v-if="mixAccess"
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

    <el-table :data="usersList" border>
      <!-- 表：username列 -->
      <el-table-column sortable :label="$t('userName')" prop="username" :width="220">
        <template slot-scope="scope">
          <i class="el-icon-ksd-table_admin ksd-fs-14" style="cursor: default;"></i>
          <span class="ksd-ml-4">{{scope.row.username}}</span>
        </template>
      </el-table-column>
      <!-- 表：group列 -->
      <el-table-column :label="$t('kylinLang.common.group')" sortable>
        <template slot-scope="scope">
          <common-tip :content="scope.row.groups && scope.row.groups.join('<br/>')" placement="top">
              <span>{{scope.row.groups && scope.row.groups.join(',')}}</span>
          </common-tip>
        </template>
      </el-table-column>
      <!-- 表：是否系统管理员列 -->
      <el-table-column :label="$t('admin')" :width="120">
        <template slot-scope="scope">
          <i class="el-icon-ksd-right admin-svg" v-if="scope.row.admin"></i>
        </template>
      </el-table-column>
      <!-- 表：status列 -->
      <el-table-column :label="$t('status')" :width="120">
        <template slot-scope="scope">
          <el-tag size="small" type="primary" v-if="scope.row.disabled">Disabled</el-tag>
          <el-tag size="small" type="success" v-else>Enabled</el-tag>
        </template>
      </el-table-column>
      <!-- 表：action列 -->
      <el-table-column v-if="mixAccess" :label="$t('action')" :width="100">
        <template slot-scope="scope">
          <el-tooltip :content="$t('resetPassword')" effect="dark" placement="top">
            <i class="el-icon-ksd-table_reset-password ksd-mr-14" v-show="isAdminRole || isProjectManager" @click="editUser('password', scope.row)"></i>
          </el-tooltip>
          <el-dropdown trigger="click" >
            <i class="el-icon-ksd-table_others" v-show="scope.row.username!=='ADMIN'"></i>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item @click.native="editUser('group', scope.row)">{{$t('groupMembership')}}</el-dropdown-item>
              <el-dropdown-item @click.native="editUser('edit', scope.row)">{{$t('editRole')}}</el-dropdown-item>
              <el-dropdown-item @click.native="dropUser(scope.row)">{{$t('drop')}}</el-dropdown-item>
              <el-dropdown-item @click.native="changeStatus(scope.row)" v-if="scope.row.disabled">{{$t('enable')}}</el-dropdown-item>
              <el-dropdown-item @click.native="changeStatus(scope.row)" v-else>{{$t('disable')}}</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </template>
      </el-table-column>
    </el-table>

    <pager class="ksd-center" ref="pager"
      :totalSize="totalSize"
      @handleCurrentChange="pageCurrentChange">
    </pager>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { pageCount } from 'config'
import { handleError, kapConfirm } from '../../../util'

@Component({
  computed: {
    ...mapGetters([
      'isAdminRole',
      'isProjectAdmin',
      'isProjectManager',
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

  get mixAccess () {
    return this.isTestingSecurityProfile && (this.isProjectAdmin || this.isAdminRole)
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
      groups: user.authorities.map(role => role.authority)
    }))
  }

  inputFilter (value) {
    clearInterval(this.filterTimer)

    this.filterTimer = setTimeout(() => {
      this.loadUsers(value)
    }, 1500)
  }

  pageCurrentChange (pager) {
    this.pagination.pageOffset = pager - 1
    this.loadUsers(this.currentGroup)
  }

  async loadUsers (name) {
    try {
      // for newten
      // const parameter = {
      //   ...this.pagination,
      //   project: this.currentSelectedProject,
      //   name,
      //   groupName: this.currentGroup
      // }

      // const { data: { data } } = !this.currentGroup
      //   ? await this.loadUsersList(parameter)
      //   : await this.loadUserListByGroupName(parameter)

      // this.userData = data.users || data.groupMembers || []
      // this.totalSize = data.size
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

      await kapConfirm(this.$t('cofirmDelUser'))
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
        name: userDetail.username,
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
}
</style>
