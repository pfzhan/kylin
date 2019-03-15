<template>
<div class="access_edit">
    <el-button class="ksd-mb-10" type="primary" plain size="small" @click="addAccess()" v-if="projectActions.includes('addGrant')" icon="el-icon-plus">{{$t('grant')}}</el-button>
    <div style="width:200px;" class="ksd-mb-10 ksd-fright">
      <el-input
        :placeholder="$t('kylinLang.common.userOrGroup')"
        @change="searchAccess"
        v-model="serarchChar"
        class="show-search-btn"
        :suffix-icon="searchLoading ? 'el-icon-loading':'el-icon-search'"
        size="small">
      </el-input>
    </div>
    <el-dialog
      :title="$t('grant')"
      width="660px"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      :visible.sync="editAccessVisible">
      <div class="grantTips">
        <h4>{{$t('grantTitle')}}</h4>
        <ul>
          <!-- <li>{{$t('grantDetail1')}}</li>
          <li>{{$t('grantDetail2')}}</li>
          <li>{{$t('grantDetail3')}}</li> -->
          <li>{{$t('grantDetail4')}}</li>
        </ul>
      </div>
      <div>
        <el-form :inline="true" :model="accessMeta" ref="accessForm" :rules="rules"  class="demo-form-inline" label-position="right" label-width="50px">
         <el-form-item :label="$t('type')" prop="principal"  style="margin-bottom: 20px;">
            <el-select placeholder="Type" v-model="accessMeta.principal" :disabled="isEdit" @change="changeUserType" size="medium">
              <el-option label="user" :value="true"></el-option>
              <el-option label="group" :value="false"></el-option>
            </el-select>
          </el-form-item>
           <el-form-item prop="sid" v-if="accessMeta.principal" style="margin-bottom: 20px;">
             <kap-filter-select :asyn="true" @req="filterUser" v-model="accessMeta.sid" :disabled="isEdit"  :list="renderUserList" placeholder="kylinLang.common.pleaseInputUserName" :size="100"></kap-filter-select>
          </el-form-item>
           <el-form-item prop="sid" v-if="!accessMeta.principal" style="margin-bottom: 20px;">
            <kap-filter-select :asyn="true" @req="filterGroup" v-model="accessMeta.sid" :disabled="isEdit"  :list="renderGroupList"  placeholder="kylinLang.common.pleaseInputUserGroup" :size="100"></kap-filter-select>
          </el-form-item>
          <el-form-item :label=" $t('access')" prop="permission">
            <el-select  :placeholder="$t('access')" v-model="accessMeta.permission" size="medium">
              <el-option :label="item.key" :value="item.value" :key="item.value" v-for="item in showMaskByOrder"></el-option>
            </el-select>
          </el-form-item>
        </el-form>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button size="medium" @click="resetAccessEdit">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain size="medium" :loading="btnLoad" @click="saveAccess">{{$t('kylinLang.common.submit')}}</el-button>
      </span>
    </el-dialog>
    <el-table
	    :data="settleAccessList"
	    border
	    style="width: 100%">
	    <el-table-column
	      prop="roleOrName"
	      :label="$t('kylinLang.common.userOrGroup')"
	     >
        <template slot-scope="scope">
          <i v-show="scope.row.type === 'User'" class="el-icon-ksd-table_admin"></i>
          <i v-show="scope.row.type === 'Group'" class="el-icon-ksd-table_group"></i>
          &nbsp;{{ scope.row.roleOrName}}
        </template>
	    </el-table-column>
	    <el-table-column
	      prop="type"
	      :label="$t('type')"
        header-align="center"
	      >
	    </el-table-column>
	    <el-table-column
	      prop="promission"
	      :label="$t('access')"
        header-align="center"
	      >
	    </el-table-column>
	   <el-table-column
	      :label="$t('kylinLang.common.action')"
        header-align="center"
	      width="83">
	      <template slot-scope="scope">
          <span v-if="!projectActions.includes('editGrant') && !projectActions.includes('deleteGrant')">N/A</span>
          <i class="el-icon-ksd-table_edit ksd-fs-14 ksd-mr-10"
            v-if="projectActions.includes('editGrant')"
            @click="beginEdit(scope.row)">
          </i><span>
          </span><i class="el-icon-ksd-table_delete ksd-fs-14"
            v-if="projectActions.includes('deleteGrant')"
            @click="removeAccess(scope.row.id, scope.row.roleOrName)">
          </i>
	      </template>
	    </el-table-column>
	  </el-table>
    <el-pagination
      background
      layout="prev, pager, next"
      :total="accessSize"
      :currentPage='currentPage'
      @current-change="pageCurrentChange"
      class="ksd-center ksd-mt-10"
      v-show="accessSize">
    </el-pagination>
   </div>
</template>

<script>
import { mapGetters, mapActions } from 'vuex'
import { permissions, pageCount } from '../../config/index'
import { objectClone } from '../../util/index'
import { handleSuccess, handleError, hasPermissionOfProjectAccess, hasRole, kapConfirm } from '../../util/business'
export default {
  name: 'access',
  props: ['accessId', 'own', 'projectName'],
  data () {
    return {
      searchLoading: false,
      ST: null,
      serarchChar: '',
      isEdit: false,
      currentPage: 1,
      editAccessVisible: false,
      settleAccessList: [],
      accessSize: 0,
      hasActionAccess: false,
      accessMeta: {
        permission: 16,
        principal: true,
        sid: ''
      },
      rules: {
        principal: [{
          required: true, message: this.$t('pleaseInput'), trigger: 'blur'
        }],
        sid: [{
          required: true, message: this.$t('pleaseInput'), trigger: 'blur'
        }]
      },
      accessList: [],
      mask: {
        // 1: 'READ',
        // 32: 'MANAGEMENT',
        // 64: 'OPERATION',
        16: 'ADMINISTRATION'
      },
      showMask: {
        // 1: 'Query',
        // 32: 'Management',
        // 64: 'Operation',
        16: 'Admin'
      },
      showMaskByOrder: [
        // { key: 'Query', value: 1 },
        // { key: 'Operation', value: 64 },
        // { key: 'Management', value: 32 },
        { key: 'Admin', value: 16 }
      ],
      groupList: [],
      userList: [],
      btnLoad: false,
      projectAccess: null
    }
  },
  methods: {
    ...mapActions({
      saveProjectAccess: 'SAVE_PROJECT_ACCESS',
      editProjectAccess: 'EDIT_PROJECT_ACCESS',
      getProjectAccess: 'GET_PROJECT_ACCESS',
      delProjectAccess: 'DEL_PROJECT_ACCESS',
      loadUsersList: 'LOAD_USERS_LIST',
      getGroupList: 'GET_GROUP_LIST',
      getAvailableUserOrGroupList: 'ACCESS_AVAILABLE_USER_OR_GROUP',
      getUserAccessByProject: 'USER_ACCESS'
    }),
    getFilterList (query) {
      this.loadUser(query)
    },
    loadUserOrGroup (filterUserName, type) {
      var para = {data: {pageSize: 100, pageOffset: 0, project: this.projectName}}
      if (filterUserName) {
        para.data.name = filterUserName
      }
      para.uuid = this.accessId
      para.type = type
      return this.getAvailableUserOrGroupList(para)
    },
    filterUser (filterUserName) {
      this.loadUserOrGroup(filterUserName, 'user').then((res) => {
        handleSuccess(res, (data) => {
          this.userList = data.sids
        })
      }, (res) => {
        handleError(res)
      })
    },
    filterGroup (filterUserName) {
      this.loadUserOrGroup(filterUserName, 'group').then((res) => {
        handleSuccess(res, (data) => {
          this.groupList = data.sids
        })
      }, (res) => {
        handleError(res)
      })
    },
    changeUserType () {
      if (this.accessMeta.sid !== '' && !this.isEdit) {
        this.accessMeta.sid = ''
      }
    },
    initMeta () {
      this.accessMeta = {
        permission: 16,
        principal: this.accessMeta.principal,
        sid: ''
      }
    },
    addAccess () {
      this.isEdit = false
      // this.$refs.accessForm.resetFields()
      this.editAccessVisible = true
      this.initMeta()
    },
    resetAccessEdit () {
      this.btnLoad = false
      this.initMeta()
      // this.$refs.accessForm.resetFields()
      this.editAccessVisible = false
    },
    saveAccess () {
      this.$refs.accessForm.validate((valid) => {
        if (!valid) {
          return
        }
        if (this.accessMeta.accessEntryId >= 0) {
          this.updateAccess()
          return
        }
        this.btnLoad = true
        var accessMeta = objectClone(this.accessMeta)
        accessMeta.permission = this.mask[this.accessMeta.permission]
        var actionType = 'saveProjectAccess'
        this[actionType]({accessData: accessMeta, id: this.accessId}).then((res) => {
          this.initMeta()
          this.btnLoad = false
          this.editAccessVisible = false
          this.loadAccess()
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.saveSuccess')
          })
          this.reloadAvaliableUserAndGroup()
        }, (res) => {
          this.btnLoad = false
          handleError(res)
        })
      })
    },
    removeAccess (id, username) {
      kapConfirm(this.$t('deleteAccess')).then(() => {
        var actionType = 'delProjectAccess'
        this[actionType]({id: this.accessId, aid: id, userName: username}).then((res) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.delSuccess')
          })
          this.loadAccess().then(() => {
            this.reloadAvaliableUserAndGroup()
          })
        }, (res) => {
          handleError(res)
        })
      })
    },
    beginEdit (data) {
      this.isEdit = true
      // this.$refs.accessForm.resetFields()
      this.editAccessVisible = true
      this.initMeta()
      this.accessMeta.accessEntryId = data.id
      this.accessMeta.permission = data.permission.mask
      this.accessMeta.sid = data.roleOrName
      this.accessMeta.principal = !data.sid.grantedAuthority
      // this.$set(this.permission)
    },
    updateAccess () {
      var actionType = 'editProjectAccess'
      var accessMeta = objectClone(this.accessMeta)
      this.btnLoad = true
      accessMeta.permission = this.mask[this.accessMeta.permission]
      this[actionType]({accessData: accessMeta, id: this.accessId}).then((res) => {
        this.initMeta()
        this.btnLoad = false
        this.editAccessVisible = false
        this.loadAccess()
      }, (res) => {
        this.btnLoad = false
        handleError(res)
      })
    },
    loadAccess () {
      var para = {
        data: {pageOffset: this.currentPage - 1, pageSize: pageCount},
        projectId: this.accessId
      }
      if (this.serarchChar) {
        para.data.name = this.serarchChar
      }
      return this.getProjectAccess(para).then((res) => {
        handleSuccess(res, (data) => {
          this.accessList = data.sids
          this.accessSize = data.size
          this.settleAccessList = this.accessList && this.accessList.map((access) => {
            access.roleOrName = access.sid.grantedAuthority || access.sid.principal
            access.type = access.sid.principal ? 'User' : 'Group'
            access.promission = this.showMask[access.permission.mask]
            return access
          }) || []
        })
      })
    },
    searchAccess () {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        this.searchLoading = true
        this.loadAccess().then(() => {
          this.searchLoading = false
        }, () => {
          this.searchLoading = false
        })
      }, 500)
    },
    pageCurrentChange (currentPage) {
      this.currentPage = currentPage
      this.loadAccess()
    },
    reloadAvaliableUserAndGroup () {
      this.filterUser()
      this.filterGroup()
    }
  },
  computed: {
    ...mapGetters([
      'projectActions'
    ]),
    hasProjectAdminPermission () {
      return hasPermissionOfProjectAccess(this, this.projectAccess, permissions.ADMINISTRATION.mask)
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    },
    accessUserList () {
      let userList = {
        users: [],
        groups: []
      }
      this.settleAccessList.forEach((user) => {
        if (user.type === 'User') {
          userList.users.push(user.roleOrName)
        } else {
          userList.groups.push(user.roleOrName)
        }
      })
      return userList
    },
    renderUserList () {
      var result = []
      this.userList.forEach((u) => {
        result.push({label: u, value: u})
      })
      return result
    },
    renderGroupList () {
      var result = []
      this.groupList.forEach((u) => {
        result.push({label: u, value: u})
      })
      return result
    }
  },
  created () {
    this.loadAccess()
    this.getUserAccessByProject({
      project: this.projectName,
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
  },
  locales: {
    'en': {grant: 'Grant', type: 'Type', user: 'User', role: 'Role', name: 'User Name', nameAccount: 'user account', permission: 'Permission', cubeAdmin: 'ADMIN', cubeEdit: 'Edit', cubeOpera: 'Operation', cubeQuery: 'cubeQuery', principal: 'Name', access: 'Access', grantTitle: 'What permissions does Kyligence Enterprise provide?', grantDetail1: '*QUERY*: Permission to query tables/cubes in the project', grantDetail2: '*OPERATION*: Permission to rebuild, resume and cancel jobs. OPERATION permission includes QUERY.', grantDetail3: '*MANAGEMENT*: Permission to edit/delete cube. MANAGEMENT permission includes OPERATION and QUERY.', grantDetail4: '*ADMIN*: Full access to project. ADMIN permission includes MANAGEMENT, OPERATION and QUERY.', deleteAccess: 'the action will delete this access, still continue?', pleaseInput: 'Please input user name.'},
    'zh-cn': {grant: '授权', type: '类型', user: '用户', role: '群组', name: '用户名', nameAccount: '用户账号', permission: '许可', cubeAdmin: '管理', cubeEdit: '编辑', cubeOpera: '操作', cubeQuery: '查询', principal: '名称', access: '权限', grantTitle: 'Kyligence Enterprise提供什么样的权限？', grantDetail1: '*QUERY*: 查询项目中的表或者cube的权限', grantDetail2: '*OPERATION*: 构建Cube的权限, 包括恢复和取消任务；OPERATION权限包含QUERY权限。', grantDetail3: '*MANAGEMENT*: 编辑和删除Cube的权限，MANAGEMENT权限包含了OPERATION权限和QUERY权限。', grantDetail4: '*ADMIN*: 对项目拥有所有权限，ADMIN权限包含了MANAGEMENT权限，OPERATION权限和QUERY权限。', deleteAccess: '此操作将删除该授权，是否继续', pleaseInput: '请填写用户名。'}
  }
}
</script>
<style lang="less">
.access_edit{
  .el-button--text{
    padding: 8px 5px 8px 5px;
  }
  .demo-form-inline .el-form-item {
    margin-bottom: 20px;
    &:last-child {
      display: block;
    }
    .el-input__inner {
      height: 32px;
    }
  }
  .el-pagination {
    padding: 0;
  }
}
.grantTips {
  margin-bottom: 20px;
  h4 {
    height:20px;
    line-height: 10px;
    font-size: 14px;
    font-weight: normal;
  }
  ul {
    line-height: 20px;
    li {
      position: relative;
      font-size: 12px;
    }
  }
}
</style>
