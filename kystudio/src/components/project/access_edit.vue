<template>
<div class="access_edit">
    <el-button class="ksd-mb-20" type="primary" size="small" @click="addAccess()" v-if="hasProjectAdminPermission()||isAdmin" > ＋ {{$t('grant')}}</el-button>
    <kap-common-popover>
      <div slot="content">
         <h4>{{$t('grantTitle')}}</h4>
        <ul>
          <li>{{$t('grantDetail1')}}</li>
          <li>{{$t('grantDetail2')}}</li>
          <li>{{$t('grantDetail3')}}</li>
          <li>{{$t('grantDetail4')}}</li>
        </ul>
      </div>
      <icon name="question-circle" class="ksd-question-circle"></icon>
    </kap-common-popover>
   <!--  <el-popover ref="popoverGrant" placement="right" trigger="click" width="400">
      <div class="grant-popover">
        <h4>{{$t('grantTitle')}}</h4>
        <ul>
          <li>{{$t('grantDetail1')}}</li>
          <li>{{$t('grantDetail2')}}</li>
          <li>{{$t('grantDetail3')}}</li>
          <li>{{$t('grantDetail4')}}</li>
        </ul>
      </div>
    </el-popover>
    <el-button v-popover:popoverGrant class="ques">?</el-button> -->

      <div v-show="editAccessVisible">
      <el-form :inline="true" :model="accessMeta" ref="accessForm" :rules="rules"  class="demo-form-inline">
       <el-form-item :label="$t('type')">
          <el-select  placeholder="Type" v-model="accessMeta.principal" :disabled="isEdit" @change="changeUserType">
            <el-option label="user" :value="true"></el-option>
            <el-option label="group" :value="false"></el-option>
          </el-select>
        </el-form-item>
         <el-form-item :label="$t('name')" prop="sid" v-show="accessMeta.principal" >
<!--           <el-input  :placeholder="$t('nameAccount')" v-model="accessMeta.sid"></el-input> -->
         <kap-filter-select v-model="accessMeta.sid" :disabled="isEdit" :dataMap="{label: 'username', value: 'username'}" :list="userList" placeholder="kylinLang.common.pleaseInputUserName" :size="100"></kap-filter-select>
         <!--  <el-select filterable :disabled="isEdit" v-model="accessMeta.sid" :placeholder="$t('nameAccount')">
            <el-option
              v-for="item in userList"
              :key="item.username"
              :label="item.username"
              :value="item.username">
            </el-option>
          </el-select> -->
        </el-form-item>
         <el-form-item :label="$t('kylinLang.common.group')" prop="sid" v-show="!accessMeta.principal">
         <!--  <el-select  placeholder="group" :disabled="isEdit" v-model="accessMeta.sid">
            <el-option :label="item" :value="item" v-for="item in groupLists"></el-option>
          </el-select> -->
          <kap-filter-select v-model="accessMeta.sid" :disabled="isEdit"  :list="groupLists" :dataMap="{label: 'label', value:'value'}" placeholder="kylinLang.common.pleaseInputUserGroup" :size="100"></kap-filter-select>
        </el-form-item>
        <el-form-item :label="$t('access')" prop="permission">
          <el-select  :placeholder="$t('access')" v-model="accessMeta.permission">

            <!--<el-option :label="key" :value="+value" v-for="(key, value) in showMask"></el-option>-->
            <el-option :label="item.key" :value="item.value" v-for="item in showMaskByOrder"></el-option>
<!--             <el-option label="Management" :value="32"></el-option>
            <el-option label="OPERATION" :value="64"></el-option>
            <el-option label="Query" :value="1"></el-option> -->
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button  @click="resetAccessEdit">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="saveAccess">{{$t('kylinLang.common.save')}}</el-button>
        </el-form-item>
      </el-form>
    </div>
    <el-table
	    :data="settleAccessList"
	    border
	    style="width: 100%">
	    <el-table-column
	      prop="roleOrName"
	      :label="$t('name')"
	     >
	    </el-table-column>
	    <el-table-column
	      prop="type"
	      :label="$t('type')"
	      >
	    </el-table-column>
	    <el-table-column
	      prop="promission"
	      :label="$t('access')"
	      >
	    </el-table-column>
	   <el-table-column
	      :label="$t('kylinLang.common.action')"
	      width="160">
	      <template scope="scope">
        <span v-if="!(hasProjectAdminPermission()||isAdmin)">N/A</span>
	        <el-button  v-if="hasProjectAdminPermission()||isAdmin"
	          @click="beginEdit(scope.row)"
	          type="blue" size="small">
	          {{$t('kylinLang.common.edit')}}
	        </el-button>
           <el-button  v-if="isAdmin || hasProjectAdminPermission()"
            @click="removeAccess(scope.row.id, scope.row.roleOrName)"
            type="danger" size="small">
            {{$t('kylinLang.common.delete')}}
          </el-button>
	      </template>
	    </el-table-column>
	  </el-table>
   </div>
</template>

<script>
import { mapActions } from 'vuex'
import { permissions } from '../../config/index'
import { objectClone } from '../../util/index'
import { handleSuccess, handleError, hasPermission, hasRole, kapConfirm } from '../../util/business'
export default {
  name: 'access',
  props: ['accessId', 'own', 'projectName'],
  data () {
    return {
      isEdit: false,
      editAccessVisible: false,
      settleAccessList: [],
      hasActionAccess: false,
      accessMeta: {
        permission: 1,
        principal: true,
        sid: ''
      },
      rules: {
        sid: [{
          required: true, message: this.$t('pleaseInput'), trigger: 'blur'
        }]
      },
      accessList: [],
      mask: {
        1: 'READ',
        32: 'MANAGEMENT',
        64: 'OPERATION',
        16: 'ADMINISTRATION'
      },
      showMask: {
        1: 'Query',
        32: 'Management',
        64: 'Operation',
        16: 'Admin'
      },
      showMaskByOrder: [
        { key: 'Query', value: 1 },
        { key: 'Operation', value: 64 },
        { key: 'Management', value: 32 },
        { key: 'Admin', value: 16 }
      ],
      groupList: []
    }
  },
  methods: {
    ...mapActions({
      saveProjectAccess: 'SAVE_PROJECT_ACCESS',
      editProjectAccess: 'EDIT_PROJECT_ACCESS',
      getProjectAccess: 'GET_PROJECT_ACCESS',
      delProjectAccess: 'DEL_PROJECT_ACCESS',
      saveCubeAccess: 'SAVE_CUBE_ACCESS',
      editCubeAccess: 'EDIT_CUBE_ACCESS',
      getCubeAccess: 'GET_CUBE_ACCESS',
      delCubeAccess: 'DEL_CUBE_ACCESS',
      getProjectEndAccess: 'GET_PROJECT_END_ACCESS',
      loadUsersList: 'LOAD_USERS_LIST',
      getGroupList: 'GET_GROUP_LIST'
    }),
    loadAllGroups (data) {
      this.getGroupList({project: localStorage.getItem('selected_project')}).then((res) => {
        handleSuccess(res, (groups) => {
          this.groupList = groups
        })
      })
    },
    changeUserType () {
      if (this.accessMeta.sid !== '' && !this.isEdit) {
        this.accessMeta.sid = ''
      }
    },
    initMeta () {
      this.accessMeta = {
        permission: 1,
        principal: true,
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
        var accessMeta = objectClone(this.accessMeta)
        accessMeta.permission = this.mask[this.accessMeta.permission]
        var actionType = this.own === 'cube' ? 'saveCubeAccess' : 'saveProjectAccess'
        this[actionType]({accessData: accessMeta, id: this.accessId}).then((res) => {
          this.editAccessVisible = false
          this.loadAccess()
          // 需要重新刷新projectlist下的权限
          this.getProjectEndAccess(this.accessId)
          this.$message(this.$t('kylinLang.common.saveSuccess'))
        }, (res) => {
          handleError(res)
        })
      })
    },
    removeAccess (id, username) {
      kapConfirm(this.$t('deleteAccess')).then(() => {
        var actionType = this.own === 'cube' ? 'delCubeAccess' : 'delProjectAccess'
        this[actionType]({id: this.accessId, aid: id, userName: username}).then((res) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.delSuccess')
          })
          this.loadAccess()
          // 需要重新刷新projectlist下的权限
          this.getProjectEndAccess(this.accessId)
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
      accessMeta.permission = this.mask[this.accessMeta.permission]
      this[actionType]({accessData: accessMeta, id: this.accessId}).then((res) => {
        this.editAccessVisible = false
        this.loadAccess()
        // 需要重新刷新projectlist下的权限
        this.getProjectEndAccess(this.accessId)
      }, (res) => {
        handleError(res)
      })
    },
    hasProjectAdminPermission () {
      var pid = this.accessId
      return hasPermission(this, pid, permissions.ADMINISTRATION.mask)
    },
    loadAccess () {
      var actionType = this.own === 'cube' ? 'getCubeAccess' : 'getProjectAccess'
      this[actionType](this.accessId).then((res) => {
        handleSuccess(res, (data) => {
          this.accessList = data
          this.settleAccessList = data && data.map((access) => {
            access.roleOrName = access.sid.grantedAuthority || access.sid.principal
            access.type = access.sid.principal ? 'User' : 'Group'
            access.promission = this.showMask[access.permission.mask]
            return access
          }) || []
        })
      })
    },
    grantDetail () {

    }
  },
  computed: {
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
    userList () {
      let userList = []
      this.$store.state.user.usersList.forEach((user) => {
        if (this.accessUserList && this.accessUserList.users.indexOf(user.username) < 0) {
          userList.push(user)
        }
      })
      return userList
    },
    groupLists () {
      let groupList = []
      this.groupList.forEach((group) => {
        if (this.accessUserList && this.accessUserList.groups.indexOf(group) < 0) {
          groupList.push({label: group, value: group})
        }
      })
      return groupList
    }
  },
  created () {
    this.loadAccess()
    if (this.hasProjectAdminPermission() || this.isAdmin) {
      this.loadAllGroups()
      this.loadUsersList({pageSize: 10000, pageOffset: 0, project: this.projectName})
    }
    if (!this.$store.state.project.projectEndAccess[this.accessId]) {
      this.getProjectEndAccess(this.accessId)
    }
  },
  locales: {
    'en': {grant: 'Grant', type: 'Type', user: 'User', role: 'Role', name: 'User Name', nameAccount: 'user account', permission: 'Permission', cubeAdmin: 'ADMIN', cubeEdit: 'Edit', cubeOpera: 'Operation', cubeQuery: 'cubeQuery', principal: 'Name', access: 'Access', grantTitle: 'What permissions does KAP provide?', grantDetail1: '*QUERY*: Permission to query tables/cubes in the project', grantDetail2: '*OPERATION*: Permission to rebuild, resume and cancel jobs. OPERATION permission includes QUERY.', grantDetail3: '*MANAGEMENT*: Permission to edit/delete cube. MANAGEMENT permission includes OPERATION and QUERY.', grantDetail4: '*ADMIN*: Full access to cube and jobs. ADMIN permission includes MANAGEMENT, OPERATION and QUERY.', deleteAccess: 'the action will delete this access, still continue?', pleaseInput: 'Please input user name.'},
    'zh-cn': {grant: '授权', type: '类型', user: '用户', role: '群组', name: '用户名', nameAccount: '用户账号', permission: '许可', cubeAdmin: '管理', cubeEdit: '编辑', cubeOpera: '操作', cubeQuery: '查询', principal: '名称', access: '权限', grantTitle: 'KAP提供什么样的权限？', grantDetail1: '*QUERY*: 查询项目中的表或者cube的权限', grantDetail2: '*OPERATION*: 构建Cube的权限, 包括恢复和取消任务；OPERATION权限包含QUERY权限。', grantDetail3: '*MANAGEMENT*: 编辑和删除Cube的权限，MANAGEMENT权限包含了OPERATION权限和QUERY权限。', grantDetail4: '*ADMIN*: 对Cube拥有所有权限，ADMIN权限包含了MANAGEMENT权限，OPERATION权限和QUERY权限。', deleteAccess: '此操作将删除该授权，是否继续', pleaseInput: '请填写用户名。'}
  }
}
</script>
<style lang="less">
.access_edit{
  .el-button--text{
    // width: 50px;
    padding: 8px 5px 8px 5px;
  }
  .el-input__inner {
    border:solid 1px #393e53;
    &:focus{
      border:1px solid #7881aa;
    }
  }

}
.grant-popover {
  h4 {
    height:30px;
    line-height: 30px;
    font-size: 14px;
  }
  ul {
    line-height: 20px;
    margin-left: 10px;
    li {
      position: relative;
      padding-left: 10px;
    }
    li:before {
      position: absolute;
      top: 8px;
      left: 0;
      content: '';
      width: 4px;
      height:4px;
      border-radius: 50%;
      background: #333;
    }
  }
}
</style>
