<template>
<div class="access_edit">
    <el-button class="ksd-mb-20" type="primary" @click="addAccess()"> ＋ {{$t('grant')}}</el-button>
    <el-popover ref="popoverGrant" placement="right" trigger="click" width="400">
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
    <el-button v-popover:popoverGrant class="ques">?</el-button>

      <div v-if="editAccessVisible">
      <el-form :inline="true" :model="accessMeta"  class="demo-form-inline">
       <el-form-item :label="$t('type')">
          <el-select  placeholder="Type" v-model="accessMeta.principal">
            <el-option :label="$t('user')" :value="true"></el-option>
            <el-option :label="$t('role')" :value="false"></el-option>
          </el-select>
        </el-form-item>
         <el-form-item :label="$t('name')" v-if="accessMeta.principal">
          <el-input  :placeholder="$t('nameAccount')" v-model="accessMeta.sid"></el-input>
        </el-form-item>
         <el-form-item label="Role" v-if="!accessMeta.principal">
          <el-select  placeholder="Role" v-model="accessMeta.sid">
            <el-option label="ADMIN" value="ROLE_ADMIN"></el-option>
            <el-option label="MODELER" value="ROLE_MODELER"></el-option>
            <el-option label="ANALYST" value="ROLE_ANALYST"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('permission')">
          <el-select  :placeholder="$t('permission')" v-model="accessMeta.permission">
            <el-option :label="$t('cubeAdmin')" :value="16"></el-option>
            <el-option :label="$t('cubeEdit')" :value="32"></el-option>
            <el-option :label="$t('cubeOpera')" :value="64"></el-option>
            <el-option :label="$t('cubeQuery')" :value="1"></el-option>
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
	      :label="$t('principal')"
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
	      width="120">
	      <template scope="scope">
	        <el-button 
	          @click="removeAccess(scope.row.id)"
	          type="text">
	          {{$t('kylinLang.common.delete')}}
	        </el-button>
	        <el-button 
	          @click="beginEdit(scope.row)"
	          type="text">
	          {{$t('kylinLang.common.edit')}}
	        </el-button>
	      </template>
	    </el-table-column>
	  </el-table>
   </div>
</template>

<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, hasPermission, hasRole } from '../../util/business'
export default {
  name: 'access',
  props: ['projectId'],
  data () {
    return {
      editAccessVisible: false,
      settleAccessList: [],
      hasActionAccess: false,
      accessMeta: {
        permission: 1,
        principal: true,
        id: ''
      },
      accessList: [],
      mask: {
        '1': 'READ',
        '32': 'MANAGEMENT',
        '64': 'OPERATION',
        '16': 'ADMINISTRATION'
      }
    }
  },
  methods: {
    ...mapActions({
      save: 'SAVE_PROJECT_ACCESS',
      editAccess: 'EDIT_PROJECT_ACCESS',
      getAccess: 'GET_PROJECT_ACCESS',
      delAccess: 'DEL_PROJECT_ACCESS'
    }),
    initMeta () {
      this.accessMeta = {
        permission: 1,
        principal: true,
        sid: ''
      }
    },
    addAccess () {
      this.editAccessVisible = true
      this.initMeta()
    },
    resetAccessEdit () {
      this.initMeta()
      this.editAccessVisible = false
    },
    saveAccess () {
      this.accessMeta.permission = this.mask[this.accessMeta.permission]
      this.save({accessData: this.accessMeta, projectId: this.projectId}).then((res) => {
        this.editAccessVisible = false
        this.loadAccess()
        this.$message('添加成功！')
      }, (res) => {
        handleError(res, (data, code, status, msg) => {
          this.$message.error(msg)
        })
      })
    },
    removeAccess (id) {
      this.$confirm('此操作将永久删除该Access, 是否继续?', '提示', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning'
      }).then(() => {
        this.delAccess({projectId: this.projectId, aid: id}).then((res) => {
          this.$message({
            type: 'success',
            message: '删除成功!'
          })
          this.loadAccess()
        }, (res) => {
          handleError(res, (data, code, status, msg) => {
            this.$message.error(msg)
          })
        })
      })
    },
    beginEdit (data) {
      this.editAccessVisible = true
      this.initMeta()
      this.accessMeta.permission = data.permission.mask
      this.accessMeta.sid = data.roleOrName
      this.accessMeta.principal = !data.sid.grantedAuthority
      // this.$set(this.permission)
    },
    updateAccess () {
      this.$emit('updateAccess', this.accessMeta, () => {
        this.editAccessVisible = false
      })
    },
    loadAccess () {
      this.getAccess(this.projectId).then((res) => {
        handleSuccess(res, (data) => {
          this.accessList = data
          this.hasActionAccess = hasRole(this, 'ROLE_ADMIN') || hasPermission(this, this.projectId, 16)
          this.settleAccessList = data && data.map((access) => {
            access.roleOrName = access.sid.grantedAuthority || access.sid.principal
            access.type = access.sid.principal ? 'User' : 'Role'
            access.promission = this.mask[access.permission.mask]
            return access
          }) || []
        })
      })
    },
    grantDetail () {

    }
  },
  computed: {
  },
  created () {
    this.loadAccess()
  },
  locales: {
    'en': {grant: 'Grant', type: 'type', user: 'User', role: 'Role', name: 'Name', nameAccount: 'user account', permission: 'Permission', cubeAdmin: 'CUBE ADMIN', cubeEdit: 'Cube Edit', cubeOpera: 'Cube Operation', cubeQuery: 'cubeQuery', principal: 'Principal', access: 'Access', grantTitle: 'What does access mean to cube?', grantDetail1: 'CUBE QUERY: Access to query cube', grantDetail2: 'CUBE OPERATION: Access to rebuild, resume and cancel jobs. Also include access of CUBE QUERY.', grantDetail3: 'CUBE MANAGEMENT: Access to edit/delete cube. Also include access of CUBE OPERATION.', grantDetail4: 'CUBE ADMIN: Full access to cube and jobs, including access management.'},
    'zh-cn': {grant: '授权', type: '类型', user: '用户', role: '群组', name: '名称', nameAccount: '用户账号', permission: '许可', cubeAdmin: 'Cube 管理', cubeEdit: 'Cube 编辑', cubeOpera: 'Cube 操作', cubeQuery: 'Cube 查询', principal: '名称', access: '权限', grantTitle: '什么是Cube权限', grantDetail1: 'CUBE QUERY: 查询Cube的权限', grantDetail2: 'CUBE OPERATION: 构建Cube的权限, 包括恢复和取消任务；包含查询权限.', grantDetail3: 'CUBE MANAGEMENT: 编辑和删除Cube的权限, 包含了构建Cube的权限.', grantDetail4: 'CUBE ADMIN: 对Cube拥有所有权限.'}
  }
}
</script>
<style lang="less">
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
