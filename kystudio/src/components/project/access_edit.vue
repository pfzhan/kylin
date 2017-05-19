   <template>
 <div>
    <el-button class="ksd-mb-20" type="primary" @click="addAccess()"> ＋ Grant</el-button>
      <div v-if="editAccessVisible">
      <el-form :inline="true" :model="accessMeta"  class="demo-form-inline">
       <el-form-item label="Type">
          <el-select  placeholder="Type" v-model="accessMeta.principal">
            <el-option label="User" :value="true"></el-option>
            <el-option label="Role" :value="false"></el-option>
          </el-select>
        </el-form-item>
         <el-form-item label="Name" v-if="accessMeta.principal">
          <el-input  placeholder="name" v-model="accessMeta.sid"></el-input>
        </el-form-item>
         <el-form-item label="Role" v-if="!accessMeta.principal">
          <el-select  placeholder="Role" v-model="accessMeta.sid">
            <el-option label="ADMIN" value="ROLE_ADMIN"></el-option>
            <el-option label="MODELER" value="ROLE_MODELER"></el-option>
            <el-option label="ANALYST" value="ROLE_ANALYST"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item label="Permission">
          <el-select  placeholder="Permission" v-model="accessMeta.permission">
            <el-option label="CUBE ADMIN" :value="16"></el-option>
            <el-option label="CUBE EDIT" :value="32"></el-option>
            <el-option label="CUBE OPERATION" :value="64"></el-option>
            <el-option label="CUBE QUERY" :value="1"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item>
          <el-button  @click="resetAccessEdit">Cancel</el-button>
          <el-button type="primary" @click="saveAccess">Save</el-button>
        </el-form-item>
      </el-form>
    </div>
    <el-table
	    :data="settleAccessList"
	    border
	    style="width: 100%">
	    <el-table-column 
	      prop="roleOrName"
	      label="Principal"
	     >
	    </el-table-column>
	    <el-table-column
	      prop="type"
	      label="Type"
	      >
	    </el-table-column>
	    <el-table-column
	      prop="promission"
	      label="Access"
	      >
	    </el-table-column>
	   <el-table-column 
	      label="操作"
	      width="120">
	      <template scope="scope">
	        <el-button 
	          @click="removeAccess(scope.row.id)"
	          type="text"
	          size="small">
	          移除
	        </el-button>
	        <el-button 
	          @click="beginEdit(scope.row)"
	          type="text"
	          size="small">
	          编辑
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
    }
  },
  computed: {
  },
  created () {
    this.loadAccess()
  }
}
</script>
<style scoped="">
</style>
