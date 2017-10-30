<template>
    <div class="access">
       <el-button type="blue" icon="plus" @click="addGrant" v-show="hasSomeProjectPermission || isAdmin">{{$t('grant')}}</el-button>
       <div style="width:200px;" class="ksd-mb-10 ksd-fright">
          <el-input :placeholder="$t('userName')" icon="search" v-model="serarchChar" class="show-search-btn" >
          </el-input>
        </div>
       <el-table class="ksd-mt-20"
            border
            v-if="pagerAclTableList.length > 0"
            :data="pagerAclTableList"
            style="width: 100%">
            <el-table-column
              show-overflow-tooltip
              sortable
              prop="name"
              :label="$t('userName')"
              width="180"
              >
            </el-table-column>
            <el-table-column
              :label="$t('access')"
              >
              <template scope="scope">Query</template>
            </el-table-column>
            <el-table-column v-if="hasSomeProjectPermission || isAdmin"
              width="100"
              prop="Action"
              :label="$t('kylinLang.common.action')">
              <template scope="scope">
              <el-button size="mini" class="ksd-btn del" icon="delete" @click="delAclOfTable(scope.row.name)"></el-button>
              </template>
            </el-table-column>
          </el-table>
          <pager class="ksd-center" :totalSize="totalLength" v-on:handleCurrentChange='pageCurrentChange' ref="pager"></pager>
          <el-dialog :title="$t('grant')" :visible.sync="addGrantDialog"  size="tiny" @close="closeDialog" :close-on-press-escape="false" :close-on-click-modal="false">
              <el-form :model="grantObj" ref="aclOfTableForm" :rules="aclTableRules">
                <el-form-item :label="$t('userName')" label-width="90px" prop="name">
                  <el-select filterable v-model="grantObj.name" style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelectUserName')">
                    <el-option v-for="b in aclBlackList" :value="b.value">{{b.value}}</el-option>
                  </el-select>
                  <!-- <el-autocomplete    :fetch-suggestions="querySearchAsync"></el-autocomplete> -->
                  <!-- <el-input v-model="grantObj.name"  auto-complete="off" placeholder="UserName"></el-input> -->
                </el-form-item>
                <el-form-item :label="$t('access')" label-width="90px" class="is-required">
                  <el-select v-model="grantObj.access">
                    <el-option label="Query" :value="1"></el-option>
                  </el-select>
                </el-form-item>
              </el-form>
              <div slot="footer" class="dialog-footer">
                <el-button @click="addGrantDialog = false">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="primary" :loading="saveBtnLoad" @click="saveAclTable">{{$t('kylinLang.common.save')}}</el-button>
              </div>
          </el-dialog>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, kapConfirm, hasRole, hasPermission } from '../../../util/business'
import { permissions } from '../../../config'
// import { permissions } from '../../config'
// import { changeDataAxis, isFireFox } from '../../util/index'
// import createKafka from '../kafka/create_kafka'
// import editKafka from '../kafka/edit_kafka'
// import viewKafka from '../kafka/view_kafka'
// import arealabel from 'components/common/area_label'
// import Scrollbar from 'smooth-scrollbar'
export default {
  name: 'tableAccess',
  data () {
    return {
      addGrantDialog: false,
      grantObj: {
        name: '',
        access: 1
      },
      currentPage: 1,
      serarchChar: '',
      aclTableData: [],
      aclBlackList: [],
      saveBtnLoad: false,
      aclTableRules: {
        name: [{
          required: true, message: this.$t('kylinLang.common.pleaseSelectUserName'), trigger: 'change'
        }]
      }
    }
  },
  components: {
  },
  created () {
  },
  methods: {
    ...mapActions({
      getAclSetOfTable: 'GET_ACL_SET_TABLE',
      saveAclSetOfTable: 'SAVE_ACL_SET_TABLE',
      delAclSetOfTable: 'DEL_ACL_SET_TABLE',
      getAclBlackList: 'GET_ACL_BLACKLIST_TABLE'
    }),
    closeDialog () {
      this.$refs.aclOfTableForm.resetFields()
    },
    delAclOfTable (userName) {
      kapConfirm(this.$t('delConfirm'), {cancelButtonText: this.$t('cancelButtonText'), confirmButtonText: this.$t('confirmButtonText')}).then(() => {
        this.delAclSetOfTable({
          tableName: this.tableName,
          project: this.$store.state.project.selected_project,
          userName: userName
        }).then((res) => {
          handleSuccess(res, (data) => {
            this.getAllAclSetOfTable()
            this.$message({message: this.$t('delSuccess'), type: 'success'})
          })
        }, (res) => {
          handleError(res)
        })
      })
    },
    resetAclTableObj () {
      this.grantObj = {
        name: '',
        access: 1
      }
    },
    addGrant () {
      this.addGrantDialog = true
      this.resetAclTableObj()
    },
    saveAclTable () {
      this.$refs.aclOfTableForm.validate((valid) => {
        if (valid) {
          this.saveBtnLoad = true
          this.saveAclSetOfTable({
            tableName: this.tableName,
            project: this.$store.state.project.selected_project,
            userName: this.grantObj.name
          }).then((res) => {
            this.saveBtnLoad = false
            this.addGrantDialog = false
            // handleSuccess(res, (data) => {})
            this.getAllAclSetOfTable()
            this.$message({message: this.$t('saveSuccess'), type: 'success'})
          }, (res) => {
            this.saveBtnLoad = false
            // this.addGrantDialog = false
            handleError(res)
          })
        }
      })
    },
    pageCurrentChange (curpage) {
      this.currentPage = curpage
    },
    getAllAclSetOfTable () {
      this.getAclSetOfTable({
        tableName: this.tableName,
        project: this.$store.state.project.selected_project
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.aclTableData = data
          this.getBlackListOfTable()
        })
      }, (res) => {
        handleError(res)
      })
    },
    getBlackListOfTable (cb) {
      this.getAclBlackList({
        tableName: this.tableName,
        project: this.$store.state.project.selected_project
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.aclBlackList = data
          var result = []
          data.forEach((d) => {
            result.push({value: d})
          })
          this.aclBlackList = result
        })
      }, (res) => {
        handleError(res)
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
    }
  },
  computed: {
    tableName () {
      var curTableData = this.$store.state.datasource.currentShowTableData
      return curTableData.database + '.' + curTableData.name
    },
    aclTableList () {
      var result = []
      this.aclTableData.forEach((k) => {
        if (this.serarchChar && k.toUpperCase().indexOf(this.serarchChar.toUpperCase()) >= 0 || !this.serarchChar) {
          result.push({name: k})
        }
      })
      return result
    },
    totalLength () {
      return this.aclTableList.length
    },
    pagerAclTableList () {
      var perPager = this.$refs.pager && this.$refs.pager.pageSize || 0
      return this.aclTableList.slice(perPager * (this.currentPage - 1), perPager * (this.currentPage))
    },
    hasSomeProjectPermission () {
      return hasPermission(this, this.getProjectIdByName(localStorage.getItem('selected_project')), permissions.ADMINISTRATION.mask)
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  watch: {
  },
  mounted () {
    this.getAllAclSetOfTable()
  },
  locales: {
    'en': {delConfirm: 'The action will delete this access, still continue?', cancelButtonText: 'No', confirmButtonText: 'Yes', delSuccess: 'Access deleted successfully.', saveSuccess: 'Access saved successfully.', userName: 'User name', access: 'Access', grant: 'Grant'},
    'zh-cn': {delConfirm: '此操作将删除该授权，是否继续?', cancelButtonText: '否', confirmButtonText: '是', delSuccess: '权限删除成功提示：权限删除成功！', saveSuccess: '权限添加成功提示：权限添加成功！', userName: '用户名', access: '权限', grant: '授权'}
  }
}
</script>
<style lang="less" >
@import '../../../less/config.less';
.access{
  .el-dialog{
    .el-input{
      padding:0;
    }
  }
}

</style>
