<template>
    <div class="access">
       <el-button type="primary" plain icon="el-icon-plus" @click="addGrant" v-show="hasSomeProjectPermission || isAdmin" size="medium">{{$t('grant')}}</el-button>
       <div style="width:200px;" class="ksd-mb-10 ksd-fright">
          <el-input :placeholder="$t('kylinLang.common.userOrGroup')" size="medium" @input="searchTableAcl" v-model="serarchChar" class="show-search-btn" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'">
          </el-input>
        </div>
       <el-table class="ksd-mt-20"
            border
            :data="aclTableList"
            style="width: 100%">
            <el-table-column
              show-overflow-tooltip
              sortable
              prop="name"
              :label="$t('kylinLang.common.userOrGroup')"
              width="180"
              >
              <template slot-scope="scope">
                <i class="el-icon-ksd-table_admin" v-show="scope.row.nameType === 'user'"></i>
                <i class="el-icon-ksd-table_group" v-show="scope.row.nameType === 'group'"></i>
                &nbsp;{{ scope.row.name}}
              </template>
            </el-table-column>
            <el-table-column
              :label="$t('access')"
              >
              <template slot-scope="scope">Query</template>
            </el-table-column>
            <el-table-column v-if="hasSomeProjectPermission || isAdmin"
              width="100"
              prop="Action"
              :label="$t('kylinLang.common.action')">
              <template slot-scope="scope">
              <el-button size="mini" class="ksd-btn del" icon="el-icon-ksd-table_delete" @click="delAclOfTable(scope.row.name, scope.row.nameType)"></el-button>
              </template>
            </el-table-column>
          </el-table>
          <pager class="ksd-center" :totalSize="aclTableTotalSize" v-on:handleCurrentChange='pageCurrentChange' ref="pager"></pager>
          <el-dialog :title="$t('grant')" width="660px" :visible.sync="addGrantDialog" @close="closeDialog" :close-on-press-escape="false" :close-on-click-modal="false" :append-to-body="true">
              <el-form :model="grantObj" ref="aclOfTableForm" :rules="aclTableRules" v-if="addGrantDialog">
                <el-form-item :label="$t('kylinLang.common.userOrGroup')" label-width="90px" required>
                  <el-col :span="11">
                    <el-select size="medium" v-model="assignType" style="width:100%"  :placeholder="$t('kylinLang.common.pleaseSelect')" @change="changeAssignType">
                    <el-option
                      v-for="item in assignTypes"
                      :key="item.value"
                      :label="item.label"
                      :value="item.value">
                    </el-option>
                  </el-select>
                  </el-col>
                  <el-col :span="11" class="ksd-ml-10">
                     <!-- <el-select filterable v-model="grantObj.name" v-if="assignType === 'user'" style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelectUserName')">
                      <el-option v-for="b in aclBlackList" :value="b.value">{{b.value}}</el-option>
                    </el-select> -->
                    <el-form-item  prop="name">
                    <kap-filter-select :asyn="true" @req="getBlackListOfTable" v-model="grantObj.name" v-show="assignType === 'user'" :dataMap="{label: 'value', value: 'value'}" :list="aclBlackList" style="width:100%" placeholder="kylinLang.common.pleaseInputUserName" :size="100"></kap-filter-select>

                   <!--  <el-select filterable v-model="grantObj.name" v-if="assignType === 'group'" style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelectUserGroup')">
                      <el-option v-for="b in aclBlackGroupList" :value="b.value">{{b.value}}</el-option>
                    </el-select> -->
                     <kap-filter-select v-model="grantObj.name" v-show="assignType === 'group'" :dataMap="{label: 'value', value: 'value'}" :list="aclBlackGroupList" placeholder="kylinLang.common.pleaseInputUserGroup" :size="100" style="width:100%"></kap-filter-select>
                   </el-form-item>
                  </el-col>
                </el-form-item>
                <el-form-item :label="$t('access')" label-width="90px" class="is-required">
                  <el-col :span="11">
                    <el-select v-model="grantObj.access" size="medium" style="width:100%">
                      <el-option label="Query" :value="1"></el-option>
                    </el-select>
                  </el-col>
                </el-form-item>
              </el-form>
              <div slot="footer" class="dialog-footer">
                <el-button @click="addGrantDialog = false">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="primary" plain :loading="saveBtnLoad" @click="saveAclTable" :disabled="!grantObj.name">{{$t('kylinLang.common.submit')}}</el-button>
              </div>
          </el-dialog>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, kapConfirm, hasRole, hasPermission } from '../../../util/business'
import { pageCount, permissions, assignTypes } from '../../../config'
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
      searchLoading: false,
      serarchChar: '',
      aclTableUserData: [],
      aclTableGroupData: [],
      aclTableTotalSize: 0,
      aclBlackList: [],
      aclBlackGroupList: [],
      saveBtnLoad: false,
      assignTypes: assignTypes,
      assignType: 'user',
      aclTableRules: {
        name: [{
          required: true, message: this.$t('kylinLang.common.pleaseInputUserOrGroupName'), trigger: 'change'
        }]
      },
      ST: null
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
      this.$refs.aclOfTableForm.clearValidate()
    },
    changeAssignType () {
      this.grantObj.name = ''
      this.getBlackListOfTable()
    },
    delAclOfTable (userName, assignType) {
      kapConfirm(this.$t('delConfirm'), {cancelButtonText: this.$t('cancelButtonText'), confirmButtonText: this.$t('confirmButtonText')}).then(() => {
        this.delAclSetOfTable({
          tableName: this.tableName,
          project: this.$store.state.project.selected_project,
          userName: userName,
          type: assignType
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
            userName: this.grantObj.name,
            type: this.assignType
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
      this.getAllAclSetOfTable()
    },
    searchTableAcl () {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        this.searchLoading = true
        this.getAllAclSetOfTable().then(() => {
          this.searchLoading = false
        }, () => {
          this.searchLoading = false
        })
      }, 500)
    },
    getAllAclSetOfTable () {
      var para = {
        pager: {
          pageSize: pageCount,
          pageOffset: this.currentPage - 1
        },
        tableName: this.tableName,
        project: this.$store.state.project.selected_project,
        type: this.assignType
      }
      if (this.serarchChar) {
        para.pager.name = this.serarchChar
      }
      // for newten
      // return this.getAclSetOfTable(para).then((res) => {
      //   handleSuccess(res, (data) => {
      //     this.aclTableUserData = data.user || []
      //     this.aclTableGroupData = data.group || []
      //     this.aclTableTotalSize = data.size
      //   })
      // }, (res) => {
      //   handleError(res)
      // })
    },
    getBlackListOfTable (filterUserName) {
      var para = {otherPara: {pageSize: 100, pageOffset: 0}, project: this.$store.state.project.selected_project, type: this.assignType, tableName: this.tableName}
      if (filterUserName) {
        para.otherPara.name = filterUserName
      }
      // for newten
      // this.getAclBlackList(para).then((res) => {
      //   handleSuccess(res, (data) => {
      //     var result = []
      //     data.users.forEach((d) => {
      //       result.push({value: d})
      //     })
      //     if (this.assignType === 'user') {
      //       this.aclBlackList = result
      //     } else {
      //       this.aclBlackGroupList = result
      //     }
      //   })
      // }, (res) => {
      //   handleError(res)
      // })
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
      this.aclTableGroupData.forEach((k) => {
        result.push({name: k, nameType: 'group'})
      })
      this.aclTableUserData.forEach((k) => {
        result.push({name: k, nameType: 'user'})
      })
      return result
    },
    hasSomeProjectPermission () {
      return hasPermission(this, permissions.ADMINISTRATION.mask)
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  watch: {
  },
  mounted () {
    this.getAllAclSetOfTable()
    this.getBlackListOfTable()
  },
  locales: {
    'en': {delConfirm: 'The action will delete this access, still continue?', cancelButtonText: 'No', confirmButtonText: 'Yes', delSuccess: 'Access deleted successfully.', saveSuccess: 'Access saved successfully.', userName: 'User name', access: 'Access', grant: 'Grant'},
    'zh-cn': {delConfirm: '此操作将删除该授权，是否继续?', cancelButtonText: '否', confirmButtonText: '是', delSuccess: '权限删除成功！', saveSuccess: '权限添加成功！', userName: '用户名', access: '权限', grant: '授权'}
  }
}
</script>
<style lang="less" >
@import '../../../assets/styles/variables.less';
.access{
  .el-dialog{
    .el-input{
      padding:0;
    }
  }
}

</style>
