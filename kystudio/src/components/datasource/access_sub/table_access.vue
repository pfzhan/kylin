<template>
    <div class="access">
       <el-button type="blue" icon="plus" @click="addGrant" v-show="hasSomeProjectPermission || isAdmin">{{$t('grant')}}</el-button>
       <div style="width:200px;" class="ksd-mb-10 ksd-fright">
          <el-input :placeholder="$t('kylinLang.common.userOrGroup')" icon="search" v-model="serarchChar" class="show-search-btn" >
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
              :label="$t('kylinLang.common.userOrGroup')"
              width="180"
              >
              <template slot-scope="scope">
                <icon name="user-o" style="color: #d4d7e3;" scale="0.8" v-show="scope.row.nameType === 'user'"></icon>
                <icon v-show="scope.row.nameType === 'group'" scale="0.8" name="group" style="color: #d4d7e3;"></icon>
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
              <el-button size="mini" class="ksd-btn del" icon="delete" @click="delAclOfTable(scope.row.name, scope.row.nameType)"></el-button>
              </template>
            </el-table-column>
          </el-table>
          <pager class="ksd-center" :totalSize="totalLength" v-on:handleCurrentChange='pageCurrentChange' ref="pager"></pager>
          <el-dialog :title="$t('grant')" :visible.sync="addGrantDialog"  size="tiny" @close="closeDialog" :close-on-press-escape="false" :close-on-click-modal="false">
              <el-form :model="grantObj" ref="aclOfTableForm" :rules="aclTableRules">
                <el-form-item :label="$t('kylinLang.common.userOrGroup')" label-width="90px" prop="name">
                  <el-col :span="11">
                    <el-select v-model="assignType" style="width:100%"  :placeholder="$t('kylinLang.common.pleaseSelect')" @change="changeAssignType">
                    <el-option
                      v-for="item in assignTypes"
                      :key="item.value"
                      :label="item.label"
                      :value="item.value">
                    </el-option>
                  </el-select>
                  </el-col>
                  <el-col :span="11">
                     <!-- <el-select filterable v-model="grantObj.name" v-if="assignType === 'user'" style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelectUserName')">
                      <el-option v-for="b in aclBlackList" :value="b.value">{{b.value}}</el-option>
                    </el-select> -->
                    <kap-filter-select :asyn="true" @req="getBlackListOfTable" v-model="grantObj.name" v-show="assignType === 'user'" :dataMap="{label: 'value', value: 'value'}" :list="aclBlackList" placeholder="kylinLang.common.pleaseInputUserName" :size="100"></kap-filter-select>

                   <!--  <el-select filterable v-model="grantObj.name" v-if="assignType === 'group'" style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelectUserGroup')">
                      <el-option v-for="b in aclBlackGroupList" :value="b.value">{{b.value}}</el-option>
                    </el-select> -->
                     <kap-filter-select v-model="grantObj.name" v-show="assignType === 'group'" :dataMap="{label: 'value', value: 'value'}" :list="aclBlackGroupList" placeholder="kylinLang.common.pleaseInputUserGroup" :size="100"></kap-filter-select>
                  </el-col>
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
import { permissions, assignTypes } from '../../../config'
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
      aclBlackGroupList: [],
      saveBtnLoad: false,
      assignTypes: assignTypes,
      assignType: 'user',
      aclTableRules: {
        name: [{
          required: true, message: this.$t('kylinLang.common.pleaseInputUserOrGroupName'), trigger: 'change'
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
    },
    getAllAclSetOfTable () {
      this.getAclSetOfTable({
        tableName: this.tableName,
        project: this.$store.state.project.selected_project,
        type: this.assignType
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.aclTableData = data
          this.getBlackListOfTable()
        })
      }, (res) => {
        handleError(res)
      })
    },
    getBlackListOfTable (filterUserName) {
      var para = {otherPara: {pageSize: 100, pageOffset: 0}, project: this.$store.state.project.selected_project, type: this.assignType, tableName: this.tableName}
      if (filterUserName) {
        para.otherPara.name = filterUserName
      }
      this.getAclBlackList(para).then((res) => {
        handleSuccess(res, (data) => {
          var result = []
          data.users.forEach((d) => {
            result.push({value: d})
          })
          if (this.assignType === 'user') {
            this.aclBlackList = result
          } else {
            this.aclBlackGroupList = result
          }
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
        if (this.serarchChar && k.first.toUpperCase().indexOf(this.serarchChar.toUpperCase()) >= 0 || !this.serarchChar) {
          result.push({name: k.first, nameType: k.second === 'u' ? 'user' : 'group'})
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
  },
  locales: {
    'en': {delConfirm: 'The action will delete this access, still continue?', cancelButtonText: 'No', confirmButtonText: 'Yes', delSuccess: 'Access deleted successfully.', saveSuccess: 'Access saved successfully.', userName: 'User name', access: 'Access', grant: 'Grant'},
    'zh-cn': {delConfirm: '此操作将删除该授权，是否继续?', cancelButtonText: '否', confirmButtonText: '是', delSuccess: '权限删除成功！', saveSuccess: '权限添加成功！', userName: '用户名', access: '权限', grant: '授权'}
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
