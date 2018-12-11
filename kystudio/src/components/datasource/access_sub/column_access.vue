<template>
    <div class="accesscolumn">
       <el-button type="primary" plain icon="el-icon-plus" @click="addGrant" v-show="hasSomeProjectPermission || isAdmin" size="medium">{{$t('restrict')}}</el-button>
       <div style="width:200px;" class="ksd-mb-10 ksd-fright">
          <el-input :placeholder="$t('kylinLang.common.userOrGroup')" size="medium" @input="searchColumnAcl" v-model="serarchChar" class="show-search-btn" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'">
          </el-input>
        </div>
       <el-table class="ksd-mt-20"
            border
            :data="aclColumnList"
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
              show-overflow-tooltip
              :label="$t('columns')"
              >
              <template slot-scope="scope">
                {{ scope.row.columns && scope.row.columns.join(',') || ''}}
              </template>
            </el-table-column>
            <el-table-column v-if="hasSomeProjectPermission || isAdmin"
              width="100"
              prop="Action"
              :label="$t('kylinLang.common.action')">
              <template slot-scope="scope">
                <common-tip :content="$t('kylinLang.common.edit')">
                  <el-button size="mini" class="ksd-btn del" icon="el-icon-ksd-table_edit" @click="editAclOfColumn(scope.row.name, scope.row.columns, scope.row.nameType)"></el-button>
                </common-tip>
                <common-tip :content="$t('kylinLang.common.delete')">
                  <el-button size="mini" class="ksd-btn del" icon="el-icon-delete" @click="delAclOfColumn(scope.row.name, scope.row.nameType)"></el-button>
                </common-tip>
              </template>
            </el-table-column>
          </el-table>
          <kap-pager
            class="ksd-center ksd-mt-20 ksd-mb-20" ref="pager"
            :totalSize="aclColumnSize"
            @handleCurrentChange="handleCurrentChange">
          </kap-pager>
          <el-dialog :title="$t('restrict')" width="660px" :visible.sync="addGrantDialog" @close="closeDialog" :close-on-press-escape="false" :close-on-click-modal="false">
               <el-alert
                :title="$t('columnAclDesc')"
                show-icon
                class="ksd-mb-6"
                :closable="false"
                type="warning">
              </el-alert>
              <el-form :model="grantObj" ref="aclOfColumnForm"  :rules="aclTableRules" v-if="addGrantDialog">
                <el-form-item  :label="$t('kylinLang.common.userOrGroup')" label-width="100px" required  style="width:100%">
                  <el-col :span="9">
                   <el-select v-model="assignType" :disabled="isEdit"  size="medium"  style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelect')" @change="changeAssignType">
                    <el-option
                      v-for="item in assignTypes"
                      :key="item.value"
                      :label="item.label"
                      :value="item.value">
                    </el-option>
                  </el-select>
                </el-col>
                <el-col :span="11" class="ksd-ml-20">
                 <!--   <el-select v-if="assignType === 'user'"  style="width:100%" filterable v-model="grantObj.name"   :placeholder="$t('kylinLang.common.pleaseSelectUserName')" :disabled="isEdit" >
                    <el-option v-for="b in aclWhiteList" :value="b.value">{{b.value}}</el-option>
                  </el-select> -->
                  <el-form-item  prop="name" style="width:100%">
                    <kap-filter-select :asyn="true" @req="getWhiteListOfColumn" v-model="grantObj.name" :disabled="isEdit" style="width:100%" v-show="assignType === 'user'" :dataMap="{label: 'value', value: 'value'}" :list="aclWhiteList" placeholder="kylinLang.common.pleaseInputUserName" :size="100"></kap-filter-select>

                   <!--  <el-select v-if="assignType === 'group'"  style="width:100%" filterable  v-model="grantObj.name" :placeholder="$t('kylinLang.common.pleaseSelectUserGroup')" :disabled="isEdit" >
                    <el-option v-for="b in aclWhiteGroupList" :value="b.value">{{b.value}}</el-option>
                  </el-select> -->
                     <kap-filter-select v-model="grantObj.name" :disabled="isEdit" style="width:100%" v-show="assignType === 'group'" :dataMap="{label: 'value', value: 'value'}" :list="aclWhiteGroupList" placeholder="kylinLang.common.pleaseInputUserGroup" :size="100"></kap-filter-select>
                 </el-form-item>
                </el-col>
              </el-form-item>
              </el-form>
              <el-form>
                <el-form-item >
                  <el-transfer filterable :titles="titles" :props="{
                    key: 'name',
                    label: 'name'
                  }" v-model="needSetColumns" :data="columnList"></el-transfer>
                </el-form-item>
              </el-form>
              <div slot="footer" class="dialog-footer">
                <el-button @click="addGrantDialog = false">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="primary" plain :loading="saveBtnLoad"  :disabled="needSetColumns.length<=0" @click="saveAclTable">{{$t('kylinLang.common.submit')}}</el-button>
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
  name: 'columnAccess',
  data () {
    return {
      addGrantDialog: false,
      grantObj: {
        name: ''
      },
      isEdit: false,
      titles: [this.$t('willcheck'), this.$t('haschecked')],
      currentPage: 1,
      aclColumnSize: 0,
      serarchChar: '',
      aclColumnUserData: [],
      aclColumnGroupData: [],
      columnList: [],
      aclWhiteList: [],
      aclWhiteGroupList: [],
      needSetColumns: [],
      assignTypes: assignTypes,
      assignType: 'user',
      saveBtnLoad: false,
      aclTableRules: {
        name: [{
          required: true, message: this.$t('kylinLang.common.pleaseInputUserOrGroupName'), trigger: 'change'
        }]
      },
      searchLoading: false,
      ST: null
    }
  },
  components: {
  },
  created () {
  },
  methods: {
    ...mapActions({
      getAclSetOfColumn: 'GET_ACL_SET_COLUMN',
      saveAclSetOfColumn: 'SAVE_ACL_SET_COLUMN',
      delAclSetOfColumn: 'DEL_ACL_SET_COLUMN',
      updateAclSetOfColumn: 'UPDATE_ACL_SET_COLUMN',
      getAclWhiteList: 'GET_ACL_WHITELIST_COLUMN',
      getGroupList: 'GET_GROUP_LIST'
    }),
    changeAssignType () {
      if (!this.isEdit) {
        this.grantObj.name = ''
      }
      this.getWhiteListOfColumn()
    },
    editAclOfColumn (userName, columns, nameType) {
      this.isEdit = true
      this.addGrantDialog = true
      this.assignType = nameType
      this.grantObj.name = userName
      this.$nextTick(() => {
        this.needSetColumns = columns
      })
    },
    delAclOfColumn (userName, assignType) {
      kapConfirm(this.$t('delConfirm'), {cancelButtonText: this.$t('cancelButtonText'), confirmButtonText: this.$t('confirmButtonText')}).then(() => {
        this.delAclSetOfColumn({
          tableName: this.tableName,
          project: this.$store.state.project.selected_project,
          userName: userName,
          type: assignType
        }).then((res) => {
          handleSuccess(res, (data) => {
            this.getAllAclSetOfColumn()
            this.getWhiteListOfColumn()
            this.$message({message: this.$t('delSuccess'), type: 'success'})
          })
        }, (res) => {
          handleError(res)
        })
      })
    },
    resetAclTableObj () {
      this.grantObj = {
        name: ''
      }
    },
    addGrant () {
      this.addGrantDialog = true
      this.isEdit = false
      // this.assignType = 'user'
      this.resetAclTableObj()
      this.needSetColumns = []
    },
    saveAclTable () {
      this.$refs.aclOfColumnForm.validate((valid) => {
        if (valid) {
          this.saveBtnLoad = true
          var action = 'saveAclSetOfColumn'
          if (this.isEdit) {
            action = 'updateAclSetOfColumn'
          }
          this[action]({
            tableName: this.tableName,
            project: this.$store.state.project.selected_project,
            userName: this.grantObj.name,
            columns: this.needSetColumns,
            type: this.assignType
          }).then((res) => {
            this.saveBtnLoad = false
            this.addGrantDialog = false
            this.getAllAclSetOfColumn()
            this.$message({message: this.$t('saveSuccess'), type: 'success'})
          }, (res) => {
            this.saveBtnLoad = false
            handleError(res)
          })
        }
      })
    },
    closeDialog () {
      this.$refs.aclOfColumnForm.clearValidate()
    },
    handleCurrentChange (curpage) {
      this.currentPage = curpage
      this.getAllAclSetOfColumn()
    },
    getAllAclSetOfColumn () {
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
      return this.getAclSetOfColumn(para).then((res) => {
        handleSuccess(res, (data) => {
          this.aclColumnUserData = data.user || []
          this.aclColumnGroupData = data.group || []
          this.aclColumnSize = data.size
        })
      }, (res) => {
        handleError(res)
      })
    },
    searchColumnAcl () {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        this.searchLoading = true
        this.getAllAclSetOfColumn().then(() => {
          this.searchLoading = false
        }, () => {
          this.searchLoading = false
        })
      }, 500)
    },
    getWhiteListOfColumn (filterUserName) {
      var para = {otherPara: {pageSize: 100, pageOffset: 0}, project: this.$store.state.project.selected_project, type: this.assignType, tableName: this.tableName}
      if (filterUserName) {
        para.otherPara.name = filterUserName
      }
      this.getAclWhiteList(para).then((res) => {
        handleSuccess(res, (data) => {
          var result = []
          data.users.forEach((d) => {
            result.push({value: d})
          })
          if (this.assignType === 'user') {
            this.aclWhiteList = result
          } else {
            this.aclWhiteGroupList = result
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
      this.columnList = curTableData.columns.slice(0)
      return curTableData.database + '.' + curTableData.name
    },
    aclColumnList () {
      var result = []
      this.aclColumnGroupData.forEach((col) => {
        for (var i in col) {
          var name = i
          var nameType = 'group'
          result.push({name: name, nameType: nameType, columns: col[i]})
        }
      })
      this.aclColumnUserData.forEach((col) => {
        for (var i in col) {
          var name = i
          var nameType = 'user'
          result.push({name: name, nameType: nameType, columns: col[i]})
        }
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
    this.getAllAclSetOfColumn()
    this.getWhiteListOfColumn()
  },
  locales: {
    'en': {delConfirm: 'The action will delete this restriction, still continue?', cancelButtonText: 'No', confirmButtonText: 'Yes', delSuccess: 'Access deleted successfully.', saveSuccess: 'Access saved successfully.', userName: 'User name', access: 'Access', restrict: 'Restrict', columnAclDesc: 'By configuring this setting, the user/group will not be able to view and query the selected column.', columns: 'Columns', willcheck: 'Column to be selected', haschecked: 'Restricted columns'},
    'zh-cn': {delConfirm: '此操作将删除该授权，是否继续?', cancelButtonText: '否', confirmButtonText: '是', delSuccess: '权限删除成功！', saveSuccess: '权限添加成功！', userName: '用户名', access: '权限', restrict: '约束', columnAclDesc: '通过以下设置，用户/组将无法查看及查询选中的列。', columns: '列', willcheck: '待选择列', haschecked: '已约束列'}
  }
}
</script>
<style lang="less" >
@import '../../../assets/styles/variables.less';
.accesscolumn{
  .el-transfer {
    margin-left: 50%;
    /* position: absolute; */
    left: -296px;
    position: relative;
    width: 100%;
  }
  .el-transfer-panel{
    width: 250px;
  }
}

</style>
