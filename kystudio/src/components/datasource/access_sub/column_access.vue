<template>
    <div class="accesscolumn">
       <el-button type="blue" icon="plus" @click="addGrant">{{$t('restrict')}}</el-button>  <span style="color:grey" class="ksd-ml-10">{{$t('columnAclDesc')}}</span>
       <div style="width:200px;float: right;">
          <el-input :placeholder="$t('userName')" icon="search" v-model="serarchChar" class="show-search-btn" >
          </el-input>
        </div>
       <el-table class="ksd-mt-20"
            border
            :data="pagerAclColumnList"
            style="width: 100%">
            <el-table-column
              sortable
              prop="name"
              :label="$t('userName')"
              width="180"
              >
            </el-table-column>
            <el-table-column
              :label="$t('columns')"
              >
              <template scope="scope">
                {{ scope.row.columns.join(',')}}
              </template>
            </el-table-column>
            <el-table-column
              width="100"
              prop="Action"
              :label="$t('kylinLang.common.action')">
              <template scope="scope">
              <el-button size="mini" class="ksd-btn del" icon="edit" @click="editAclOfColumn(scope.row.name, scope.row.columns)"></el-button>
              <el-button size="mini" class="ksd-btn del" icon="delete" @click="delAclOfColumn(scope.row.name)"></el-button>
              </template>
            </el-table-column>
          </el-table>
          <pager class="ksd-center" :totalSize="totalLength" v-on:handleCurrentChange='pageCurrentChange' ref="pager"></pager>
          <el-dialog :title="$t('restrict')" :visible.sync="addGrantDialog"  size="small" @close="closeDialog">
               <el-alert
                :title="$t('columnAclDesc')"
                show-icon
                class="ksd-mb-6 trans"
                :closable="false"
                type="warning">
              </el-alert>
              <el-form :model="grantObj" ref="aclOfColumnForm" :rules="aclTableRules">
                <el-form-item  :label="$t('userName')" label-width="80px" prop="name">
                  <el-select v-model="grantObj.name" style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelectUserName')">
                    <el-option v-for="b in aclWhiteList" :value="b.value">{{b.value}}</el-option>
                  </el-select>
                  <!-- <el-autocomplete  v-model="grantObj.name" style="width:100%" :fetch-suggestions="querySearchAsync"></el-autocomplete> -->
                  <!-- <el-input v-model="grantObj.name"  auto-complete="off" placeholder="UserName"></el-input> -->
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
                <el-button type="primary" :loading="saveBtnLoad"  :disabled="needSetColumns.length<=0" @click="saveAclTable">{{$t('kylinLang.common.save')}}</el-button>
              </div>
          </el-dialog>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, kapConfirm } from '../../../util/business'
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
      serarchChar: '',
      aclColumnData: [],
      columnList: [],
      aclWhiteList: [],
      needSetColumns: [],
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
      getAclSetOfColumn: 'GET_ACL_SET_COLUMN',
      saveAclSetOfColumn: 'SAVE_ACL_SET_COLUMN',
      delAclSetOfColumn: 'DEL_ACL_SET_COLUMN',
      updateAclSetOfColumn: 'UPDATE_ACL_SET_COLUMN',
      getAclWhiteList: 'GET_ACL_WHITELIST_COLUMN'
    }),
    editAclOfColumn (userName, columns) {
      this.isEdit = true
      this.addGrantDialog = true
      this.$nextTick(() => {
        this.grantObj.name = userName
        this.needSetColumns = columns
      })
    },
    delAclOfColumn (userName) {
      kapConfirm(this.$t('delConfirm')).then(() => {
        this.delAclSetOfColumn({
          tableName: this.tableName,
          project: this.$store.state.project.selected_project,
          userName: userName
        }).then((res) => {
          handleSuccess(res, (data) => {
            this.getAllAclSetOfColumn()
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
            columns: this.needSetColumns
          }).then((res) => {
            this.saveBtnLoad = false
            this.addGrantDialog = false
            // handleSuccess(res, (data) => {})
            this.getAllAclSetOfColumn()
            this.$message({message: this.$t('saveSuccess'), type: 'success'})
          }, (res) => {
            this.saveBtnLoad = false
            // this.addGrantDialog = false
            handleError(res)
          })
        }
      })
    },
    closeDialog () {
      this.$refs.aclOfColumnForm.resetFields()
    },
    pageCurrentChange (curpage) {
      this.currentPage = curpage
    },
    getAllAclSetOfColumn () {
      this.getAclSetOfColumn({
        tableName: this.tableName,
        project: this.$store.state.project.selected_project
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.aclColumnData = data
          this.getWhiteListOfColumn()
        })
      }, (res) => {
        handleError(res)
      })
    },
    getWhiteListOfColumn (cb) {
      this.getAclWhiteList({
        tableName: this.tableName,
        project: this.$store.state.project.selected_project
      }).then((res) => {
        handleSuccess(res, (data) => {
          var result = []
          data.forEach((d) => {
            result.push({value: d})
          })
          this.aclWhiteList = result
        })
      }, (res) => {
        handleError(res)
      })
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
      for (var i in this.aclColumnData) {
        if (this.serarchChar && i.toUpperCase().indexOf(this.serarchChar.toUpperCase()) >= 0 || !this.serarchChar) {
          result.push({name: i, columns: this.aclColumnData[i]})
        }
      }
      return result
    },
    totalLength () {
      return this.aclColumnList.length
    },
    pagerAclColumnList () {
      var perPager = this.$refs.pager && this.$refs.pager.pageSize || 0
      return this.aclColumnList.slice(perPager * (this.currentPage - 1), perPager * (this.currentPage))
    }
  },
  watch: {
  },
  mounted () {
    this.getAllAclSetOfColumn()
  },
  locales: {
    'en': {delConfirm: 'The action will delete this restrict, still continue?', delSuccess: 'Access deleted successfully.', saveSuccess: 'Access saved successfully.', userName: 'User name', access: 'Access', restrict: 'Restrict', columnAclDesc: 'By configure this setting, user will not be able to view and query the selected column.', columns: 'Columns', willcheck: 'Column to be selected', haschecked: 'Restricted columns'},
    'zh-cn': {delConfirm: '此操作将删除该授权，是否继续?', delSuccess: '权限删除成功！', saveSuccess: '权限添加成功提示：权限添加成功！', userName: '用户名', access: '权限', restrict: '约束', columnAclDesc: '通过以下设置，用户将无法查看及查询选中的列。', columns: '列', willcheck: '待选择列', haschecked: '已约束列'}
  }
}
</script>
<style lang="less" >
@import '../../../less/config.less';
.accesscolumn{
  .el-transfer{
    text-align: center;
  }
  .el-transfer-panel__item{
    &:hover{
      background: none;
      color:@base-color;
    }
  }
  .el-transfer-panel__filter{
    margin-top: 0;
    &.el-input{
     .el-input__inner{
      width: 90%;
     }
    }
  }
  .el-transfer-panel {
    width: 40%;
    background-color: #20222e;
    border:solid 1px #20222e;
    text-align: left;
    .el-transfer-panel__header{
      background-color: #393e53;
      border-bottom: none;
      color:#fff;
      text-align: left;
    }
    .el-transfer-panel__footer{
      background-color: #393e53;
      border-top: none;
      color:#fff;
      text-align: left;
    }
  }
  .el-transfer-panel:first-child{
    float: left;
  }
  .el-transfer-panel:last-child{
    float: right;
  }
  .el-transfer__buttons{
    margin-top: 80px;
  }
  .el-dialog{
    &.el-dialog--small{
      width: 600px!important;
    }
    .el-input{
      padding:0;
    }
  }
}

</style>
