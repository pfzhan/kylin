<template>
    <div class="access">
       <el-button type="trans" icon="plus" @click="addGrant">{{$t('grant')}}</el-button>
       <div style="width:200px;float: right;">
          <el-input :placeholder="$t('userName')" icon="search" v-model="serarchChar" class="show-search-btn" >
          </el-input>
        </div>
       <el-table class="ksd-mt-20"
            border
            :data="pagerAclTableList"
            style="width: 100%">
            <el-table-column
              sortable
              prop="name"
              :label="$t('userName')"
              >
            </el-table-column>
            <el-table-column
              :label="$t('access')"
              width="180"
              >
              <template scope="scope">Query</template>
            </el-table-column>
            <el-table-column
              width="80"
              prop="Action"
              :label="$t('kylinLang.common.action')">
              <template scope="scope">
              <el-button size="mini" class="ksd-btn del" icon="delete" @click="delAclOfTable(scope.row.name)"></el-button>
              </template>
            </el-table-column>
          </el-table>
          <pager class="ksd-center" :totalSize="totalLength" v-on:handleCurrentChange='pageCurrentChange' ref="pager"></pager>
          <el-dialog title="Grant" :visible.sync="addGrantDialog"  size="tiny">
              <el-form :model="grantObj" ref="aclOfTableForm" :rules="aclTableRules">
                <el-form-item :label="$t('userName')" label-width="90px" prop="name">
                  <el-autocomplete  v-model="grantObj.name" style="width:100%" :fetch-suggestions="querySearchAsync"></el-autocomplete>
                  <!-- <el-input v-model="grantObj.name"  auto-complete="off" placeholder="UserName"></el-input> -->
                </el-form-item>
                <el-form-item :label="$t('access')" label-width="90px">
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
import { handleSuccess, handleError, kapConfirm } from '../../../util/business'
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
          required: true, message: '请输入用户名字！', trigger: 'change'
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
    delAclOfTable (userName) {
      kapConfirm(this.$t('delConfirm')).then(() => {
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
          if (typeof cb === 'function') {
            var result = []
            data.forEach((d) => {
              result.push({value: d})
            })
            cb(result)
          }
        })
      }, (res) => {
        handleError(res)
      })
    },
    querySearchAsync (queryString, cb) {
      this.getBlackListOfTable((data) => {
        cb(data)
      })
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
    }
  },
  watch: {
  },
  mounted () {
    this.getAllAclSetOfTable()
  },
  locales: {
    'en': {delConfirm: 'The action will delete this access, still continue?', delSuccess: 'Access deleted successfully.', saveSuccess: 'Access saved successfully.', userName: 'User name', access: 'Access', grant: 'Grant'},
    'zh-cn': {delConfirm: '此操作将删除该授权，是否继续?', delSuccess: '权限删除成功提示：权限删除成功！', saveSuccess: '权限添加成功提示：权限添加成功！', userName: '用户名', access: '权限', grant: '授权'}
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
