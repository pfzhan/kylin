<template>
    <div class="accessrow">
       <el-button type="blue" icon="plus" @click="addGrant" v-show="hasSomeProjectPermission || isAdmin">{{$t('restrict')}}</el-button>
       <div style="width:200px;" class="ksd-mb-10 ksd-fright">
          <el-input :placeholder="$t('userName')" icon="search" v-model="serarchChar" class="show-search-btn" >
          </el-input>
        </div>
      <p style="color:#717587;line-height: 16px;" class="ksd-mt-20" v-show="pagerAclRowList && pagerAclRowList.length"><icon name="exclamation-circle" class="ksd-fleft"></icon><span class="ksd-ml-10">{{$t('rowAclDesc')}}</span></p>
       <el-table class="ksd-mt-10" v-show="pagerAclRowList && pagerAclRowList.length"
            border
            :data="pagerAclRowList"
            style="width: 100%">
            <el-table-column
              sortable
              prop="name"
              :label="$t('userName')"
              width="180"
              >
            </el-table-column>
            <el-table-column
              :label="$t('condition')"
              >
              <template scope="scope">
                <p v-for="(key, v) in scope.row.conditions">{{v}} = {{key && key.join(',')}}</p>
              </template>
            </el-table-column>
            <el-table-column v-if="hasSomeProjectPermission || isAdmin"
              width="100"
              prop="Action"
              :label="$t('kylinLang.common.action')">
              <template scope="scope">
              <el-button size="mini" class="ksd-btn del" icon="edit" @click="editAclOfRow(scope.row.name, scope.row.conditions)"></el-button>
              <el-button size="mini" class="ksd-btn del" icon="delete" @click="delAclOfRow(scope.row.name)"></el-button>
              </template>
            </el-table-column>
          </el-table>
          <pager class="ksd-center" :totalSize="totalLength" v-on:handleCurrentChange='pageCurrentChange' ref="pager"></pager>
          <el-dialog :title="$t('restrict')" :visible.sync="addGrantDialog"  size="small" @close="closeDialog" :close-on-press-escape="false" :close-on-click-modal="false">
              <el-alert
                :title="$t('rowAclDesc')"
                show-icon
                class="ksd-mb-10 trans"
                :closable="false"
                type="warning">
              </el-alert>
              <el-form :model="grantObj" ref="aclOfRowForm" :rules="aclTableRules" >
                <el-form-item :label="$t('userName')" label-width="80px" prop="name">
                  <!-- <el-autocomplete  v-model="grantObj.name" style="width:100%" :fetch-suggestions="querySearchAsync"></el-autocomplete> -->
                  <el-select filterable v-model="grantObj.name" style="width:100%" :disabled="isEdit"  :placeholder=" $t('kylinLang.common.pleaseSelectUserName')">
                    <el-option v-for="b in aclWhiteList" :value="b.value">{{b.value}}</el-option>
                  </el-select>
                  <!-- <el-input v-model="grantObj.name"  auto-complete="off" placeholder="UserName"></el-input> -->
                </el-form-item>
                <el-form-item :label="$t('condition')" label-width="80px" class="ksd-mt-20 is-required" style="position:relative">
                  <el-row v-for="(rowset, index) in rowSetDataList" >
                    <el-col :sm="20" :md="21" :lg="22">
                      <el-select v-model="rowset.columnName" style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelectColumnName')" @change="selectColumnName(index, rowset.columnName)">
                        <el-option v-for="(item, index) in filterHasSelected(columnList, index, rowSetDataList, 'columnName')" :key="item.name" :label="item.name" :value="item.name">
                        <span style="float: left">{{ item.name }}</span>
                          <span style="float: right; color: #8492a6; font-size: 13px">{{ item.datatype }}</span>
                        </el-option>
                      </el-select>


                    </el-col>
                    <el-col :sm="4" :md="3" :lg="2" class="ksd-right"><el-button type="danger" icon="minus" @click="removeRowSet(index)"></el-button></el-col>
                    <el-col :span="24" class="ksd-pt-4" style="position: relative">
                      <arealabel  :placeholder="$t('pressEnter')" :ignoreSpecialChar="true" @refreshData="refreshData" :selectedlabels="rowset.valueList" :allowcreate='true'  :refreshInfo="{columnName: rowset.columnName, index: index}" @validateFail="validateFail" :validateRegex="rowset.validateReg"></arealabel>
                      <datepicker ref="hideDatePicker" :dateType="rowset.timeComponentType" :selectList="rowset.valueList" v-show="dateTypeList.indexOf(getColumnType(rowset.columnName))>=0 || dateTimeTypeList.indexOf(getColumnType(rowset.columnName))>=0"></datepicker>
                    </el-col>
                    <el-col :span="24"><div class="line"></div></el-col>
                  </el-row>
                  <el-button type="blue" icon="plus" class="ksd-mt-10" @click="addRowSet"></el-button>
                  <div v-show="previewDialgVisible" class="previewDialog">
                    <div class="header_tool">SQL<span class="el-icon-close" @click="previewDialgVisible=false"></span></div>
                    <editor class="ksd-mt-4" ref="preview" lang="sql" useWrapMode="true" v-model="previewInfo"  theme="monokai" width="100%"></editor>
                    <span class="dot-bottom"></span>
                  </div>
                  <div @click="openPreview" style="width:40px;" class="action_preview" v-show="!(saveConditionListLen === 0 || saveConditionListLen !== rowSetDataList.length)">{{$t('preview')}}</div>
                </el-form-item>
              </el-form>
              <div slot="footer" class="dialog-footer">
                <el-button @click="addGrantDialog = false">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="primary" :loading="saveBtnLoad" @click="saveAclTable" :disabled="saveConditionListLen === 0 || saveConditionListLen !== rowSetDataList.length">{{$t('kylinLang.common.save')}}</el-button>
              </div>
          </el-dialog>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, kapConfirm, hasRole, hasPermission, transToUtcTimeFormat, transToUtcDateFormat, transToUTCMs } from '../../../util/business'
import { permissions } from '../../../config'
// import {transToUtcTimeFormat, transToUtcDateFormat} from '../../../util/index'
import arealabel from 'components/common/area_label'
import datepicker from 'components/common/date_picker'
// import { permissions } from '../../config'
// import { changeDataAxis, isFireFox } from '../../util/index'
// import createKafka from '../kafka/create_kafka'
// import editKafka from '../kafka/edit_kafka'
// import viewKafka from '../kafka/view_kafka'
// import arealabel from 'components/common/area_label'
// import Scrollbar from 'smooth-scrollbar'
export default {
  name: 'rowaccess',
  data () {
    return {
      grantObj: {
        name: ''
      },
      needInit: true,
      addGrantDialog: false,
      columnList: [],
      currentPage: 1,
      serarchChar: '',
      aclTableRow: {},
      aclWhiteList: [],
      previewInfo: '',
      saveBtnLoad: false,
      previewDialgVisible: false,
      isEdit: false,
      timeSelect: '',
      rowSetDataList: [],
      intTypeList: ['int', 'smallint', 'tinyint', 'integer', 'bigint'],
      dateTypeList: ['date'],
      dateTimeTypeList: ['datetime', 'timestamp'],
      timeTypeList: ['time'],
      aclTableRules: {
        name: [{
          required: true, message: this.$t('kylinLang.common.pleaseSelectUserName'), trigger: 'change'
        }]
      }
    }
  },
  components: {
    arealabel,
    datepicker
  },
  created () {
    // console.log('2012-1-1')
    // console.log(transToUtcDateFormat(1325347200000))
  },
  methods: {
    ...mapActions({
      getAclSetOfRow: 'GET_ACL_SET_ROW',
      saveAclSetOfRow: 'SAVE_ACL_SET_ROW',
      delAclSetOfRow: 'DEL_ACL_SET_ROW',
      getAclWhiteList: 'GET_ACL_WHITELIST_ROW',
      updateAclSetOfRow: 'UPDATE_ACL_SET_ROW',
      previewSQL: 'PREVIEW_ACL_SET_ROW_SQL'
    }),
    openPreview () {
      this.previewSQL({
        tableName: this.tableName,
        project: this.$store.state.project.selected_project,
        userName: this.grantObj.name,
        conditions: this.saveConditionData
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.previewInfo = data
        })
      }, (res) => {
        handleError(res)
      })
      this.previewDialgVisible = true
      var editor = this.$refs.preview.editor
      if (!(editor && editor.session)) {
        return
      }
      editor.setReadOnly(true)
      editor.setOption('wrap', 'free')
      editor.session.gutterRenderer = {
        getWidth: (session, lastLineNumber, config) => {
          return lastLineNumber.toString().length * 1
        },
        getText: function (session, row) {
          return row + 1
        }
      }
    },
    closeDialog () {
      this.$refs.aclOfRowForm.resetFields()
      this.previewDialgVisible = false
    },
    selectColumnName (i, columnName) {
      if (!this.needInit) {
        this.needInit = true
        return
      }
      var result = this.getFilterRegExp(columnName)
      this.$set(this.rowSetDataList, i, {columnName: columnName, valueList: [], validateReg: result.reg, timeComponentType: result.timeComponentType})
    },
    validateFail () {
      this.$message(this.$t('valueValidateFail'))
    },
    getFilterRegExp (columnsName) {
      var columnType = this.getColumnType(columnsName)
      var result = ''
      var timeComponentType = ''
      var intTypeList = this.intTypeList
      if (intTypeList.indexOf(columnType) >= 0) {
        result = '^\\d+$'
      } else if (columnType.indexOf('decimal') >= 0) {
        result = '^\\d+(.\\d+)?$'
      } else if (this.dateTypeList.indexOf(columnType) >= 0) {
        result = '^[1-9]\\d{3}[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[1-2][0-9]|3[0-1])$'
        timeComponentType = 'date'
      } else if (this.dateTimeTypeList.indexOf(columnType) >= 0) {
        result = '^[1-9]\\d{3}[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[1-2][0-9]|3[0-1])(\\s+(20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d)$'
        timeComponentType = 'datetime'
      } else if (this.timeTypeList.indexOf(columnType) >= 0) {
        result = '^((20|21|22|23|[0-1]\\d):[0-5]\\d:[0-5]\\d)$'
        // timeComponentType = 'datetime'
      }
      return {reg: result, timeComponentType: timeComponentType}
    },
    getColumnType (columnsName) {
      var columnLen = this.columnList && this.columnList.length || 0
      var columnType = ''
      for (var i = 0; i < columnLen; i++) {
        if (this.columnList[i].name === columnsName) {
          columnType = this.columnList[i].datatype
          break
        }
      }
      return columnType
    },
    refreshData (a, refreshInfo) {
      this.rowSetDataList[refreshInfo.index].valueList = a
    },
    filterHasSelected (data, index, filterData, key) {
      var len = data && data.length || 0
      var lenfilter = filterData && filterData.length || 0
      var result = []
      for (var i = 0; i < len; i++) {
        var checked = false
        for (var s = 0; s < lenfilter; s++) {
          if (s !== index && data[i].name === filterData[s][key]) {
            checked = true
            break
          }
        }
        if (!checked) {
          result.push(data[i])
        }
      }
      return result
    },
    delAclOfRow (userName) {
      kapConfirm(this.$t('delConfirm'), {cancelButtonText: this.$t('cancelButtonText'), confirmButtonText: this.$t('confirmButtonText')}).then(() => {
        this.delAclSetOfRow({
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
    editAclOfRow (userName, conditions) {
      this.needInit = false
      this.isEdit = true
      this.addGrantDialog = true
      this.grantObj.name = userName
      this.rowSetDataList = []
      var index = 0
      for (var i in conditions) {
        var obj = {
          columnName: i,
          valueList: conditions[i]
        }
        var result = this.getFilterRegExp(i)
        obj.timeComponentType = result.timeComponentType
        obj.validateReg = result.reg
        this.$set(this.rowSetDataList, index, obj)
        // Object.assign(this.rowSetDataList, this.rowSetDataList.length, obj)
        index++
        // this.rowSetDataList.push(obj)
      }
    },
    resetAclRowObj () {
      this.grantObj = {
        name: ''
      }
      this.rowSetDataList = []
    },
    addRowSet () {
      var obj = {
        columnName: '',
        valueList: []
      }
      this.rowSetDataList.push(obj)
    },
    removeRowSet (i) {
      this.rowSetDataList.splice(i, 1)
    },
    addGrant () {
      this.isEdit = false
      this.addGrantDialog = true
      this.resetAclRowObj()
    },
    saveAclTable () {
      this.$refs.aclOfRowForm.validate((valid) => {
        if (valid) {
          this.saveBtnLoad = true
          var action = 'saveAclSetOfRow'
          if (this.isEdit) {
            action = 'updateAclSetOfRow'
          }
          this[action]({
            tableName: this.tableName,
            project: this.$store.state.project.selected_project,
            userName: this.grantObj.name,
            conditions: this.saveConditionData
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
      this.getAclSetOfRow({
        tableName: this.tableName,
        project: this.$store.state.project.selected_project
      }).then((res) => {
        handleSuccess(res, (data) => {
          this.aclTableRow = data
          this.getWhiteListOfTable()
        })
      }, (res) => {
        handleError(res)
      })
    },
    getWhiteListOfTable (cb) {
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
    aclRowList () {
      var result = []
      for (var i in this.aclTableRow) {
        if (this.serarchChar && i.toUpperCase().indexOf(this.serarchChar.toUpperCase()) >= 0 || !this.serarchChar) {
          for (var c in this.aclTableRow[i]) {
            var columnType = this.getColumnType(c)
            this.aclTableRow[i][c] = this.aclTableRow[i][c].map((k) => {
              if (this.dateTypeList.indexOf(columnType) >= 0) {
                k = transToUtcDateFormat(+k.leftExpr)
                return k
              } else if (this.dateTimeTypeList.indexOf(columnType) >= 0) {
                k = transToUtcTimeFormat(+k.leftExpr)
                return k
              } else if (this.timeTypeList.indexOf(columnType) >= 0) {
                k = transToUtcTimeFormat(+k.leftExpr)
                var result = k.split(/\s+/)
                if (result.length >= 2) {
                  return result[1]
                }
                return k
              } else {
                return k.leftExpr
              }
            })
          }
          result.push({name: i, conditions: this.aclTableRow[i]})
        }
      }
      return result
    },
    totalLength () {
      return this.aclRowList.length
    },
    pagerAclRowList () {
      var perPager = this.$refs.pager && this.$refs.pager.pageSize || 0
      return this.aclRowList.slice(perPager * (this.currentPage - 1), perPager * (this.currentPage))
    },
    saveConditionListLen () {
      var k = 0
      /* eslint-disable no-unused-vars */
      for (var i in this.saveConditionData) {
        k++
      }
      return k
    },
    saveConditionData () {
      var obj = {}
      this.rowSetDataList.forEach((row) => {
        if (row.columnName && row.valueList && row.valueList.length > 0) {
          this.$set(obj, row.columnName, [])
          var columnType = this.getColumnType(row.columnName)
          // var valueList = row.valueList.map((k) => {
          //   if (this.dateTypeList.indexOf(columnType) >= 0 || this.dateTimeTypeList.indexOf(columnType) >= 0) {
          //     k = transToUTCMs(k)
          //     return k
          //   } else if (this.timeTypeList.indexOf(columnType) >= 0) {
          //     var timeContent = k.split(/[^\d]+/)
          //     if (timeContent && timeContent.length === 3) {
          //       return Date.UTC(1970, 0, 1, timeContent[0], timeContent[1], timeContent[2])
          //     }
          //     return k
          //   } else {
          //     return k
          //   }
          // })
          // obj[row.columnName] = valueList
          row.valueList.forEach((k) => {
            let valueRange = {
              type: 'CLOSED',
              leftExpr: '',
              rightExpr: ''
            }
            if (this.dateTypeList.indexOf(columnType) >= 0 || this.dateTimeTypeList.indexOf(columnType) >= 0) {
              k = transToUTCMs(k)
            } else if (this.timeTypeList.indexOf(columnType) >= 0) {
              var timeContent = k.split(/[^\d]+/)
              if (timeContent && timeContent.length === 3) {
                k = Date.UTC(1970, 0, 1, timeContent[0], timeContent[1], timeContent[2])
              }
            }
            valueRange.leftExpr = k
            valueRange.rightExpr = k
            obj[row.columnName].push(valueRange)
          })
        }
      })
      return obj
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
    'en': {delConfirm: 'The action will delete this restriction, still continue?', cancelButtonText: 'No', confirmButtonText: 'Yes', delSuccess: 'Access deleted successfully.', saveSuccess: 'Access saved successfully.', userName: 'User name', access: 'Access', restrict: 'Restrict', condition: 'Condition', rowAclDesc: 'By configuring this setting, the user will only be able to view data for the column that qualify the filtering criteria.', valueValidateFail: 'The input value does not match the column type.', 'pressEnter': 'Mutiple value can be entered. Hit enter to confirm each value,Click on calendar icon on the right side to input date or time.', preview: 'Preview'},
    'zh-cn': {delConfirm: '此操作将删除该授权，是否继续?', cancelButtonText: '否', confirmButtonText: '是', delSuccess: '行约束删除成功！', saveSuccess: '行约束保存成功！', userName: '用户名', access: '权限', restrict: '约束', condition: '条件', rowAclDesc: '通过以下设置，用户将仅能查看到表中列的值符合筛选条件的数据。', valueValidateFail: '输入值和列类型不匹配。', 'pressEnter': '每次输入列值后，按回车确认，可输入多个值，或点击最右日历图标选择时间', preview: '预览'}
  }
}
</script>
<style lang="less" >
@import '../../../less/config.less';
.accessrow{
  .el-tag--primary {
      background: rgba(33, 143, 234, 0.1);
      color: #218fea;
      border-color: rgba(33, 143, 234, 0.2) !important;
  }
  .line{
    width: 100%;
    height: 1px;
    background-color: #474E6A;
  }
  .el-select__tags {
    left: 0;
  }
  .el-dialog{
    .action_preview{
      color: @base-color;
      cursor: pointer;
      text-decoration: underline;
    }
    .previewDialog{
      box-shadow: 0 14px 6px 4px rgba(0,0,0,.3);
      .dot-bottom {
        font-size: 0;
        line-height: 0;
        border-width: 10px;
        border-color: #272822;
        border-bottom-width: 0;
        border-style: dashed;
        border-top-style: solid;
        border-left-color: transparent;
        border-right-color: transparent;
        position: absolute;
        left: 34px;
      }
      position: absolute;
      width: 100%;
      height: 100px;
      bottom:54px;
      left: -20px;
      z-index: 99999;
      border-radius: 5px 5px 5px 5px;
      background-color: #43496b;
      padding-bottom: 10px;
      .header_tool{
        height: 24px;
        line-height: 24px;
        padding-right: 10px;
        padding-left: 10px;
        span{
          display: inline-block;
          float: right;
          cursor: pointer;
          font-size: 16px;
          margin-top: 4px;
          &:before{
            font-size: 12px;
          }
        }
      }
      .ace_editor{
        height: 100%;
        border:none;
        border-radius: 0;
        .ace_gutter-layer{
          background-color: #2b2f43;
        }
        // .ace_scroller{
        //   left: 20px;
        // }
        .ace_gutter-cell{
          background-color: #2b2f43;
        }
      }
    }
    .el-dialog__body {
      padding-top: 10px;
      .el-form-item{
        margin-bottom: 10px;
      }
    }
    .el-col{
      // text-align: center;

      .el-button--danger {
        border:none;
      }
    }
    .el-input{
      padding:0;
    }
  }
}

</style>
