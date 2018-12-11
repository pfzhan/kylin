<template>
    <div class="accessrow">
       <el-button type="primary" plain icon="el-icon-plus" @click="addGrant" v-show="hasSomeProjectPermission || isAdmin" size="medium">{{$t('restrict')}}</el-button>
       <div style="width:200px;" class="ksd-mb-10 ksd-fright">
          <el-input :placeholder="$t('kylinLang.common.userOrGroup')" size="medium" @input="searchRowAcl" v-model="serarchChar" class="show-search-btn" :prefix-icon="searchLoading? 'el-icon-loading':'el-icon-search'">
          </el-input>
        </div>
       <el-table class="ksd-mt-20"
            border
            :data="aclRowList"
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
              :label="$t('condition')"
              >
              <template slot-scope="scope">
                <p v-for="(key,val) in scope.row.conditions">{{val}} = {{key.join(',')}}</p>
              </template>
            </el-table-column>
            <el-table-column v-if="hasSomeProjectPermission || isAdmin"
              width="100"
              prop="Action"
              :label="$t('kylinLang.common.action')">
              <template slot-scope="scope">
                <common-tip :content="$t('kylinLang.common.edit')">
                  <el-button size="mini" class="ksd-btn del" icon="el-icon-ksd-table_edit" @click="editAclOfRow(scope.row.name, scope.row.conditions, scope.row.nameType)"></el-button>
                </common-tip>
                <common-tip :content="$t('kylinLang.common.delete')">
                  <el-button size="mini" class="ksd-btn del" icon="el-icon-delete" @click="delAclOfRow(scope.row.name, scope.row.nameType)"></el-button>
                </common-tip>
              </template>
            </el-table-column>
          </el-table>
          <kap-pager
            class="ksd-center ksd-mt-20 ksd-mb-20" ref="pager"
            :totalSize="aclRowTotalSize"
            @handleCurrentChange="handleCurrentChange">
          </kap-pager>
          <el-dialog :title="$t('restrict')" width="660px" :visible.sync="addGrantDialog" @close="closeDialog" :close-on-press-escape="false" :close-on-click-modal="false">
              <el-alert
                :title="$t('rowAclDesc')"
                show-icon
                class="ksd-mb-10"
                :closable="false"
                type="warning">
              </el-alert>
              <el-form :model="grantObj" ref="aclOfRowForm" :rules="aclTableRules" v-if="addGrantDialog">
                <el-form-item :label="$t('kylinLang.common.userOrGroup')" label-width="100px" required>
                  <!-- <el-autocomplete  v-model="grantObj.name" style="width:100%" :fetch-suggestions="querySearchAsync"></el-autocomplete> -->
                  <el-col :span="11">
                     <el-select v-model="assignType" style="width:100%" :disabled="isEdit" size="medium"  :placeholder="$t('kylinLang.common.pleaseSelect')" @change="changeAssignType">
                    <el-option
                      v-for="item in assignTypes"
                      :key="item.value"
                      :label="item.label"
                      :value="item.value">
                    </el-option>
                  </el-select>
                  </el-col>
                  <el-col :span="11" class="ksd-ml-10">
                   <!--  <el-select filterable v-if="assignType === 'user'" v-model="grantObj.name" style="width:100%" :disabled="isEdit"  :placeholder=" $t('kylinLang.common.pleaseSelectUserName')">
                      <el-option v-for="b in aclWhiteList" :value="b.value">{{b.value}}</el-option>
                    </el-select> -->
                    <el-form-item  prop="name">
                      <kap-filter-select :asyn="true" @req="getWhiteListOfTable"  v-model="grantObj.name" style="width:100%" :disabled="isEdit"  v-show="assignType === 'user'" :dataMap="{label: 'value', value: 'value'}" :list="aclWhiteList" placeholder="kylinLang.common.pleaseInputUserName" :size="100"></kap-filter-select>


                      <!-- <el-select filterable v-if="assignType === 'group'" v-model="grantObj.name" style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelectUserGroup')" :disabled="isEdit" >
                      <el-option v-for="b in aclWhiteGroupList" :value="b.value">{{b.value}}</el-option>
                    </el-select> -->

                    <kap-filter-select v-model="grantObj.name" style="width:100%" :disabled="isEdit"  v-show="assignType === 'group'" :dataMap="{label: 'value', value: 'value'}" :list="aclWhiteGroupList" placeholder="kylinLang.common.pleaseInputUserGroup" :size="100"></kap-filter-select>
                    </el-form-item>
                  </el-col>
                </el-form-item>
                <el-form-item :label="$t('condition')" label-width="100px" class="ksd-mt-20 is-required" style="position:relative">
                  <el-row v-for="(rowset, index) in rowSetDataList" :key="index">
                    <el-col :sm="20" :md="21" :lg="22">
                      <el-select v-model="rowset.columnName" size="medium" style="width:100%" :placeholder="$t('kylinLang.common.pleaseSelectColumnName')" @change="selectColumnName(index, rowset.columnName)">
                        <el-option v-for="(item, index) in filterHasSelected(columnList, index, rowSetDataList, 'columnName')" :key="item.name" :label="item.name" :value="item.name">
                        <span style="float: left">{{ item.name }}</span>
                          <span style="float: right; color: #8492a6; font-size: 13px">{{ item.datatype }}</span>
                        </el-option>
                      </el-select>


                    </el-col>
                    <el-col :sm="4" :md="3" :lg="2" class="ksd-right">
                      <el-button size="medium" class="ksd-btn del" icon="el-icon-delete" @click="removeRowSet(index)"></el-button>
                      <!-- <el-button type="danger" icon="el-icon-minus" @click="removeRowSet(index)"></el-button> -->
                    </el-col>
                    <el-col :span="24" class="ksd-pt-4" style="position: relative">
                      <arealabel  :placeholder="$t('pressEnter')" :ignoreSplitChar="true" @refreshData="refreshData" :selectedlabels="rowset.valueList" :allowcreate='true'  :refreshInfo="{columnName: rowset.columnName, index: index}" @validateFail="validateFail" :validateRegex="rowset.validateReg"></arealabel>
                      <datepicker ref="hideDatePicker" :dateType="rowset.timeComponentType" :selectList="rowset.valueList" v-show="dateTypeList.indexOf(getColumnType(rowset.columnName))>=0 || dateTimeTypeList.indexOf(getColumnType(rowset.columnName))>=0"></datepicker>
                    </el-col>
                    <el-col :span="24"><div class="ky-line ksd-mtb-14"></div></el-col>
                  </el-row>
                  <el-button type="primary" plain icon="el-icon-plus" class="" @click="addRowSet" size="medium">{{$t('condition')}}</el-button>
                  <!-- <div @click="openPreview" style="width:40px;" class="action-preview ksd-ml-10" v-show="!(saveConditionListLen === 0 || saveConditionListLen !== rowSetDataList.length)">{{$t('preview')}}</div> -->
                  <div class="row-condition-preview">
                    <el-button type="primary" text @click="openPreview" v-show="!(saveConditionListLen === 0 || saveConditionListLen !== rowSetDataList.length)">{{$t('preview')}}</el-button>
                    <div v-show="previewDialgVisible" class="preview-dialog">
                      <div class="header_tool">SQL<span class="el-icon-close" @click="previewDialgVisible=false"></span></div>
                      <editor class="ksd-mt-4" ref="preview" lang="sql" useWrapMode="true" v-model="previewInfo"  theme="chrome"></editor>
                      <span class="dot-bottom"></span>
                    </div>
                  </div>
                  
                </el-form-item>
              </el-form>
              <div slot="footer" class="dialog-footer">
                <el-button @click="addGrantDialog = false">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="primary" plain :loading="saveBtnLoad" @click="saveAclTable" :disabled="saveConditionListLen === 0 || saveConditionListLen !== rowSetDataList.length">{{$t('kylinLang.common.submit')}}</el-button>
              </div>
          </el-dialog>
    </div>
</template>
<script>
import { mapActions } from 'vuex'
import { handleSuccess, handleError, kapConfirm, hasRole, hasPermission, transToUtcTimeFormat, transToUtcDateFormat, transToUTCMs } from 'util/business'
import { pageCount, permissions, assignTypes } from '../../../config'
import arealabel from 'components/common/area_label'
import datepicker from 'components/common/date_picker'
export default {
  name: 'rowaccess',
  data () {
    return {
      grantObj: {
        name: ''
      },
      addGrantDialog: false,
      columnList: [],
      currentPage: 1,
      aclRowUserData: [],
      aclRowGroupData: [],
      aclRowTotalSize: 0,
      serarchChar: '',
      aclWhiteList: [],
      previewInfo: '',
      saveBtnLoad: false,
      previewDialgVisible: false,
      assignTypes: assignTypes,
      isEdit: false,
      timeSelect: '',
      rowSetDataList: [],
      intTypeList: ['int', 'smallint', 'tinyint', 'integer', 'bigint'],
      dateTypeList: ['date'],
      dateTimeTypeList: ['datetime', 'timestamp'],
      timeTypeList: ['time'],
      assignType: 'user',
      aclWhiteGroupList: [],
      searchLoading: false,
      ST: null,
      aclTableRules: {
        name: [{
          required: true, message: this.$t('kylinLang.common.pleaseInputUserOrGroupName'), trigger: 'change'
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
      previewSQL: 'PREVIEW_ACL_SET_ROW_SQL',
      getGroupList: 'GET_GROUP_LIST'
    }),
    changeAssignType () {
      if (!this.isEdit) {
        this.grantObj.name = ''
      }
      this.getWhiteListOfTable()
    },
    getGroups () {
      this.getGroupList({
        project: this.$store.state.project.selected_project
      }).then((res) => {
        handleSuccess(res, (data) => {
          var result = []
          data.forEach((d) => {
            result.push({value: d})
          })
          this.aclWhiteGroupList = result
        })
      }, (res) => {
        handleError(res)
      })
    },
    openPreview () {
      this.previewSQL({
        tableName: this.tableName,
        project: this.$store.state.project.selected_project,
        userName: this.grantObj.name,
        conditions: {condsWithColumn: this.saveConditionData}
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
      this.$refs.aclOfRowForm.clearValidate()
      this.previewDialgVisible = false
    },
    selectColumnName (i, columnName) {
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
      console.log(a)
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
    delAclOfRow (userName, assignType) {
      kapConfirm(this.$t('delConfirm'), {cancelButtonText: this.$t('cancelButtonText'), confirmButtonText: this.$t('confirmButtonText')}).then(() => {
        this.delAclSetOfRow({
          tableName: this.tableName,
          project: this.$store.state.project.selected_project,
          userName: userName,
          type: assignType
        }).then((res) => {
          handleSuccess(res, (data) => {
            this.getAllAclSetOfRow()
            this.$message({message: this.$t('delSuccess'), type: 'success'})
          })
        }, (res) => {
          handleError(res)
        })
      })
    },
    editAclOfRow (userName, conditions, userType) {
      this.isEdit = true
      this.addGrantDialog = true
      this.grantObj.name = userName
      this.rowSetDataList.splice(0, this.rowSetDataList.length)
      this.assignType = userType
      this.$nextTick(() => {
        for (var i in conditions) {
          var obj = {
            columnName: i,
            valueList: conditions[i]
          }
          var result = this.getFilterRegExp(i)
          obj.timeComponentType = result.timeComponentType
          obj.validateReg = result.reg
          this.rowSetDataList.push(obj)
        }
      })
    },
    resetAclRowObj () {
      this.grantObj.name = ''
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
            conditions: {condsWithColumn: this.saveConditionData},
            type: this.assignType
          }).then((res) => {
            this.saveBtnLoad = false
            this.addGrantDialog = false
            // handleSuccess(res, (data) => {})
            this.getAllAclSetOfRow()
            this.$message({message: this.$t('saveSuccess'), type: 'success'})
          }, (res) => {
            this.saveBtnLoad = false
            // this.addGrantDialog = false
            handleError(res)
          })
        }
      })
    },
    handleCurrentChange (curpage) {
      this.currentPage = curpage
      this.getAllAclSetOfRow()
    },
    getAllAclSetOfRow () {
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
      return this.getAclSetOfRow(para).then((res) => {
        handleSuccess(res, (data) => {
          this.aclRowUserData = data.user || []
          this.aclRowGroupData = data.group || []
          this.aclRowTotalSize = data.size
          this.getWhiteListOfTable()
        })
      }, (res) => {
        handleError(res)
      })
    },
    searchRowAcl () {
      clearTimeout(this.ST)
      this.ST = setTimeout(() => {
        this.searchLoading = true
        this.getAllAclSetOfRow().then(() => {
          this.searchLoading = false
        }, () => {
          this.searchLoading = false
        })
      }, 500)
    },
    getWhiteListOfTable (filterUserName) {
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
    },
    renderRowAclList (aclRowData, type) {
      var result = []
      aclRowData.forEach((row) => {
        for (var i in row) {
          var name = i
          var nameType = type
          var conditions = {}
          for (var c in row[i]) {
            conditions[c] = []
            row[i][c].forEach((_condition) => {
              var columnType = this.getColumnType(c)
              var columnMatchVal = _condition[1]
              var condition = ''
              if (this.dateTypeList.indexOf(columnType) >= 0) {
                condition = transToUtcDateFormat(+columnMatchVal)
              } else if (this.dateTimeTypeList.indexOf(columnType) >= 0) {
                condition = transToUtcTimeFormat(+columnMatchVal)
              } else if (this.timeTypeList.indexOf(columnType) >= 0) {
                var k = transToUtcTimeFormat(+columnMatchVal)
                var temps = k.split(/\s+/)
                if (temps.length >= 2) {
                  condition = temps[1]
                } else {
                  condition = k
                }
              } else {
                condition = columnMatchVal
              }
              conditions[c].push(condition)
            })
          }
          result.push({name: name, nameType: nameType, conditions: conditions})
        }
      })
      return result
    }
  },
  computed: {
    tableName () {
      var curTableData = this.$store.state.datasource.currentShowTableData
      this.columnList = curTableData.columns.slice(0)
      return curTableData.database + '.' + curTableData.name
    },
    aclRowList () {
      return this.renderRowAclList(this.aclRowGroupData, 'group').concat(this.renderRowAclList(this.aclRowUserData, 'user'))
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
      return hasPermission(this, permissions.ADMINISTRATION.mask)
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  watch: {
  },
  mounted () {
    this.getAllAclSetOfRow()
  },
  locales: {
    'en': {delConfirm: 'The action will delete this restriction, still continue?', cancelButtonText: 'No', confirmButtonText: 'Yes', delSuccess: 'Access deleted successfully.', saveSuccess: 'Access saved successfully.', userName: 'User name', access: 'Access', restrict: 'Restrict', condition: 'Condition', rowAclDesc: 'By configuring this setting, the user/group will only be able to view data for the column that qualify the filtering criteria.', valueValidateFail: 'The input value does not match the column type.', 'pressEnter': 'Mutiple value can be entered. Hit enter to confirm each value,Click on calendar icon on the right side to input date or time.', preview: 'Preview'},
    'zh-cn': {delConfirm: '此操作将删除该授权，是否继续?', cancelButtonText: '否', confirmButtonText: '是', delSuccess: '行约束删除成功！', saveSuccess: '行约束保存成功！', userName: '用户名', access: '权限', restrict: '约束', condition: '条件', rowAclDesc: '通过以下设置，用户/组将仅能查看到表中列的值符合筛选条件的数据。', valueValidateFail: '输入值和列类型不匹配。', 'pressEnter': '每次输入列值后，按回车确认，可输入多个值，或点击最右日历图标选择时间', preview: '预览'}
  }
}
</script>
<style lang="less" >
@import '../../../assets/styles/variables.less';
.accessrow{
  .el-dialog{
    .row-condition-preview{
       position: relative;
       display: inline-block;
       margin-left: 10px;

    }
    .preview-dialog{
      // box-shadow: 0 14px 6px 4px rgba(0,0,0,.3);
      .dot-bottom {
        font-size: 0;
        line-height: 0;
        border-width: 10px;
        border-color: @text-normal-color;
        border-bottom-width: 0;
        border-style: dashed;
        border-top-style: solid;
        border-left-color: transparent;
        border-right-color: transparent;
        position: absolute;
        left: 34px;
      }
      position: absolute;
      width: 404px;
      height: 100px;
      bottom:46px;
      left: -34px;
      z-index: 99999;
      border-radius: 5px 5px 5px 5px;
      background-color: @text-normal-color;
      // padding-bottom: 10px;
      .header_tool{
        height: 18px;
        line-height: 18px;
        position: relative;
        padding-right: 10px;
        // padding-left: 10px;
        span{
          display: inline-block;
          float: right;
          cursor: pointer;
          font-size: 16px;
          margin-top: 4px;
          color:@text-secondary-color;
          &:before{
            font-size: 12px;
          }
        }
      }
      .ace_editor{
        height: 75%!important;
        width: 98%!important;
        // border:none;
        border-radius: 0;
        border: solid 4px @text-normal-color;
      }
    }
  }
}

</style>
