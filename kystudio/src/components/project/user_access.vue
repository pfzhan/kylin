<template>
  <div class="user-access-block">
    <el-button type="primary" plain size="small" class="ksd-mb-10" @click="editAccess" v-if="!isEdit">{{$t('kylinLang.common.edit')}}</el-button>
    <el-row>
      <el-col :span="8">
        <div class="access-card">
          <div class="access-title">
            <span v-if="!isEdit">{{$t('accessTables')}} ({{tableAuthorizedNum}})</span>
            <el-checkbox v-model="selectAllTables" @change="checkAllTables" v-else>{{$t('accessTables')}} ({{tableAuthorizedNum}}/{{totalNum}})</el-checkbox>
          </div>
          <div class="access-search">
            <el-input size="small" :placeholder="$t('searchKey')" v-model="tableFilter">
              <i slot="prefix" class="el-input__icon el-icon-search"></i>
            </el-input>
          </div>
          <div class="access-tips" v-if="isAllTablesAccess&&!isEdit">
            <i class="el-icon-ksd-info ksd-fs-14"></i>
            <span class="ksd-fs-12">{{$t('accessTips')}}</span>
          </div>
          <div class="access-content">
            <el-tree :data="tables" v-if="tables.length" :show-checkbox="isEdit" show-overflow-tooltip :props="defaultProps" @node-click="handleNodeClick" node-key="id" :default-expanded-keys="['0']" :default-checked-keys="defaultCheckedKeys" @check-change="checkChange" ref="tableTree" :highlight-current="true">
              <span class="custom-tree-node" slot-scope="{ node, data }">
                <i class="ksd-mr-2" :class="data.icon"></i>
                <span class="ky-ellipsis" :title="node.label">{{ node.label }}</span>
              </span>
            </el-tree>
            <kap-nodata v-else>
            </kap-nodata>
          </div>
        </div>
      </el-col>
      <el-col :span="8">
        <div class="access-card column-card">
          <div class="access-title">
            <span v-if="!isEdit">{{$t('accessColumns')}} ({{colAuthorizedNum}})</span>
            <el-checkbox v-model="selectAllColumns" @change="checkAllColumns" v-else>{{$t('accessColumns')}} ({{colAuthorizedNum}}/{{columns.length}})</el-checkbox>
          </div>
          <div class="access-search">
            <el-input size="small" :placeholder="$t('searchKey')" v-model="tableFilter">
              <i slot="prefix" class="el-input__icon el-icon-search"></i>
            </el-input>
          </div>
          <div class="access-tips" v-if="isAllColAccess&&!isEdit">
            <i class="el-icon-ksd-info ksd-fs-14"></i>
            <span class="ksd-fs-12">{{$t('accessColsTips')}}</span>
          </div>
          <div class="access-content">
            <ul v-if="columns.length">
              <li v-for="(col, index) in columns" :key="col.name">
                <el-checkbox @change="val => selectColumn(val, index)" size="medium" v-if="isEdit" :value="col.authorized">{{col.name}}</el-checkbox>
                <span v-else>{{col.name}}</span>
              </li>
            </ul>
            <kap-nodata v-else>
            </kap-nodata>
          </div>
        </div>
      </el-col>
      <el-col :span="8">
        <div class="access-card row-card">
          <div class="access-title">
            <span>{{$t('accessRows')}}</span>
            <el-button type="primary" plain size="small" icon="el-icon-ksd-add_2" class="ksd-fright ksd-mt-5" @click="addRowAccess" v-if="isEdit">{{$t('addRowAccess')}}</el-button>
          </div>
          <div class="access-search">
            <el-input size="small" :placeholder="$t('searchKey')" v-model="tableFilter">
              <i slot="prefix" class="el-input__icon el-icon-search"></i>
            </el-input>
          </div>
          <div class="access-tips" v-if="isCurrentTableChecked&&!rows.length">
            <i class="el-icon-ksd-info ksd-fs-14"></i>
            <span class="ksd-fs-12">{{$t('accessRowsTips')}}</span>
          </div>
          <div class="access-content">
            <ul v-if="rows.length">
              <li v-for="(row, key) in rows" :key="key" class="row-list">
                <el-row>
                  <el-col :span="isEdit?21:24">
                    <span>{{row.column_name}}</span>
                    <span>=</span>
                    <span>{{row.items.toString()}}</span>
                  </el-col>
                  <el-col :span="3" class="ky-no-br-space btn-icons" v-if="isEdit">
                    <i class="el-icon-ksd-table_edit ksd-fs-16" @click="editRowAccess(key, row)"></i>
                    <i class="el-icon-ksd-table_delete ksd-fs-16 ksd-ml-10" @click="deleteRowAccess(key)"></i>
                  </el-col>
                </el-row>
              </li>
            </ul>
            <kap-nodata v-if="!isCurrentTableChecked&&!rows.length">
            </kap-nodata>
            <div class="view-all-tips" v-if="isCurrentTableChecked&&!rows.length">{{$t('viewAllDataTips')}}</div>
          </div>
        </div>
      </el-col>
    </el-row>
    <div class="expand-footer ky-no-br-space ksd-right" v-if="isEdit">
      <el-button plain size="small" @click="cancelAccess">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" plain size="small" class="ksd-ml-10" :loading="submitLoading" @click="submitAccess">{{$t('kylinLang.common.submit')}}</el-button>
    </div>

    <el-dialog :title="rowAuthorTitle" width="720px" class="author_dialog" :close-on-press-escape="false" :close-on-click-modal="false" :visible.sync="rowAccessVisible" @close="resetRowAccess">
      <div v-for="(row, key) in rowLists" :key="key" class="ksd-mb-10">
        <el-select v-model="row.column_name" :placeholder="$t('kylinLang.common.pleaseSelect')" :disabled="isRowAuthorEdit">
          <el-option v-for="c in columns" :key="c.name" :label="c.name" :value="c.name">
          </el-option>
        </el-select>
        <span>=</span>
        <el-select
          v-model="row.items"
          multiple
          filterable
          clearable
          remote
          allow-create
          default-first-option
          :style="{width: isRowAuthorEdit? '470px' : '400px'}"
          @change="setRowValues(row.items, key)"
          :placeholder="$t('pleaseInput')">
        </el-select>
        <span class="ky-no-br-space ksd-ml-10" v-if="!isRowAuthorEdit">
          <el-button type="primary" icon="el-icon-ksd-add_2" plain circle size="mini" @click="addRow" v-if="key==0"></el-button>
          <el-button icon="el-icon-minus" plain circle size="mini" @click="removeRow(key)"></el-button>
        </span>
      </div>
      <span slot="footer" class="dialog-footer ky-no-br-space">
        <el-button @click="cancelRowAccess" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" plain @click="submitRowAccess" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { handleSuccessAsync, indexOfObjWithSomeKey, objectClone } from '../../util'
import { handleSuccess, handleError } from '../../util/business'
import { mapActions } from 'vuex'
@Component({
  props: ['roleOrName', 'projectName', 'type'],
  methods: {
    ...mapActions({
      getAccessDetailsByUser: 'GET_ACCESS_DETAILS_BY_USER',
      submitAccessData: 'SUBMIT_ACCESS_DATA'
    })
  },
  locales: {
    'en': {
      accessTables: 'Table Access List',
      accessColumns: 'Column Access List',
      accessRows: 'Row Access List',
      searchKey: 'Please input a keyword',
      accessTips: 'All tables in current datasource are accessible.',
      accessColsTips: 'All columns in current table are accessible.',
      accessRowsTips: 'All rows in current table are accessible.',
      viewAllDataTips: 'By setting the row ACL, the user/group will only be able to view data that match the specified filters. For the columns without conditions, users will be able to view all the data for this column.',
      addRowAccess: 'Add Row ACL',
      addRowAccess1: 'Add Row ACL - {tableName}',
      editRowAccess: 'Edit Row ACL - {tableName}',
      pleaseInput: 'Please hit enter to confirm each value. Multiple values are supported, please use comma to split.'
    },
    'zh-cn': {
      accessTables: '表级访问列表',
      accessColumns: '列级访问列表',
      accessRows: '行级访问列表',
      searchKey: '请输入关键字搜索',
      accessTips: '当前数据源上所有表均可访问',
      accessColsTips: '当前表上所有列均可访问',
      accessRowsTips: '当前表上所有行均可访问',
      viewAllDataTips: '通过设置行级权限，用户/组将仅能查看到表中符合筛选条件的数据。对于没有设置的列，将默认能够查看该列所有数据。',
      addRowAccess: '添加行级权限',
      addRowAccess1: '添加行级权限 - {tableName}',
      editRowAccess: '编辑行级权限 - {tableName}',
      pleaseInput: '请输入后按回车确认。支持输入多个值，请用逗号分隔。'
    }
  }
})
export default class UserAccess extends Vue {
  defaultProps = {
    children: 'children',
    label: 'label'
  }
  tables = []
  isEdit = false
  isRowAuthorEdit = false
  rowAccessVisible = false
  tableFilter = ''
  columnFilter = ''
  rowFilter = ''
  columns = []
  rows = []
  rowLists = [{column_name: '', items: []}]
  isSelectTable = false
  tableAuthorizedNum = 0
  totalNum = 0
  defaultCheckedKeys = []
  allTables = []
  databaseIndex = -1
  tableIndex = -1
  editRowIndex = -1
  currentTable = ''
  isAllTablesAccess = false
  isAllColAccess = false
  colAuthorizedNum = 0
  submitLoading = false
  isCurrentTableChecked = false
  selectAllTables = false
  selectAllColumns = false
  currentTableId = ''
  handleNodeClick (data) {
    if (!data.children) { // tables data
      this.isSelectTable = true
      this.isCurrentTableChecked = data.authorized
      this.currentTableId = data.id
      this.$refs.tableTree.setCurrentKey(this.currentTableId)
      const indexs = data.id.split('_')
      this.databaseIndex = indexs[0]
      this.tableIndex = indexs[1]
      this.currentTable = data.label
      this.initColsAndRows(data.columns, data.rows, data.totalColNum)
    }
  }
  initColsAndRows (columns, rows, totalColNum) {
    this.colAuthorizedNum = 0
    this.columns = columns.map((col) => {
      if (col.authorized) {
        this.colAuthorizedNum++
      }
      return {name: col.column_name, authorized: col.authorized}
    })
    this.isAllColAccess = this.colAuthorizedNum === totalColNum
    this.selectAllColumns = this.isAllColAccess
    this.rows = objectClone(rows)
  }
  checkAllTables (val) {
    const checkedKeys = this.tables.map((database) => {
      return database.id
    })
    if (val) {
      this.$refs.tableTree.setCheckedKeys(checkedKeys)
    } else {
      this.$refs.tableTree.setCheckedKeys([])
    }
  }
  checkAllColumns (val) {
    this.colAuthorizedNum = 0
    this.columns.forEach((col) => {
      col.authorized = val
      if (col.authorized) {
        this.colAuthorizedNum++
      }
    })
    this.allTables[this.databaseIndex].tables[this.tableIndex].columns.forEach((col) => {
      col.authorized = val
    })
  }
  checkChange (data, isChecked) {
    if (!data.children) { // tables data
      data.authorized = isChecked
      const indexs = data.id.split('_')
      this.allTables[indexs[0]].tables[indexs[1]].authorized = isChecked
      this.currentTable = data.label
      this.isCurrentTableChecked = isChecked
      this.selectAllColumns = isChecked
      this.allTables[indexs[0]].tables[indexs[1]].columns.forEach((col) => {
        col.authorized = isChecked
      })
      this.allTables[indexs[0]].tables[indexs[1]].rows = []
      this.initColsAndRows(this.allTables[indexs[0]].tables[indexs[1]].columns, this.allTables[indexs[0]].tables[indexs[1]].rows, data.totalColNum)
      if (isChecked) {
        this.tableAuthorizedNum++
      } else {
        this.tableAuthorizedNum--
      }
    }
  }
  get rowAuthorTitle () {
    return !this.isRowAuthorEdit ? this.$t('addRowAccess1', {tableName: this.currentTable}) : this.$t('editRowAccess', {tableName: this.currentTable})
  }
  editAccess () {
    this.loadAccessDetails(false)
    this.isEdit = true
  }
  cancelAccess () {
    this.isEdit = false
    this.loadAccessDetails(true)
  }
  submitAccess () {
    this.submitLoading = true
    this.submitAccessData({projectName: this.projectName, userType: this.type, roleOrName: this.roleOrName, accessData: this.allTables}).then((res) => {
      handleSuccess(res, () => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.submitSuccess')
        })
        this.submitLoading = false
        this.isEdit = false
        this.loadAccessDetails(true)
      })
    }, (res) => {
      handleError(res)
      this.submitLoading = false
    })
  }
  selectColumn (val, index) {
    let columns = this.allTables[this.databaseIndex].tables[this.tableIndex].columns
    columns[index].authorized = val
    this.columns[index].authorized = val
    if (val) {
      this.colAuthorizedNum++
    } else {
      this.colAuthorizedNum--
    }
  }
  setRowValues (values, key) {
    let formatValues = []
    values.forEach((v) => {
      const val = v.trim().split(/[,，]/g)
      formatValues = [...formatValues, ...val]
    })
    this.rowLists[key].items = formatValues
  }
  addRowAccess () {
    this.isRowAuthorEdit = false
    this.rowAccessVisible = true
  }
  editRowAccess (index, row) {
    this.isRowAuthorEdit = true
    this.editRowIndex = index
    this.rowLists = [objectClone(row)]
    this.rowAccessVisible = true
  }
  deleteRowAccess (index) {
    this.rows.splice(index, 1)
    this.allTables[this.databaseIndex].tables[this.tableIndex].rows.splice(index, 1)
  }
  cancelRowAccess () {
    this.rowAccessVisible = false
  }
  submitRowAccess () {
    if (!this.isRowAuthorEdit) {
      this.rowLists.forEach((row) => {
        if (row.column_name && row.items.length) {
          const index = indexOfObjWithSomeKey(this.rows, 'column_name', row.column_name)
          if (index === -1) {
            this.rows.push(row)
          } else {
            this.rows[index].items = [...this.rows[index].items, row.items]
          }
        }
      })
    } else {
      if (this.rowLists[0].items.length > 0) {
        this.rows[this.editRowIndex].items = this.rowLists[0].items
      } else {
        this.rows.splice(this.editRowIndex, 1)
      }
    }
    this.allTables[this.databaseIndex].tables[this.tableIndex].rows = objectClone(this.rows)
    this.rowAccessVisible = false
  }
  addRow () {
    this.rowLists.unshift({column_name: '', items: []})
  }
  removeRow (index) {
    this.rowLists.splice(index, 1)
  }
  resetRowAccess () {
    this.rowLists = [{column_name: '', items: []}]
  }
  async loadAccessDetails (authorizedOnly) {
    this.defaultCheckedKeys = []
    this.allTables = []
    this.tables = []
    const response = await this.getAccessDetailsByUser({data: {authorizedOnly: authorizedOnly}, roleOrName: this.roleOrName, type: this.type, projectName: this.projectName})
    const result = await handleSuccessAsync(response)
    if (result.length) {
      this.allTables = result
      this.tableAuthorizedNum = 0
      this.totalNum = 0
      this.currentTableId = ''
      this.tables = result.map((database, key) => {
        this.tableAuthorizedNum = this.tableAuthorizedNum + database.authorized_table_num
        this.totalNum = this.totalNum + database.total_table_num
        const labelNum = this.authorizedOnly ? ` (${database.authorized_table_num})` : ` (${database.authorized_table_num}/${database.total_table_num})`
        return {
          id: key + '',
          label: database.database_name + labelNum,
          children: database.tables.map((t, i) => {
            const id = key + '_' + i
            if (t.authorized && !authorizedOnly) {
              this.defaultCheckedKeys.push(id)
            }
            if (t.table_name === this.currentTable) {
              this.currentTableId = id
            }
            return {id: id, label: t.table_name, authorized: t.authorized, columns: t.columns, rows: t.rows, totalColNum: t.total_column_num}
          })
        }
      })
      this.isAllTablesAccess = this.tableAuthorizedNum === this.totalNum
      this.$nextTick(() => {
        if (this.currentTableId) {
          const indexs = this.currentTableId.split('_')
          this.handleNodeClick(this.tables[indexs[0]].children[indexs[1]])
        } else {
          this.handleNodeClick(this.tables[0].children[0])
        }
      })
    }
  }
  created () {
    this.loadAccessDetails(true)
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
</style>
