<template>
  <div class="user-access-block" v-loading="loading">
    <el-button type="primary" plain size="small" class="ksd-mb-10" @click="editAccess" v-if="!isEdit">{{$t('kylinLang.common.edit')}}</el-button>
    <el-row>
      <el-col :span="8">
        <div class="access-card">
          <div class="access-title">
            <span v-if="!isEdit">{{$t('accessTables')}} ({{tableAuthorizedNum}})</span>
            <el-checkbox v-model="isAllTablesAccess" @change="checkAllTables" :indeterminate="tableAuthorizedNum !== totalNum && tableAuthorizedNum>0" :disabled="!tables.length" v-else>{{$t('accessTables')}} ({{tableAuthorizedNum}}/{{totalNum}})</el-checkbox>
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
          <div class="access-content tree-content" :class="{'all-tips': isAllTablesAccess&&!isEdit}">
            <el-tree
              v-if="filterTableData.length&&isRerender"
              show-overflow-tooltip
              node-key="id"
              ref="tableTree"
              class="acl-tree"
              :data="filterTableData"
              :show-checkbox="isEdit"
              :props="defaultProps"
              :render-after-expand="false"
              :highlight-current="true"
              :default-expanded-keys="defaultExpandedKeys"
              :default-checked-keys="defaultCheckedKeys"
              @check="checkChange"
              @node-expand="pushTableId"
              @node-collapse="removeTableId"
              @node-click="handleNodeClick">
              <span class="custom-tree-node" slot-scope="{ node, data }">
                <i class="ksd-mr-2" :class="data.icon"></i>
                <span class="ky-ellipsis" :class="data.class" :title="node.label">{{ node.label }}</span>
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
            <el-checkbox v-model="selectAllColumns" @change="checkAllColumns" :disabled="!isCurrentTableChecked || !columns.length" :indeterminate="colAuthorizedNum !== columns.length && colAuthorizedNum>0" v-else>{{$t('accessColumns')}} ({{colAuthorizedNum}}/{{columns.length}})</el-checkbox>
          </div>
          <div class="access-search">
            <el-input size="small" :placeholder="$t('searchKey')" v-model="columnFilter">
              <i slot="prefix" class="el-input__icon el-icon-search"></i>
            </el-input>
          </div>
          <div class="access-tips" v-if="isAllColAccess&&!isEdit">
            <i class="el-icon-ksd-info ksd-fs-14"></i>
            <span class="ksd-fs-12">{{$t('accessColsTips')}}</span>
          </div>
          <div class="access-content" :class="{'all-tips': isAllColAccess&&!isEdit}">
            <div v-if="pagedFilterColumns.length">
              <ul>
                <li v-for="(col, index) in pagedFilterColumns" :key="col.name">
                  <el-checkbox @change="val => selectColumn(val, index)" :disabled="!isCurrentTableChecked" size="medium" v-if="isEdit" :value="col.authorized">{{col.name}}</el-checkbox>
                  <span v-else>{{col.name}}</span>
                </li>
              </ul>
              <div class="list-load-more" @click="loadMoreCols" v-if="pagedFilterColumns.length<filterCols.length">{{$t('loadMore')}}</div>
            </div>
            <kap-nodata v-else>
            </kap-nodata>
          </div>
        </div>
      </el-col>
      <el-col :span="8">
        <div class="access-card row-card">
          <div class="access-title">
            <span>{{$t('accessRows')}}</span>
            <el-button type="primary" plain size="small" icon="el-icon-ksd-add_2" class="ksd-fright ksd-mt-5" @click="addRowAccess" v-if="isEdit" :disabled="!isCurrentTableChecked">{{$t('addRowAccess')}}</el-button>
          </div>
          <div class="access-search">
            <el-input size="small" :placeholder="$t('searchKey')" v-model="rowFilter">
              <i slot="prefix" class="el-input__icon el-icon-search"></i>
            </el-input>
          </div>
          <div class="access-tips" v-if="isCurrentTableChecked&&!rows.length">
            <i class="el-icon-ksd-info ksd-fs-14"></i>
            <span class="ksd-fs-12">{{$t('accessRowsTips')}}</span>
          </div>
          <div class="access-content">
            <ul v-if="rows.length">
              <li v-for="(row, key) in rows" :key="key" class="row-list" v-show="isShowRow(row)">
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
        <el-select v-model="row.column_name" :placeholder="$t('kylinLang.common.pleaseSelect')" filterable :disabled="isRowAuthorEdit">
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
import { pageSizeMapping } from '../../config'
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
      pleaseInput: 'Please hit enter to confirm each value. Multiple values are supported, please use comma to split.',
      loadMore: 'Load More ...'
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
      pleaseInput: '请输入后按回车确认。支持输入多个值，请用逗号分隔。',
      loadMore: '加载更多 ...'
    }
  }
})
export default class UserAccess extends Vue {
  defaultProps = {
    children: 'children',
    label: 'label'
  }
  tables = []
  filterOriginDatas = []
  isEdit = false
  isRerender = true
  isRowAuthorEdit = false
  rowAccessVisible = false
  tableFilter = ''
  columnFilter = ''
  rowFilter = ''
  columns = []
  filterCols = []
  rows = []
  rowLists = [{column_name: '', items: []}]
  isSelectTable = false
  tableAuthorizedNum = 0
  totalNum = 0
  defaultCheckedKeys = []
  defaultExpandedKeys = ['0']
  catchDefaultExpandedKeys = ['0']
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
  selectAllColumns = false
  currentTableId = ''
  loading = false
  columnPageSize = 100
  columnCurrentPage = 1
  showLoading () {
    this.loading = true
  }
  hideLoading () {
    this.loading = false
  }
  pushTableId (data) {
    const index = this.catchDefaultExpandedKeys.indexOf(data.id)
    if (index === -1) {
      this.catchDefaultExpandedKeys.push(data.id)
    }
  }
  removeTableId (data) {
    const index = this.catchDefaultExpandedKeys.indexOf(data.id)
    if (index !== -1) {
      this.catchDefaultExpandedKeys.splice(index, 1)
    }
  }
  handleNodeClick (data, node) {
    if (!data.children && !data.isMore) { // tables data 中‘非加载更多’的node
      this.isSelectTable = true
      this.currentTable = data.database + '.' + data.label
      this.isCurrentTableChecked = data.authorized
      this.currentTableId = data.id
      this.$refs.tableTree.setCurrentKey(this.currentTableId)
      const indexs = data.id.split('_')
      this.databaseIndex = indexs[0]
      this.tableIndex = indexs[1]
      this.initColsAndRows(data.columns, data.rows, data.totalColNum)
    } else if (!data.children && data.isMore) {
      node.parent.data.currentIndex++
      const index = data.id.split('_')[0]
      this.tables[index].currentIndex++
      const renderNums = node.parent.data.currentIndex * pageSizeMapping.TABLE_TREE
      const renderMoreTables = node.parent.data.originTables.slice(0, renderNums)
      if (renderNums < node.parent.data.originTables.length) {
        renderMoreTables.push(data)
      }
      node.parent.data.children = renderMoreTables
      this.reRenderTree(true)
    }
  }
  initColsAndRows (columns, rows, totalColNum) {
    this.columnCurrentPage = 1
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
  get pagedFilterColumns () {
    const filterCols = objectClone(this.columns)
    this.filterCols = filterCols.filter((col) => {
      return col.name.toLowerCase().indexOf(this.columnFilter.trim().toLowerCase()) !== -1
    })
    return this.filterCols.slice(0, this.columnCurrentPage * this.columnPageSize)
  }
  loadMoreCols () {
    this.columnCurrentPage++
  }
  checkAllTables (val) {
    this.showLoading()
    setTimeout(() => {
      for (let i = this.tables.length - 1; i >= 0; i--) {
        this.tables[i].originTables.forEach((d) => {
          if (!d.isMore) {
            this.handleTableData(d, val)
          }
        })
      }
      this.setCurrentTable(this.tables[0].children[0], val)
      this.reRenderTree()
      this.hideLoading()
    }, 500)
  }
  checkAllColumns (val) {
    this.colAuthorizedNum = 0
    this.columns.forEach((col) => {
      col.authorized = val
      if (col.authorized) {
        this.colAuthorizedNum++
      }
    })
    const columns = this.allTables[this.databaseIndex].tables[this.tableIndex].columns
    columns.forEach((col) => {
      col.authorized = val
    })
    this.tables[this.databaseIndex].originTables[this.tableIndex].columns = columns
  }
  handleTableData (data, isChecked) {
    const indexs = data.id.split('_')
    this.allTables[indexs[0]].tables[indexs[1]].authorized = isChecked
    let database = objectClone(this.tables[indexs[0]])
    let defaultCheckedKeys = objectClone(this.defaultCheckedKeys)
    if (isChecked && !data.authorized) {
      this.tableAuthorizedNum++
      database.authorizedNum++
      database.label = database.databaseName + ` (${database.authorizedNum}/${database.totalNum})`
      defaultCheckedKeys.push(data.id)
      if (database.authorizedNum && database.authorizedNum === database.totalNum) {
        defaultCheckedKeys.push(database.id)
      }
    } else if (!isChecked && data.authorized) {
      this.tableAuthorizedNum--
      database.authorizedNum--
      database.label = database.databaseName + ` (${database.authorizedNum}/${database.totalNum})`
      const removeKeyIndex = defaultCheckedKeys.indexOf(data.id)
      defaultCheckedKeys.splice(removeKeyIndex, 1)
      const removeDatabaseKeyIndex = defaultCheckedKeys.indexOf(database.id)
      if (removeDatabaseKeyIndex !== -1) {
        defaultCheckedKeys.splice(removeDatabaseKeyIndex, 1)
      }
    }
    data.authorized = isChecked
    data.columns.forEach((col) => {
      col.authorized = isChecked
    })
    data.rows = []
    database.originTables[indexs[1]] = data
    this.tables[indexs[0]] = database
    this.defaultCheckedKeys = defaultCheckedKeys
    this.isAllTablesAccess = this.tableAuthorizedNum === this.totalNum
  }
  setCurrentTable (data, isChecked) {
    const indexs = data.id.split('_')
    this.databaseIndex = indexs[0]
    this.tableIndex = indexs[1]
    this.currentTable = data.database + '.' + data.label
    this.currentTableId = data.id
    this.isCurrentTableChecked = isChecked
    this.selectAllColumns = isChecked
    this.allTables[indexs[0]].tables[indexs[1]].columns.forEach((col) => {
      col.authorized = isChecked
    })
    this.allTables[indexs[0]].tables[indexs[1]].rows = []
    this.initColsAndRows(this.allTables[indexs[0]].tables[indexs[1]].columns, this.allTables[indexs[0]].tables[indexs[1]].rows, data.totalColNum)
  }
  checkChange (data, checkNode, node) {
    const isChecked = node.checked
    if (!data.children && !data.isMore) { // tables data 中‘非加载更多’的node
      this.handleTableData(data, isChecked)
      this.setCurrentTable(data, isChecked)
      this.reRenderTree()
    } else if (data.children && data.children.length) {
      this.showLoading()
      setTimeout(() => {
        data.originTables.forEach((d) => {
          if (!d.isMore) {
            this.handleTableData(d, isChecked)
          }
        })
        this.setCurrentTable(data.children[0], isChecked)
        this.reRenderTree()
        this.hideLoading()
      }, 100)
    }
  }
  reRenderTree (isLoadMoreRender) { // isLoadMoreRender 为 true 时，不重置数据
    this.isRerender = false
    if (!isLoadMoreRender) {
      this.tables = [...this.tables]
    }
    this.defaultExpandedKeys = objectClone(this.catchDefaultExpandedKeys)
    this.$nextTick(() => {
      this.isRerender = true
      this.handleLoadMoreStyle()
      setTimeout(() => {
        this.$refs.tableTree.setCurrentKey(this.currentTableId)
      })
    })
  }
  get rowAuthorTitle () {
    return !this.isRowAuthorEdit ? this.$t('addRowAccess1', {tableName: this.currentTable}) : this.$t('editRowAccess', {tableName: this.currentTable})
  }
  handleLoadMoreStyle () {
    this.$nextTick(() => {
      const loadMore = this.$el.querySelectorAll('.acl-tree .load-more')
      const indeterminateNodes = this.$el.querySelectorAll('.acl-tree .indeterminate-node')
      if (loadMore.length) {
        loadMore.forEach((m) => {
          const targetCheckbox = m.parentNode.parentNode.querySelector('.el-checkbox')
          if (targetCheckbox) {
            targetCheckbox.style.display = 'none'
          }
        })
      }
      if (indeterminateNodes.length) {
        indeterminateNodes.forEach((n) => {
          const indeterminateCheckbox = n.parentNode.parentNode.querySelector('.el-checkbox .el-checkbox__input')
          if (indeterminateCheckbox) {
            indeterminateCheckbox.className = 'el-checkbox__input is-indeterminate'
          }
        })
      }
    })
  }
  get filterTableData () {
    let filterOriginDatas = objectClone(this.tables)
    filterOriginDatas = filterOriginDatas.filter((data) => {
      const originFilterTables = data.originTables.filter((t) => {
        return t.label.toLowerCase().indexOf(this.tableFilter.trim().toLowerCase()) !== -1
      })
      const pagedFilterTables = originFilterTables.slice(0, pageSizeMapping.TABLE_TREE * data.currentIndex)
      if (pageSizeMapping.TABLE_TREE < originFilterTables.length) {
        pagedFilterTables.push({
          id: data.id + '_more',
          label: this.$t('loadMore'),
          class: 'load-more ksd-fs-12',
          isMore: true
        })
      }
      data.children = pagedFilterTables
      return pagedFilterTables.length > 0
    })
    this.defaultExpandedKeys = objectClone(this.catchDefaultExpandedKeys)
    this.$nextTick(() => {
      this.handleLoadMoreStyle()
      if (this.$refs.tableTree) {
        this.$refs.tableTree.setCurrentKey(this.currentTableId)
      }
    })
    return filterOriginDatas
  }
  isShowRow (row) {
    let isShow = false
    if (row.column_name.toLowerCase().indexOf(this.rowFilter.trim().toLowerCase()) !== -1) {
      isShow = true
    }
    for (let i = 0; i < row.items.length; i++) {
      if (row.items[i].toLowerCase().indexOf(this.rowFilter.trim().toLowerCase()) !== -1) {
        isShow = true
        break
      }
    }
    return isShow
  }
  editAccess () {
    this.isEdit = true
    this.loadAccessDetails(false)
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
    this.tables[this.databaseIndex].originTables[this.tableIndex].columns = columns
    this.columns[index].authorized = val
    if (val) {
      this.colAuthorizedNum++
    } else {
      this.colAuthorizedNum--
    }
    this.selectAllColumns = this.colAuthorizedNum === this.columns.length
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
    this.tables[this.databaseIndex].originTables[this.tableIndex].rows.splice(index, 1)
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
            this.rows[index].items = [...this.rows[index].items, ...row.items]
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
    this.tables[this.databaseIndex].originTables[this.tableIndex].rows = objectClone(this.rows)
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
    this.rows = []
    this.columns = []
    const response = await this.getAccessDetailsByUser({data: {authorizedOnly: authorizedOnly}, roleOrName: this.roleOrName, type: this.type, projectName: this.projectName})
    const result = await handleSuccessAsync(response)
    if (result.length) {
      this.allTables = objectClone(result)
      this.tableAuthorizedNum = 0
      this.totalNum = 0
      this.currentTableId = ''
      this.tables = result.map((database, key) => {
        this.tableAuthorizedNum = this.tableAuthorizedNum + database.authorized_table_num
        this.totalNum = this.totalNum + database.total_table_num
        const labelNum = this.authorizedOnly ? ` (${database.authorized_table_num})` : ` (${database.authorized_table_num}/${database.total_table_num})`
        const originTableDatas = database.tables.map((t, i) => {
          const id = key + '_' + i
          if (t.authorized && !authorizedOnly) {
            this.defaultCheckedKeys.push(id)
          }
          if (database.database_name + '.' + t.table_name === this.currentTable) {
            this.currentTableId = id
          }
          return {id: id, label: t.table_name, database: database.database_name, authorized: t.authorized, columns: t.columns, rows: t.rows, totalColNum: t.total_column_num}
        })
        const pegedTableDatas = originTableDatas.slice(0, pageSizeMapping.TABLE_TREE)
        if (pageSizeMapping.TABLE_TREE < originTableDatas.length) {
          pegedTableDatas.push({
            id: key + '_more',
            label: this.$t('loadMore'),
            class: 'load-more ksd-fs-12',
            isMore: true
          })
        }
        if (database.authorized_table_num && database.authorized_table_num === database.total_table_num) {
          this.defaultCheckedKeys.push(key + '')
        }
        return {
          id: key + '',
          label: database.database_name + labelNum,
          class: database.authorized_table_num && database.authorized_table_num < database.total_table_num ? 'indeterminate-node' : '',
          databaseName: database.database_name,
          authorizedNum: database.authorized_table_num,
          totalNum: database.total_table_num,
          originTables: originTableDatas,
          currentIndex: 1,
          children: pegedTableDatas
        }
      })
      this.isAllTablesAccess = this.tableAuthorizedNum === this.totalNum
      this.$nextTick(() => {
        if (this.currentTableId) {
          const indexs = this.currentTableId.split('_')
          if (indexs[1] <= pageSizeMapping.TABLE_TREE) {
            this.handleNodeClick(this.tables[indexs[0]].children[indexs[1]])
          } else {
            this.handleNodeClick(this.tables[0].children[0])
          }
        } else {
          this.handleNodeClick(this.tables[0].children[0])
        }
        this.handleLoadMoreStyle()
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
