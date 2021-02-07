<template>
  <div class="user-access-block" v-loading="loading">
    <el-button type="primary" plain size="small" class="ksd-mb-10" @click="editAccess" v-if="!isEdit && isAuthority">{{$t('kylinLang.common.edit')}}</el-button>
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
            <kap-nodata :content="emptyText" v-else>
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
                <li v-for="col in pagedFilterColumns" :key="col.name">
                  <el-checkbox @change="val => selectColumn(val, col.name)" :disabled="!isCurrentTableChecked" size="medium" v-if="isEdit" :value="col.authorized">{{col.name}}</el-checkbox>
                  <span v-else>{{col.name}}</span>
                </li>
              </ul>
              <div class="list-load-more" @click="loadMoreCols" v-if="pagedFilterColumns.length<filterCols.length">{{$t('loadMore')}}</div>
            </div>
            <kap-nodata :content="emptyText2" v-else>
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
                  <el-col :span="isEdit ? 21 : 24">
                    <span>{{row.column_name}}</span><span v-if="row.items.length">={{row.items.toString()}}</span><span v-if="row.likeItems.length">&nbsp;like {{row.likeItems.toString()}}</span>
                  </el-col>
                  <el-col :span="3" class="ky-no-br-space btn-icons" v-if="isEdit">
                    <i class="el-icon-ksd-table_edit ksd-fs-16" @click="editRowAccess(key, row)"></i>
                    <i class="el-icon-ksd-table_delete ksd-fs-16 ksd-ml-10" @click="deleteRowAccess(key, row)"></i>
                  </el-col>
                </el-row>
              </li>
            </ul>
            <kap-nodata :content="emptyText3" v-if="!isCurrentTableChecked&&!rows.length">
            </kap-nodata>
            <div class="view-all-tips" v-if="isCurrentTableChecked&&!rows.length">
              <div><i class="point">•</i> {{$t('viewAllDataTips')}}</div>
              <div><i class="point">•</i> {{$t('viewAllDataTips1')}}</div>
            </div>
          </div>
        </div>
      </el-col>
    </el-row>
    <div class="expand-footer ky-no-br-space ksd-right" v-if="isEdit">
      <el-button plain size="small" @click="cancelAccess">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" :disabled="disabledSubmitBtn" plain size="small" class="ksd-ml-10" :loading="submitLoading" @click="submitAccess">{{$t('kylinLang.common.submit')}}</el-button>
    </div>

    <el-dialog :title="rowAuthorTitle" width="750px" class="author_dialog" :close-on-press-escape="false" :close-on-click-modal="false" :visible.sync="rowAccessVisible" @close="resetRowAccess">
      <div class="like-tips-block ksd-mb-10">
        <div class="ksd-mb-5">{{$t('tipsTitle')}}<span class="review-details" @click="showDetails = !showDetails">{{$t('viewDetail')}}<i :class="[showDetails ? 'el-icon-ksd-more_01-copy' : 'el-icon-ksd-more_02', 'arrow']"></i></span></div>
        <div class="detail-content" v-if="showDetails">
          <p>{{$t('rules1')}}</p>
          <p>{{$t('rules2')}}</p>
          <p>{{$t('rules3')}}</p>
        </div>
      </div>
      <div v-for="(row, key) in rowLists" :key="key" class="ksd-mb-10">
        <el-select v-model="row.column_name" class="row-column" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" filterable :disabled="isRowAuthorEdit" @change="isUnCharColumn(row.column_name)">
          <i slot="prefix" class="el-input__icon el-icon-search" v-if="!row.column_name"></i>
          <el-option v-for="c in columns" :disabled="c.datatype.indexOf('char') === -1 && c.datatype.indexOf('varchar') === -1 && row.joinType === 'like'" :key="c.name" :label="c.name" :value="c.name">
            <el-tooltip :content="c.name" effect="dark" placement="top"><span>{{c.name | omit(30, '...')}}</span></el-tooltip>
            <span class="ky-option-sub-info">{{c.datatype.toLocaleLowerCase()}}</span>
          </el-option>
        </el-select>
        <el-select
          :placeholder="$t('kylinLang.common.pleaseSelect')"
          style="width:65px;"
          class="link-type"
          popper-class="js_like-type"
          :disabled="isRowAuthorEdit"
          v-model="row.joinType">
          <el-option :disabled="isNeedDisableLike && key === 'like'" :value="key" v-for="(key, i) in linkKind" :key="i">{{key}}</el-option>
        </el-select>
        <el-select
          v-model="row.items"
          multiple
          filterable
          clearable
          remote
          allow-create
          default-first-option
          :class="{'row-values-edit': isRowAuthorEdit, 'row-values-add': !isRowAuthorEdit}"
          @change="setRowValues(row.items, key)"
          :placeholder="$t('pleaseInput')">
        </el-select>
        <span class="ky-no-br-space ksd-ml-10" v-if="!isRowAuthorEdit">
          <el-button type="primary" icon="el-icon-ksd-add_2" plain circle size="mini" @click="addRow" v-if="key==0"></el-button>
          <el-button icon="el-icon-minus" plain circle size="mini" @click="removeRow(key)"></el-button>
        </span>
      </div>
      <span slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain @click="cancelRowAccess" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" @click="submitRowAccess" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
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
      submitAccessData: 'SUBMIT_ACCESS_DATA',
      getAclPermission: 'GET_ACL_PERMISSION'
    })
  },
  locales: {
    'en': {
      accessTables: 'Table Access List',
      accessColumns: 'Column Access List',
      accessRows: 'Row Access List',
      searchKey: 'Search by table or column name',
      accessTips: 'All tables in current datasource are accessible.',
      accessColsTips: 'All columns in current table are accessible.',
      accessRowsTips: 'All rows in current table are accessible.',
      viewAllDataTips: 'After the row ACL was set, the user/user group could only access the data that match the specified filters.',
      viewAllDataTips1: 'For the columns without conditions set, user/user group could access all the data.',
      addRowAccess: 'Add Row ACL',
      addRowAccess1: 'Add Row ACL (Table: {tableName})',
      editRowAccess: 'Edit Row ACL (Table: {tableName})',
      pleaseInput: 'Confirm by pressing "enter" key and separate multiple values by comma.',
      loadMore: 'Load More',
      tipsTitle: 'The row ACL provides = and like operators. The like operator could only be used for char or varchar data type, and needs to be used with wildcards. ',
      viewDetail: 'View Rules',
      rules1: '_ (underscore) wildcard characters, matches any single character. ',
      rules2: '% (percent) wildcard characters, matches with zero or more characters.',
      rules3: '\\ (backslash) escape character. The characters following "\\" won\'t be regarded as any special characters.'
    },
    'zh-cn': {
      accessTables: '表级访问列表',
      accessColumns: '列级访问列表',
      accessRows: '行级访问列表',
      searchKey: '搜索表名或列名',
      accessTips: '当前数据源上所有表均可访问',
      accessColsTips: '当前表上所有列均可访问',
      accessRowsTips: '当前表上所有行均可访问',
      viewAllDataTips: '设置行级权限后，用户/用户组仅能查看到表中符合筛选条件的数据。',
      viewAllDataTips1: '对于没有设置权限的列，用户/用户组仍能够查看该列所有数据。',
      addRowAccess: '添加行级权限',
      addRowAccess1: '添加行级权限（表： {tableName}）',
      editRowAccess: '编辑行级权限（表： {tableName}）',
      pleaseInput: '请用回车进行输入确认并用逗号进行多个值分割',
      loadMore: '加载更多',
      tipsTitle: '行级权限支持 = 和 like 操作符。其中，like 仅支持 char 和 varchar 类型的列，需配合通配符使用。',
      viewDetail: '查看规则',
      rules1: '_（下划线）通配符，匹配任意单个字符。',
      rules2: '%（百分号）通配符，匹配空白字符或任意多个字符。',
      rules3: '\\（反斜杠）转义符，转义符后的通配符或转义符将不被识别为特殊字符。'
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
  rowLists = [{column_name: '', joinType: '=', items: []}]
  linkKind = ['=', 'like']
  isSelectTable = false
  tableAuthorizedNum = 0
  totalNum = 0
  defaultCheckedKeys = []
  defaultExpandedKeys = ['0']
  catchDefaultExpandedKeys = ['0']
  allTables = []
  copyOriginTables = []
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
  isAuthority = false
  showDetails = false
  isNeedDisableLike = false
  get emptyText () {
    return this.tableFilter ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get emptyText2 () {
    return this.columnFilter ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get emptyText3 () {
    return this.rowFilter ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get disabledSubmitBtn () {
    return JSON.stringify(this.copyOriginTables) === JSON.stringify(this.allTables)
  }
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
      this.$refs.tableTree && this.$refs.tableTree.setCurrentKey(this.currentTableId)
      const indexs = data.id.split('_')
      this.databaseIndex = indexs[0]
      this.tableIndex = indexs[1]
      this.initColsAndRows(data.columns, data.rows, data.like_rows, data.totalColNum)
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
  initColsAndRows (columns, rows, like_rows, totalColNum) {
    this.columnCurrentPage = 1
    this.colAuthorizedNum = 0
    this.columns = columns.map((col) => {
      if (col.authorized) {
        this.colAuthorizedNum++
      }
      return {name: col.column_name, authorized: col.authorized, datatype: col.datatype}
    })
    this.isAllColAccess = this.colAuthorizedNum === totalColNum
    this.selectAllColumns = this.isAllColAccess
    this.rows = rows.map((r) => {
      return { column_name: r.column_name, items: r.items, likeItems: [] }
    })
    like_rows && like_rows.forEach((l) => {
      const index = indexOfObjWithSomeKey(this.rows, 'column_name', l.column_name)
      if (index === -1) {
        this.rows.push({ column_name: l.column_name, items: [], likeItems: l.items })
      } else {
        this.rows[index].likeItems = l.items
      }
    })
  }
  getColumns (type) {
    let columns = this.columns
    if (type === 'like') {
      columns = columns.filter((c) => {
        return c.datatype.indexOf('char') !== -1 || c.datatype.indexOf('varchar') !== -1
      })
    }
    return columns
  }
  isUnCharColumn (columnName) {
    if (columnName) {
      const index = indexOfObjWithSomeKey(this.columns, 'name', columnName)
      let datatype = ''
      if (index !== -1) {
        datatype = this.columns[index].datatype
      }
      this.isNeedDisableLike = datatype.indexOf('char') === -1 && datatype.indexOf('varchar') === -1
    } else {
      this.isNeedDisableLike = false
    }
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
            this.setCurrentTable(d, val)
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
    this.allTables[indexs[0]].tables[indexs[1]].like_rows = []
    this.initColsAndRows(this.allTables[indexs[0]].tables[indexs[1]].columns, this.allTables[indexs[0]].tables[indexs[1]].rows, this.allTables[indexs[0]].tables[indexs[1]].like_rows, data.totalColNum)
  }
  checkChange (data, checkNode, node) {
    this.showLoading()
    setTimeout(() => {
      const isChecked = node.checked
      if (!data.children && !data.isMore) { // tables data 中‘非加载更多’的node
        this.handleTableData(data, isChecked)
        this.setCurrentTable(data, isChecked)
        this.reRenderTree()
      } else if (data.children && data.children.length) {
        data.originTables.forEach((d) => {
          if (!d.isMore) {
            this.handleTableData(d, isChecked)
            this.setCurrentTable(d, isChecked)
          }
        })
        this.setCurrentTable(data.children[0], isChecked)
        this.reRenderTree()
      }
      this.hideLoading()
    }, 100)
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
    for (let k = 0; k < row.likeItems.length; k++) {
      if (row.likeItems[k].toLowerCase().indexOf(this.rowFilter.trim().toLowerCase()) !== -1) {
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
  selectColumn (val, col) {
    let columns = this.allTables[this.databaseIndex].tables[this.tableIndex].columns
    const index = indexOfObjWithSomeKey(columns, 'column_name', col)
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
    this.rowLists[key].items = formatValues.filter(it => !!it)
  }
  addRowAccess () {
    this.isRowAuthorEdit = false
    this.rowAccessVisible = true
  }
  editRowAccess (index, row) {
    this.isRowAuthorEdit = true
    this.editRowIndex = index
    this.rowLists = []
    if (row.items.length > 0) {
      this.rowLists.push({column_name: row.column_name, joinType: '=', items: row.items})
    }
    if (row.likeItems.length > 0) {
      this.rowLists.push({column_name: row.column_name, joinType: 'like', items: row.likeItems})
    }
    this.rowAccessVisible = true
  }
  deleteRowAccess (index, row) {
    let idx = index
    this.rows.splice(idx, 1)
    if (row.likeItems && row.likeItems.length) {
      idx = this.allTables[this.databaseIndex].tables[this.tableIndex].like_rows.findIndex(it => it.items.join(',') === row.likeItems.join(','))
      this.allTables[this.databaseIndex].tables[this.tableIndex].like_rows.splice(idx, 1)
      this.tables[this.databaseIndex].originTables[this.tableIndex].like_rows.splice(idx, 1)
    }
    if (row.items.length) {
      idx = this.allTables[this.databaseIndex].tables[this.tableIndex].rows.findIndex(it => it.items.join(',') === row.items.join(','))
      this.allTables[this.databaseIndex].tables[this.tableIndex].rows.splice(idx, 1)
      this.tables[this.databaseIndex].originTables[this.tableIndex].rows.splice(idx, 1)
    }
  }
  cancelRowAccess () {
    this.rowAccessVisible = false
    this.isNeedDisableLike = false
  }
  submitRowAccess () {
    if (!this.isRowAuthorEdit) {
      this.rowLists.forEach((row) => {
        if (row.column_name && row.items.length) {
          const index = indexOfObjWithSomeKey(this.rows, 'column_name', row.column_name)
          if (index === -1) {
            const rowObj = {column_name: row.column_name, items: row.joinType === '=' ? row.items : [], likeItems: row.joinType === 'like' ? row.items : []}
            this.rows.push(rowObj)
          } else {
            if (row.joinType === '=') {
              this.rows[index].items = [...this.rows[index].items, ...row.items]
            } else if (row.joinType === 'like') {
              this.rows[index].likeItems = [...this.rows[index].likeItems, ...row.items]
            }
          }
        }
      })
    } else {
      this.rowLists.forEach((r) => {
        if (r.joinType === '=') {
          this.rows[this.editRowIndex].items = r.items
        } else if (r.joinType === 'like') {
          this.rows[this.editRowIndex].likeItems = r.items
        }
      })
      if (this.rows[this.editRowIndex].items.length === 0 && this.rows[this.editRowIndex].likeItems.length === 0) {
        this.rows.splice(this.editRowIndex, 1)
      }
    }
    this.allTables[this.databaseIndex].tables[this.tableIndex].rows = this.rows.map((row) => {
      return { column_name: row.column_name, items: row.items }
    }).filter((row) => {
      return row.items.length > 0
    })
    this.allTables[this.databaseIndex].tables[this.tableIndex].like_rows = this.rows.map((row) => {
      return { column_name: row.column_name, items: row.likeItems }
    }).filter((row) => {
      return row.items.length > 0
    })
    this.tables[this.databaseIndex].originTables[this.tableIndex].rows = this.rows.map((row) => {
      return { column_name: row.column_name, items: row.items }
    }).filter((row) => {
      return row.items.length > 0
    })
    this.tables[this.databaseIndex].originTables[this.tableIndex].like_rows = this.rows.map((row) => {
      return { column_name: row.column_name, items: row.likeItems }
    }).filter((row) => {
      return row.items.length > 0
    })
    this.rowAccessVisible = false
  }
  addRow () {
    this.rowLists.unshift({column_name: '', joinType: '=', items: []})
  }
  removeRow (index) {
    this.rowLists.splice(index, 1)
  }
  resetRowAccess () {
    this.rowLists = [{column_name: '', joinType: '=', items: []}]
    this.isNeedDisableLike = false
  }
  async loadAccessDetails (authorizedOnly) {
    this.defaultCheckedKeys = []
    this.allTables = []
    this.tables = []
    this.rows = []
    this.columns = []
    const response = await this.getAccessDetailsByUser({data: {authorized_only: authorizedOnly, project: this.projectName}, roleOrName: this.roleOrName, type: this.type, projectName: this.projectName})
    const result = await handleSuccessAsync(response)
    if (result.length) {
      this.allTables = objectClone(result)
      this.copyOriginTables = objectClone(result)
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
          return {id: id, label: t.table_name, database: database.database_name, authorized: t.authorized, columns: t.columns, rows: t.rows, like_rows: t.like_rows, totalColNum: t.total_column_num}
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
    this.getAclPermission({project: this.projectName}).then((res) => {
      const { data } = res.data
      handleSuccess(res, () => {
        this.isAuthority = data
      })
    }, (err) => {
      handleError(err)
    })
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  .access-content {
    .row-list {
      .el-col {
        display: block !important;
        word-break: break-all;
      }
    }
  }
  .row-column {
    width: 210px;
  }
  .row-values-edit {
    width: calc(~'100% - 285px');
  }
  .row-values-add {
    width: calc(~'100% - 355px');
  }
  .row-values-edit,
  .row-values-add {
    .el-select__tags > span {
      max-width: 100%;
      .el-tag {
        max-width: 100%;
        position: relative;
        padding-right: 16px;
        .el-select__tags-text {
          max-width: 100%;
          overflow: hidden;
          text-overflow: ellipsis;
          display: inline-block;
        }
        .el-tag__close {
          position: absolute;
          right: 1px;
          top: 5px;
        }
      }
    }
  }
  .author_dialog {
    .el-dialog {
      position: absolute;
      left: 0;
      right: 0;
      margin: auto;
      max-height: 70%;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      .el-dialog__header {
        min-height: 47px;
      }
      .el-dialog__body {
        overflow: auto;
      }
    }
  }
  .like-tips-block {
    color: @text-normal-color;
    .review-details {
      color: @base-color;
      cursor: pointer;
      position: relative;
    }
    .el-icon-ksd-more_01-copy {
      transform: scale(0.8);
    }
    .arrow {
      transform: rotate(90deg) scale(0.8);
      margin-left: 3px;
      font-size: 7px;
      color: @base-color;
      position: absolute;
      top: 4px;
    }
    .detail-content {
      background-color: @base-background-color-1;
      padding: 10px 15px;
      box-sizing: border-box;
      font-size: 12px;
      color: @text-normal-color;
    }
  }
</style>
