<template>
  <div id="snapshot">
    <div class="ksd-title-label">{{$t('snapshotList')}}
      <common-tip placement="bottom-start">
        <div slot="content" class="snapshot-desc">
          <p>*&nbsp;{{$t('snapshotDesc1')}}</p>
          <p>*&nbsp;{{$t('snapshotDesc2')}}</p>
          <p>*&nbsp;{{$t('snapshotDesc3')}}</p>
        </div>
        <i class="el-icon-ksd-what ksd-fs-14"></i>
      </common-tip>
    </div>
    <div class="ksd-mtb-10">{{$t('snapshotDesc')}}</div>

    <div class="clearfix">
      <div class="ksd-fleft ky-no-br-space" v-if="datasourceActions.includes('snapshotAction')">
        <el-button-group class="ksd-mr-10 ke-it-add_snapshot"><el-button icon="el-icon-plus" @click="addSnapshot">{{$t('snapshot')}}</el-button></el-button-group>
        <el-button-group class="ke-it-other_actions">
          <el-button icon="el-icon-ksd-table_refresh" :disabled="!multipleSelection.length || hasEventAuthority('refresh')" @click="refreshSnapshot">{{$t('kylinLang.common.refresh')}}</el-button>
          <el-button icon="el-icon-ksd-table_delete" :disabled="!multipleSelection.length" @click="deleteSnap">{{$t('kylinLang.common.delete')}}</el-button>
        </el-button-group>
      </div>
      <el-input class="ksd-fright search-input ke-it-search_snapshot" v-global-key-event.enter.debounce="onFilterChange" @clear="onFilterChange()" :value="filter.table" @input="handleFilterInput" prefix-icon="el-icon-search" :placeholder="$t('searchSnapshot')" size="medium"></el-input>
    </div>
    <el-table class="ksd-mt-10 snapshot-table ke-it-snapshot_table"
      :data="snapshotTables"
      @sort-change="sortSnapshotList"
      :default-sort = "{prop: 'last_modified_time', order: 'descending'}"
      :empty-text="emptyText"
      :row-class-name="setRowClass"
      @selection-change="handleSelectionChange"
      border
      style="width: 100%">
      <el-table-column type="selection" align="center" width="44"></el-table-column>
      <el-table-column
        prop="table"
        :label="$t('tableName')"
        sortable="custom"
        show-overflow-tooltip>
        <template slot-scope="scope">
          <div v-if="scope.row.forbidden_colunms&&scope.row.forbidden_colunms.length" style="height: 23px;">
            <span v-custom-tooltip="{text: scope.row.table, w: 20, tableClassName: 'snapshot-table'}">{{scope.row.table}}</span>
            <i class="el-icon-ksd-lock ksd-fs-16" @click="openAuthorityDetail(scope.row)"></i>
          </div>
          <span v-else>{{scope.row.table}}</span>
        </template>
      </el-table-column>
      <el-table-column
        prop="database"
        :label="$t('databaseName')"
        show-overflow-tooltip
        width="130">
      </el-table-column>
      <el-table-column
        prop="usage"
        sortable="custom"
        :label="$t('usage')"
        header-align="right"
        align="right"
        width="120">
      </el-table-column>
      <el-table-column
        width="180"
        :filters="allStatus.map(item => ({text: $t(item), value: item}))" :filtered-value="filter.status" :label="$t('status')" filter-icon="el-icon-ksd-filter" :show-multiple-footer="false" :filter-change="(v) => filterContent(v, 'status')">
        <template slot-scope="scope">
          <el-tag size="mini" :type="getTagType(scope.row)">{{scope.row.status}}</el-tag>
        </template>
      </el-table-column>
      <el-table-column
        header-align="right"
        align="right"
        prop="storage"
        sortable="custom"
        show-overflow-tooltip
        width="120px"
        :label="$t('storage')">
        <template slot-scope="scope">
          {{scope.row.storage | dataSize}}
        </template>
      </el-table-column>
      <el-table-column
        prop="lookup_table_count"
        info-icon="el-icon-ksd-what"
        :info-tooltip="$t('lookupModelsTip')"
        :label="$t('lookupModels')"
        header-align="right"
        align="right"
        width="200">
      </el-table-column>
      <el-table-column
        prop="fact_table_count"
        info-icon="el-icon-ksd-what"
        :info-tooltip="$t('factModelsTip')"
        :label="$t('factModels')"
        header-align="right"
        align="right"
        width="180">
      </el-table-column>
      <el-table-column prop="last_modified_time" sortable="custom" width="180" show-overflow-tooltip :label="$t('modifyTime')">
        <template slot-scope="scope">
          <span v-if="!scope.row.last_modified_time">-</span>
          <span v-else>{{scope.row.last_modified_time | toServerGMTDate}}</span>
        </template>
      </el-table-column>
    </el-table>
    <kap-pager :totalSize="snapshotTotal" :curPage="filter.page_offset+1"  v-on:handleCurrentChange='currentChange' ref="snapshotPager" :refTag="pageRefTags.snapshotPager" :perPageSize="20" class="ksd-mtb-10 ksd-center" ></kap-pager>

    <!-- 添加Snapshot -->
    <SnapshotModel v-on:reloadSnapshotList="getSnapshotList"/>

    <el-dialog
      :visible.sync="authorityVisible"
      width="480px"
      :close-on-click-modal="false"
      class="authority-dialog ke-it-authority_snapshot"
      :close="handleClose">
      <span slot="title" class="ksd-title-label">{{$t('authorityTitle')}}</span>
      <div class="ksd-m-b-10">{{$t('authorityTips', {snapshot: snapshotObj.table})}}</div>
      <div class="ksd-mb-5 ksd-mt-10">{{$t('columns')}}({{snapshotObj.forbidden_colunms.length}})</div>
      <div class="authority-block">
        <el-button class="copy-btn"
          v-clipboard:copy="snapshotObj.forbidden_colunms.join('\r\n')"
          v-clipboard:success="onCopy"
          v-clipboard:error="onError"
          size="mini">{{$t('kylinLang.common.copy')}}</el-button>
        <p v-for="c in snapshotObj.forbidden_colunms" :key="c">{{c}}</p>
      </div>
      <span slot="footer" class="dialog-footer">
        <el-button plain @click="authorityVisible = false">{{$t('kylinLang.common.close')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { handleSuccessAsync, handleError, kapConfirm } from '../../../util'
import { pageRefTags } from 'config'
import SnapshotModel from './SnapshotModel/SnapshotModel.vue'

@Component({
  beforeRouteEnter (to, from, next) {
    next(vm => {
      if (to.params.table) {
        vm.filter.table = to.params.table
      }
      vm.getSnapshotList()
    })
  },
  components: {
    SnapshotModel
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'datasourceActions'
    ])
  },
  methods: {
    ...mapActions({
      fetchSnapshotList: 'FETCH_SNAPSHOT_LIST',
      refreshSnapshotTable: 'REFRESH_SNAPSHOT_TABLE',
      deleteSnapshotCheck: 'DELETE_SNAPSHOT_CHECK',
      deleteSnapshot: 'DELETE_SNAPSHOT'
    }),
    ...mapActions('SnapshotModel', {
      showSnapshotModelDialog: 'CALL_MODAL'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  locales
})
export default class Snapshot extends Vue {
  pageRefTags = pageRefTags
  snapshotTables = []
  allStatus = ['ONLINE', 'LOADING', 'REFRESHING']
  filter = {
    page_offset: 0,
    page_size: +localStorage.getItem(this.pageRefTags.snapshotPager) || 20,
    status: [],
    sort_by: 'last_modified_time',
    reverse: true,
    table: ''
  }
  snapshotTotal = 0
  multipleSelection = []
  authorityVisible = false
  snapshotObj = {
    table: '',
    forbidden_colunms: []
  }

  handleClose () {
    this.authorityVisible = false
    this.snapshotObj = {
      table: '',
      forbidden_colunms: []
    }
  }

  openAuthorityDetail (row) {
    this.snapshotObj = row
    this.authorityVisible = true
  }

  onCopy () {
    this.$message.success(this.$t('kylinLang.common.copySuccess'))
  }
  onError () {
    this.$message.error(this.$t('kylinLang.common.copyfail'))
  }

  get emptyText () {
    return this.filter.table ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  setRowClass (res) {
    const { row } = res
    return 'forbidden_colunms' in row && row.forbidden_colunms.length ? 'no-authority-model' : ''
  }
  handleFilterInput (val) {
    this.filter.table = val
  }
  onFilterChange () {
    this.filter.page_offset = 0
    this.getSnapshotList()
  }
  sortSnapshotList ({ column, prop, order }) {
    if (order === 'ascending') {
      this.filter.reverse = false
    } else {
      this.filter.reverse = true
    }
    this.filter.sort_by = prop
    this.filter.page_offset = 0
    this.getSnapshotList()
  }
  getTagType (row) {
    if (row.status === 'ONLINE') {
      return 'success'
    } else if (row.status === 'WARNING') {
      return 'warning'
    } else if (['LOCKED'].includes(row.status)) {
      return 'info'
    } else {
      return ''
    }
  }
  // 状态控制按钮的使用
  hasEventAuthority (type) {
    let typeList = (type) => {
      return this.multipleSelection.length && typeof this.multipleSelection[0] !== 'undefined' ? this.multipleSelection.filter(it => !type.includes(it.status)).length > 0 : false
    }
    if (type === 'refresh') {
      return typeList(['ONLINE'])
    }
  }
  filterContent (val, type) {
    this.filter[type] = val
    this.filter.page_offset = 0
    this.getSnapshotList()
  }
  // renderFactModelsHeader (h, { column, $index }) {
  //   return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
  //     <span>{this.$t('factModels')}</span>&nbsp;
  //     <common-tip placement="top" content={this.$t('factModelsTip')}>
  //      <span class='el-icon-ksd-what'></span>
  //     </common-tip>
  //   </span>)
  // }
  // renderLookupModelsHeader (h, { column, $index }) {
  //   return (<span class="ky-hover-icon" onClick={e => (e.stopPropagation())}>
  //     <span>{this.$t('lookupModels')}</span>&nbsp;
  //     <common-tip placement="top" content={this.$t('lookupModelsTip')}>
  //      <span class='el-icon-ksd-what'></span>
  //     </common-tip>
  //   </span>)
  // }
  handleSelectionChange (val) {
    this.multipleSelection = val
  }
  async refreshSnapshot () {
    if (!this.multipleSelection.length) return
    try {
      await kapConfirm(this.$t('refreshTips', {snapshotNum: this.multipleSelection.length}), {confirmButtonText: this.$t('kylinLang.common.refresh'), dangerouslyUseHTMLString: true, type: 'warning'}, this.$t('refreshTitle'))
      const tables = this.multipleSelection.map((s) => {
        return s.database + '.' + s.table
      })
      await this.refreshSnapshotTable({ project: this.currentSelectedProject, tables })
      this.$message({
        dangerouslyUseHTMLString: true,
        type: 'success',
        customClass: 'build-full-load-success',
        message: (
          <div>
            <span>{this.$t('kylinLang.common.buildSuccess')}</span>
            <a href="javascript:void(0)" onClick={() => this.$router.push('/monitor/job')}>{this.$t('kylinLang.common.toJoblist')}</a>
          </div>
        )
      })
      this.getSnapshotList()
    } catch (e) {
      handleError(e)
    }
  }
  async deleteSnap () {
    if (!this.multipleSelection.length) return
    const tables = this.multipleSelection.map((s) => {
      return s.database + '.' + s.table
    })
    const res = await this.deleteSnapshotCheck({ project: this.currentSelectedProject, tables })
    const data = await handleSuccessAsync(res)
    if (data.affected_jobs && data.affected_jobs.length) {
      await this.callGlobalDetail(data.affected_jobs, this.$t('deleteTips', {snapshotNum: this.multipleSelection.length}) + this.$t('deleteTablesTitle'), this.$t('deleteTitle'), 'warning', this.$t('kylinLang.common.delete'))
    } else {
      await kapConfirm(this.$t('deleteTips', {snapshotNum: this.multipleSelection.length}), {confirmButtonText: this.$t('kylinLang.common.delete'), dangerouslyUseHTMLString: true, type: 'warning'}, this.$t('deleteTitle'))
    }
    await this.deleteSnapshot({ project: this.currentSelectedProject, tables: tables.join(',') })
    this.$message({
      type: 'success',
      message: this.$t('kylinLang.common.delSuccess')
    })
    this.getSnapshotList()
  }
  async callGlobalDetail (targetTables, msg, title, type, submitText) {
    const tableData = []
    targetTables.forEach((t) => {
      const obj = {}
      obj['table'] = t.table
      obj['database'] = t.database
      tableData.push(obj)
    })
    await this.callGlobalDetailDialog({
      msg: msg,
      title: title,
      detailTableData: tableData,
      detailColumns: [
        {column: 'table', label: this.$t('tableName')},
        {column: 'database', label: this.$t('databaseName')}
      ],
      dialogType: type,
      showDetailBtn: false,
      submitText: submitText
    })
  }
  async addSnapshot (filterText) {
    const isSubmit = await this.showSnapshotModelDialog()
    if (isSubmit) {
      this.getSnapshotList()
    }
  }
  async getSnapshotList () {
    try {
      this.filter.project = this.currentSelectedProject
      const res = await this.fetchSnapshotList(this.filter)
      const { value, total_size } = await handleSuccessAsync(res)
      this.snapshotTables = value
      this.snapshotTotal = total_size
    } catch (e) {
      handleError(e)
    }
  }
  currentChange (size, count) {
    this.filter.page_offset = size
    this.filter.page_size = count
    this.getSnapshotList()
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
#snapshot {
  margin: 20px;
  .search-input {
    width: 400px;
  }
  .snapshot-table {
    .el-icon-ksd-filter {
      position: relative;
      font-size: 17px;
      top: 2px;
      left: 5px;
    }
    .el-table__row.no-authority-model {
      background-color: @table-stripe-color;
      color: @text-disabled-color;
    }
    .ksd-nobr-text {
      width: calc(~'100% - 20px');
    }
    .el-icon-ksd-lock {
      position: absolute;
      right: 10px;
      top: 10px;
      color: @text-title-color;
    }
  }
}
.authority-dialog {
  .authority-block {
    border: 1px solid @line-border-color;
    background-color: @base-background-color;
    color: @text-normal-color;
    padding: 10px;
    border-radius: 2px;
    position: relative;
    .copy-btn {
      position: absolute;
      right: 10px;
    }
  }
}
</style>
