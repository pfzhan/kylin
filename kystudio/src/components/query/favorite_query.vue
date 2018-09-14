<template>
  <div id="favoriteQuery">
    <div class="clearfix ksd-mb-10">
      <div class="ksd-fleft table-title">
        <span>{{$t('kylinLang.menu.favorite_query')}}</span>
        <el-tooltip placement="right">
          <div slot="content">{{$t('favDesc')}}</div>
          <i class="el-icon-ksd-what"></i>
        </el-tooltip>
      </div>
      <div class="ksd-fright btn-group">
        <el-button size="medium" icon="el-icon-ksd-query_add" plain type="primary" @click="openCandidateList">{{$t('addCandidate')}}</el-button>
        <el-button size="medium" icon="el-icon-ksd-query_import" plain>{{$t('import')}}</el-button>
        <el-button size="medium" icon="el-icon-ksd-table_delete" plain @click="removeFav">{{$t('remove')}}</el-button>
      </div>
    </div>
    <el-table
      :data="favQueList.favorite_queries"
      border
      class="favorite-table"
      @selection-change="handleSelectionChange"
      ref="favoriteTable"
      style="width: 100%">
      <el-table-column type="selection" width="55" align="center"></el-table-column>
      <el-table-column :label="$t('kylinLang.query.sqlContent_th')" prop="sql" header-align="center" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('kylinLang.query.lastModefied')" prop="last_modified" sortable header-align="center" width="250">
        <template slot-scope="props">
          {{props.row.last_modified | gmtTime}}
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.rate')" prop="success_rate" sortable align="center" width="200">
        <template slot-scope="props">
          {{props.row.success_rate * 100 | number(2)}}%
        </template>
      </el-table-column>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.frequency')" prop="frequency" sortable align="center" width="200"></el-table-column>
      <el-table-column :label="$t('kylinLang.query.avgDuration')" prop="average_duration" sortable align="center" width="200">
        <template slot-scope="props">
          {{props.row.average_duration / 1000}}s
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn" prop="status" align="center" width="120">
        <template slot-scope="props">
          <i class="status-icon" :class="{
            'el-icon-ksd-acclerate': props.row.status === 'FULLY_ACCELERATED',
            'el-icon-ksd-acclerate_portion': props.row.status === 'PARTLY_ACCELERATED',
            'el-icon-ksd-acclerate_ready': props.row.status === 'WAITING',
            'el-icon-ksd-acclerate_ongoing': props.row.status === 'ACCELERATING'
          }"></i>
        </template>
      </el-table-column>
    </el-table>
    <kap-pager ref="favoriteQueryPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="favQueList.size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
    <el-dialog
      :title="$t('candidateQuery')"
      :visible.sync="candidateVisible"
      width="80%"
      top="5vh"
      class="candidateDialog">
      <query_history_table :queryHistoryData="queryHistoryData.candidates" :isCandidate="true" v-on:selectionChanged="selectionChanged" v-on:markToFav="markToFav" v-on:loadFilterList="loadFilterList"></query_history_table>
      <kap-pager ref="filterHistoryPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="queryHistoryData.size"  v-on:handleCurrentChange='historyCurrentChange'></kap-pager>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import $ from 'jquery'
import { handleSuccessAsync, handleError } from '../../util/index'
import { handleSuccess } from '../../util/business'
import queryHistoryTable from './query_history_table'
@Component({
  methods: {
    ...mapActions({
      getFavoriteList: 'GET_FAVORITE_LIST',
      getCandidateList: 'GET_CANDIDATE_LIST',
      deleteFav: 'DELETE_FAV',
      markFav: 'MARK_FAV'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    'query_history_table': queryHistoryTable
  },
  locales: {
    'en': {addCandidate: 'Add Candidate Query', import: 'Input', remove: 'Remove', candidateQuery: 'Candidate Query', favDesc: 'Critical SQL statement for business. System will create aggregate index or table index to serve them and do pre-computing to improve query performance.'},
    'zh-cn': {addCandidate: '添加查询', import: '导入查询文件', remove: '删除查询', candidateQuery: '待选查询', favDesc: '重要SQL语句的列表。系统针对加速查询中的SQL对应生成聚合索引（agggregate index）或明细表索引（table index），通过预计算索引提升SQL查询响应速度。'}
  }
})
export default class FavoriteQuery extends Vue {
  favQueList = {}
  statusFilteArr = [{name: 'el-icon-ksd-acclerate', value: 'FULLY_ACCELERATED'}, {name: 'el-icon-ksd-acclerate_ready', value: 'WAITING'}, {name: 'el-icon-ksd-acclerate_portion', value: 'PARTLY_ACCELERATED'}, {name: 'el-icon-ksd-acclerate_ongoing', value: 'ACCELERATING'}]
  checkedStatus = []
  candidateVisible = false
  favoriteCurrentPage = 1
  candidateCurrentPage = 1
  selectToUnFav = {}
  selectToFav = {}
  filterData = {
    startTimeFrom: null,
    startTimeTo: null,
    latencyFrom: null,
    latencyTo: null,
    realization: [],
    accelerateStatus: [],
    sql: null
  }
  queryHistoryData = {}

  async loadFavoriteList (pageIndex, pageSize) {
    const res = await this.getFavoriteList({
      project: this.currentSelectedProject || null,
      limit: pageSize || 10,
      offset: pageIndex || 0,
      accelerateStatus: this.checkedStatus
    })
    const data = await handleSuccessAsync(res)
    this.favQueList = data
    if (this.selectToUnFav[this.favoriteCurrentPage]) {
      this.$nextTick(() => {
        this.$refs.favoriteTable.toggleRowSelection(this.selectToUnFav[this.favoriteCurrentPage])
      })
    }
  }

  filterFav () {
    this.loadFavoriteList()
  }

  loadFilterList (data) {
    this.filterData = data
    this.loadCandidateList()
  }

  async loadCandidateList (pageIndex, pageSize) {
    const resData = {
      project: this.currentSelectedProject || null,
      limit: pageSize || 10,
      offset: pageIndex || 0,
      startTimeFrom: this.filterData.startTimeFrom,
      startTimeTo: this.filterData.startTimeTo,
      latencyFrom: this.filterData.latencyFrom,
      latencyTo: this.filterData.latencyTo,
      'realization[]': this.filterData.realization.join(','),
      'accelerateStatus[]': this.filterData.accelerateStatus.join(','),
      sql: this.filterData.sql
    }
    const res = await this.getCandidateList(resData)
    const data = await handleSuccessAsync(res)
    this.queryHistoryData = data
  }

  created () {
    this.loadFavoriteList()
  }

  openCandidateList () {
    this.candidateVisible = true
    this.loadCandidateList()
  }

  pageCurrentChange (offset, pageSize) {
    this.favoriteCurrentPage = offset + 1
    this.loadFavoriteList(offset, pageSize)
  }

  historyCurrentChange (offset, pageSize) {
    this.candidateCurrentPage = offset + 1
    this.loadCandidateList(offset, pageSize)
  }

  handleSelectionChange (rows) {
    this.selectToUnFav[this.favoriteCurrentPage] = rows
  }

  selectionChanged (rows) {
    this.selectToFav[this.candidateCurrentPage] = rows
  }

  removeFav () {
    let uuidArr = []
    $.each(this.selectToUnFav, (index, item) => {
      const uuids = item.map((t) => {
        return t.uuid
      })
      uuidArr = uuidArr.concat(uuids)
    })
    this.deleteFav({project: this.currentSelectedProject, 'uuids': uuidArr}).then((res) => {
      handleSuccess(res, () => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
        this.selectToUnFav = {}
        this.loadFavoriteList()
      })
    }, (res) => {
      handleError(res)
    })
  }

  markToFav () {
    let uuidArr = []
    $.each(this.selectToFav, (index, item) => {
      const uuids = item.map((t) => {
        return t.uuid
      })
      uuidArr = uuidArr.concat(uuids)
    })
    this.markFav({project: this.currentSelectedProject, uuids: uuidArr}).then((res) => {
      handleSuccess(res, () => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.markSuccess')
        })
        this.selectToFav = {}
        this.loadFavoriteList()
        this.loadCandidateList()
      })
    }, (res) => {
      handleError(res)
    })
  }

  renderColumn (h) {
    let items = []
    for (let i = 0; i < this.statusFilteArr.length; i++) {
      items.push(<el-checkbox label={this.statusFilteArr[i].value} key={this.statusFilteArr[i].value}><i class={this.statusFilteArr[i].name}></i></el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.query.acceleration_th')}</span>
      <el-popover
        ref="ipFilterPopover"
        placement="bottom"
        popperClass="filter-popover">
        <el-checkbox-group class="filter-groups" value={this.checkedStatus} onInput={val => (this.checkedStatus = val)} onChange={this.filterFav}>
          {items}
        </el-checkbox-group>
        <i class="el-icon-ksd-filter" slot="reference"></i>
      </el-popover>
    </span>)
  }
}
</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #favoriteQuery {
    padding: 0 20px 50px 20px;
    .table-title {
      color: @text-title-color;
      font-size: 16px;
      line-height: 32px;
    }
    .candidateDialog .el-dialog > .el-dialog__body {
      height: 78vh;
      box-sizing:border-box;
      overflow-y: scroll;
    }
    .favorite-table {
      .status-icon {
        font-size: 20px;
        &.el-icon-ksd-acclerate {
          color: @normal-color-1;
        }
        &.el-icon-ksd-acclerate_portion,
        &.el-icon-ksd-acclerate_ongoing {
          color: @base-color;
        }
      }
      .el-icon-ksd-filter {
        position: relative;
        top: 2px;
      }
    }
    .fav-dropdown {
      .el-icon-ksd-table_setting {
        color: inherit;
      }
    }
  }
</style>
