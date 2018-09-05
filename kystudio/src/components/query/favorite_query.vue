<template>
  <div id="favoriteQuery">
    <div class="clearfix ksd-mb-10">
      <div class="ksd-fleft table-title">
        <span>Favorite Query <i class="el-icon-ksd-what"></i></span>
      </div>
      <div class="ksd-fright btn-group">
        <el-button size="medium" icon="el-icon-ksd-query_add" plain type="primary" @click="candidateVisible = true">{{$t('kylinLang.common.add')}}</el-button>
        <el-button size="medium" icon="el-icon-ksd-query_import" plain>{{$t('kylinLang.common.import')}}</el-button>
        <el-button size="medium" icon="el-icon-ksd-table_delete" plain>{{$t('kylinLang.common.remove')}}</el-button>
      </div>
    </div>
    <el-table
      :data="favQueList"
      border
      class="favorite-table"
      style="width: 100%">
      <el-table-column type="selection" width="55" align="center"></el-table-column>
      <el-table-column label="SQL" prop="sqlPattern" header-align="center" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('kylinLang.query.lastModefied')" prop="last_executing_time" sortable header-align="center" width="250">
        <template slot-scope="props">
          {{props.row.last_executing_time | gmtTime}}
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
          {{props.row.average_duration}}s
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn" prop="status" align="center" width="100">
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
    <pager ref="FavoriteQueryPager" class="ksd-center" :totalSize="favQueList.length"  v-on:handleCurrentChange='pageCurrentChange' ></pager>
    <el-dialog
      title="Candidate Query"
      :visible.sync="candidateVisible"
      width="80%">
      <query_history_table :queryHistoryData="queryHistoryData" :isCandidate="true"></query_history_table>
      <pager ref="queryHistoryPager" class="ksd-center" :totalSize="queryHistoryData.length"  v-on:handleCurrentChange='historyCurrentChange' ></pager>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import { handleSuccessAsync } from '../../util/index'
import queryHistoryTable from './query_history_table'
@Component({
  methods: {
    ...mapActions({
      getFavoriteList: 'GET_FAVORITE_LIST'
    })
  },
  components: {
    'query_history_table': queryHistoryTable
  }
})
export default class FavoriteQuery extends Vue {
  // favQueList = [
  //   {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'unSpeed'},
  //   {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
  //   {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speeding'},
  //   {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'partSpeed'},
  //   {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
  //   {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
  //   {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
  //   {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
  //   {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
  //   {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
  //   {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'}
  // ]
  favQueList = [
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sqlPattern: 'parttern1', last_executing_time: 543535, success_rate: 0.89, frequency: 1, average_duration: 2.1, model_name: 'models1', status: 'WAITING', success_query_count: 10},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sqlPattern: 'parttern1', last_executing_time: 543535, success_rate: 0.89, frequency: 1, average_duration: 2.1, model_name: 'models1', status: 'ACCELERATING', success_query_count: 10},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sqlPattern: 'parttern1', last_executing_time: 543535, success_rate: 0.89, frequency: 1, average_duration: 2.1, model_name: 'models1', status: 'PARTLY_ACCELERATED', success_query_count: 10},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sqlPattern: 'parttern1', last_executing_time: 543535, success_rate: 0.89, frequency: 1, average_duration: 2.1, model_name: 'models1', status: 'FULLY_ACCELERATED', success_query_count: 10},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sqlPattern: 'parttern1', last_executing_time: 543535, success_rate: 0.89, frequency: 1, average_duration: 2.1, model_name: 'models1', status: 'WAITING', success_query_count: 10},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sqlPattern: 'parttern1', last_executing_time: 543535, success_rate: 0.89, frequency: 1, average_duration: 2.1, model_name: 'models1', status: 'WAITING', success_query_count: 10},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sqlPattern: 'parttern1', last_executing_time: 543535, success_rate: 0.89, frequency: 1, average_duration: 2.1, model_name: 'models1', status: 'WAITING', success_query_count: 10},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sqlPattern: 'parttern1', last_executing_time: 543535, success_rate: 0.89, frequency: 1, average_duration: 2.1, model_name: 'models1', status: 'WAITING', success_query_count: 10},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sqlPattern: 'parttern1', last_executing_time: 543535, success_rate: 0.89, frequency: 1, average_duration: 2.1, model_name: 'models1', status: 'WAITING', success_query_count: 10},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sqlPattern: 'parttern1', last_executing_time: 543535, success_rate: 0.89, frequency: 1, average_duration: 2.1, model_name: 'models1', status: 'WAITING', success_query_count: 10},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sqlPattern: 'parttern1', last_executing_time: 543535, success_rate: 0.89, frequency: 1, average_duration: 2.1, model_name: 'models1', status: 'WAITING', success_query_count: 10}
  ]
  statusFilteArr = [{speed: 'el-icon-ksd-acclerate'}, {unSpeed: 'el-icon-ksd-acclerate_ready'}, {partSpeed: 'el-icon-ksd-acclerate_portion'}, {speeding: 'el-icon-ksd-acclerate_ongoing'}]
  checkedStatus = []
  queryCurrentPage = 1
  historyCurrentPage = 1
  candidateVisible = false
  queryHistoryData = [
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'ACCELERATING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'PARTLY_ACCELERATED', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'FULLY_ACCELERATED', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false},
    {uuid: 'fdsf23534', version: 'version1', id: 1, project: 'kylin', sql: 'select * from', startTime: 543535, latency: 0.9, realization: 'realization1', queryNode: 'node1', thread: 'thread1', user: 'ADMIN', history_queries_status_enum: 'NEW', favorite: 'favorite1', accelerate_status: 'WAITING', queryId: 'FFDS6-R5345', model_name: 'model1', content: ['select1', 'select2'], total_scan_count: 435, total_scan_bytes: 65464, result_row_count: 43, is_cubeHit: false}
  ]

  async loadFavoriteList (pageIndex) {
    const res = await this.getFavoriteList({
      pageData: {
        project: this.project || null,
        limit: this.listRows,
        offset: pageIndex || 0
      }
    })
    this.favQueList = await handleSuccessAsync(res)
  }

  created () {
    this.loadFavoriteList()
  }

  pageCurrentChange (currentPage) {
    this.queryCurrentPage = currentPage
    this.loadHistoryList(currentPage - 1)
  }

  historyCurrentChange () {}
  renderColumn (h) {
    let items = []
    for (let i = 0; i < this.statusFilteArr.length; i++) {
      const keyName = Object.keys(this.statusFilteArr[i])[0]
      const labelClass = this.statusFilteArr[i][keyName]
      items.push(<el-checkbox key={keyName}><slot><i class={labelClass}></i></slot></el-checkbox>)
    }
    return (<span>
      <span>{this.$t('kylinLang.common.status')}</span>
      <el-popover
        ref="ipFilterPopover"
        placement="bottom"
        popperClass="filter-popover">
        <el-checkbox-group class="filter-groups" value={this.checkedStatus} onInput={val => (this.checkedStatus = val)}>
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
  .fav-dropdown-item {
      i {
        margin-left: 5px;
      }
      &:hover {
        i {
          color: @text-normal-color;
          &:hover {
            color: @base-color;
          }
        }
      }
    }
</style>
