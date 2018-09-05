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
      <el-table-column label="SQL" prop="query" header-align="center"></el-table-column>
      <el-table-column :label="$t('kylinLang.query.lastModefied')" prop="lastModefied" sortable header-align="center" width="250">
        <template slot-scope="props">
          {{props.row.lastModefied | gmtTime}}
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.rate')" prop="rate" sortable align="center" width="200">
        <template slot-scope="props">
          {{props.row.rate * 100 | number(2)}}%
        </template>
      </el-table-column>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.frequency')" prop="frequency" sortable align="center" width="200"></el-table-column>
      <el-table-column :label="$t('kylinLang.query.avgDuration')" prop="duration" sortable align="center" width="200">
        <template slot-scope="props">
          {{props.row.duration}}s
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn" prop="status" align="center" width="100">
        <template slot-scope="props">
          <i class="status-icon" :class="{
            'el-icon-ksd-acclerate': props.row.status === 'speed',
            'el-icon-ksd-acclerate_portion': props.row.status === 'partSpeed',
            'el-icon-ksd-acclerate_ready': props.row.status === 'unSpeed',
            'el-icon-ksd-acclerate_ongoing': props.row.status === 'speeding'
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
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions } from 'vuex'
import queryHistoryTable from '../common/query_history_table'
@Component({
  methods: {
    ...mapActions({})
  },
  components: {
    'query_history_table': queryHistoryTable
  }
})
export default class FavoriteQuery extends Vue {
  favQueList = [
    {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'unSpeed'},
    {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
    {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speeding'},
    {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'partSpeed'},
    {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
    {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
    {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
    {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
    {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
    {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'},
    {query: 'Select count (*) from table_1', lastModefied: 1524829437628, rate: 0.94376, frequency: 55, duration: 5.98, status: 'speed'}
  ]
  statusFilteArr = [{speed: 'el-icon-ksd-acclerate'}, {unSpeed: 'el-icon-ksd-acclerate_ready'}, {partSpeed: 'el-icon-ksd-acclerate_portion'}, {speeding: 'el-icon-ksd-acclerate_ongoing'}]
  checkedStatus = []
  candidateVisible = false
  queryHistoryData = [
    {queryId: 1248332349, startTime: 1524829437628, duration: '2.2s', resultRowCount: '100,000', ifHitCache: false, modelName: 'Model_auto1', realization: 'Aggreaget data ID / Raw data ID', content: 'columnA, columnB', totalScanCount: '100,000', totalScanBytes: '100,000,000', queryContent: 'Select count (*) from table_1', user: 'Admin', queryTarget: 'Pushdowm to Hive', ip: '101.1.1.181', status: 'speed'},
    {queryId: 1248332349, startTime: 1524829437628, duration: '2.2s', resultRowCount: '100,000', ifHitCache: false, modelName: 'Model_auto1', realization: 'Aggreaget data ID / Raw data ID', content: 'columnA, columnB', totalScanCount: '100,000', totalScanBytes: '100,000,000', queryContent: 'Select count (*) from table_1', user: 'Admin', queryTarget: 'Pushdowm to Hive', ip: '101.1.1.181', status: 'unSpeed'},
    {queryId: 1248332349, startTime: 1524829437628, duration: '2.2s', resultRowCount: '100,000', ifHitCache: false, modelName: 'Model_auto1', realization: 'Aggreaget data ID / Raw data ID', content: 'columnA, columnB', totalScanCount: '100,000', totalScanBytes: '100,000,000', queryContent: 'Select count (*) from table_1', user: 'Admin', queryTarget: 'Pushdowm to Hive', ip: '101.1.1.181', status: 'partSpeed'},
    {queryId: 1248332349, startTime: 1524829437628, duration: '2.2s', resultRowCount: '100,000', ifHitCache: false, modelName: 'Model_auto1', realization: 'Aggreaget data ID / Raw data ID', content: 'columnA, columnB', totalScanCount: '100,000', totalScanBytes: '100,000,000', queryContent: 'Select count (*) from table_1', user: 'Admin', queryTarget: 'Pushdowm to Hive', ip: '101.1.1.181', status: 'speeding'},
    {queryId: 1248332349, startTime: 1524829437628, duration: '2.2s', resultRowCount: '100,000', ifHitCache: false, modelName: 'Model_auto1', realization: 'Aggreaget data ID / Raw data ID', content: 'columnA, columnB', totalScanCount: '100,000', totalScanBytes: '100,000,000', queryContent: 'Select count (*) from table_1', user: 'Admin', queryTarget: 'Pushdowm to Hive', ip: '101.1.1.181', status: 'unSpeed'},
    {queryId: 1248332349, startTime: 1524829437628, duration: '2.2s', resultRowCount: '100,000', ifHitCache: false, modelName: 'Model_auto1', realization: 'Aggreaget data ID / Raw data ID', content: 'columnA, columnB', totalScanCount: '100,000', totalScanBytes: '100,000,000', queryContent: 'Select count (*) from table_1', user: 'Admin', queryTarget: 'Pushdowm to Hive', ip: '101.1.1.181', status: 'unSpeed'},
    {queryId: 1248332349, startTime: 1524829437628, duration: '2.2s', resultRowCount: '100,000', ifHitCache: false, modelName: 'Model_auto1', realization: 'Aggreaget data ID / Raw data ID', content: 'columnA, columnB', totalScanCount: '100,000', totalScanBytes: '100,000,000', queryContent: 'Select count (*) from table_1', user: 'Admin', queryTarget: 'Pushdowm to Hive', ip: '101.1.1.181', status: 'unSpeed'},
    {queryId: 1248332349, startTime: 1524829437628, duration: '2.2s', resultRowCount: '100,000', ifHitCache: false, modelName: 'Model_auto1', realization: 'Aggreaget data ID / Raw data ID', content: 'columnA, columnB', totalScanCount: '100,000', totalScanBytes: '100,000,000', queryContent: 'Select count (*) from table_1', user: 'Admin', queryTarget: 'Pushdowm to Hive', ip: '101.1.1.181', status: 'unSpeed'},
    {queryId: 1248332349, startTime: 1524829437628, duration: '2.2s', resultRowCount: '100,000', ifHitCache: false, modelName: 'Model_auto1', realization: 'Aggreaget data ID / Raw data ID', content: 'columnA, columnB', totalScanCount: '100,000', totalScanBytes: '100,000,000', queryContent: 'Select count (*) from table_1', user: 'Admin', queryTarget: 'Pushdowm to Hive', ip: '101.1.1.181', status: 'unSpeed'},
    {queryId: 1248332349, startTime: 1524829437628, duration: '2.2s', resultRowCount: '100,000', ifHitCache: false, modelName: 'Model_auto1', realization: 'Aggreaget data ID / Raw data ID', content: 'columnA, columnB', totalScanCount: '100,000', totalScanBytes: '100,000,000', queryContent: 'Select count (*) from table_1', user: 'Admin', queryTarget: 'Pushdowm to Hive', ip: '101.1.1.181', status: 'unSpeed'}
  ]

  pageCurrentChange () {}
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
