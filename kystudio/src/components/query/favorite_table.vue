<template>
  <el-table
    :data="favoriteTableData"
    border
    class="favorite-table"
    ref="favoriteTable"
    @sort-change="sortFavoriteList"
    :row-class-name="tableRowClassName"
    style="width: 100%">
    <el-table-column :label="$t('kylinLang.query.sqlContent_th')" prop="sql_pattern" header-align="center" show-overflow-tooltip></el-table-column>
    <el-table-column :label="$t('kylinLang.query.lastModefied')" prop="last_query_time" sortable header-align="center" width="210">
      <template slot-scope="props">
        {{transToGmtTime(props.row.last_query_time)}}
      </template>
    </el-table-column>
    <el-table-column :label="$t('kylinLang.query.type')" prop="channel" align="center" width="135">
    </el-table-column>
    <el-table-column :label="$t('kylinLang.query.rate')" prop="success_rate" sortable align="center" width="135" v-if="isAccelerated">
      <template slot-scope="props">
        {{props.row.success_rate * 100 | number(2)}}%
      </template>
    </el-table-column>
    <el-table-column :label="$t('kylinLang.query.frequency')" prop="frequency" sortable align="center" width="120" v-if="isAccelerated"></el-table-column>
    <el-table-column :label="$t('kylinLang.query.avgDuration')" prop="average_duration" sortable align="center" width="160" v-if="isAccelerated">
      <template slot-scope="props">
        <span v-if="props.row.average_duration < 1000"> &lt; 1s</span>
        <span v-else>{{props.row.average_duration / 1000 | fixed(2)}}s</span>
      </template>
    </el-table-column>
    <el-table-column :renderHeader="renderColumn" prop="status" header-align="center" width="155">
      <template slot-scope="props">
        <div v-if="props.row.status === 'FULLY_ACCELERATED'">
          <i class="status-icon el-icon-ksd-acclerate_all"></i>
          <span>{{$t('kylinLang.query.fullyAcce')}}</span>
        </div>
        <div v-if="props.row.status === 'PARTLY_ACCELERATED'">
          <i class="status-icon el-icon-ksd-acclerate_portion"></i>
          <span>{{$t('kylinLang.query.partlyAcce')}}</span>
        </div>
        <div v-if="props.row.status === 'ACCELERATING'">
          <i class="status-icon el-icon-ksd-acclerate_ongoing"></i>
          <span>{{$t('kylinLang.query.ongoingAcce')}}</span>
        </div>
        <div v-if="props.row.status === 'WAITING'">
          <i class="status-icon el-icon-ksd-acclerate_pendding"></i>
          <span>{{$t('kylinLang.query.wartingAcce')}}</span>
        </div>
        <el-tooltip class="item" effect="dark" :content="props.row.comment" placement="top-end" v-if="props.row.status === 'BLOCKED'">
          <i class="status-icon el-icon-ksd-table_discard"></i>
        </el-tooltip>
      </template>
    </el-table-column>
  </el-table>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { transToGmtTime } from '../../util/business'
@Component({
  props: ['favoriteTableData', 'isAccelerated'],
  methods: {
    transToGmtTime: transToGmtTime
  }
})
export default class FavoriteTable extends Vue {
  statusFilteArr = [
    // {name: 'el-icon-ksd-acclerate_all', value: 'FULLY_ACCELERATED'},
    {name: 'el-icon-ksd-acclerate_pendding', value: 'WAITING'},
    // {name: 'el-icon-ksd-acclerate_portion', value: 'PARTLY_ACCELERATED'},
    {name: 'el-icon-ksd-acclerate_ongoing', value: 'ACCELERATING'},
    {name: 'el-icon-ksd-table_discard', value: 'BLOCKED'}
  ]
  checkedStatus = []
  filterData = {
    sortBy: 'last_query_time',
    reverse: false
  }

  renderColumn (h) {
    if (!this.isAccelerated) {
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
    } else {
      return (<span>{this.$t('kylinLang.query.acceleration_th')}</span>)
    }
  }

  tableRowClassName ({row, rowIndex}) {
    if (row.channel === 'whitelist-based') {
      return 'impored-row'
    }
    return ''
  }

  sortFavoriteList ({ column, prop, order }) {
    if (order === 'ascending') {
      this.filterData.reverse = false
    } else {
      this.filterData.reverse = true
    }
    this.filterData.sortBy = prop
    this.$emit('sortTable', this.filterData)
  }

  filterFav () {
    this.$emit('loadFavoriteList', this.checkedStatus)
  }
}
</script>
