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
    <el-table-column :label="$t('kylinLang.query.frequency')" prop="total_count" sortable align="center" width="120" v-if="isAccelerated"></el-table-column>
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
    <el-table-column :label="$t('kylinLang.common.action')" align="center" width="100">
      <template slot-scope="props">
        <span @click="delFav(props.row.uuid, props.row.channel)">
          <i class="el-icon-ksd-table_delete" v-if="props.row.channel==='Imported'"></i>
          <i class="el-icon-ksd-table_discard" v-if="props.row.channel==='Rule-based'"></i>
        </span>
      </template>
    </el-table-column>
  </el-table>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccess, transToGmtTime, kapConfirm } from '../../util/business'
import { handleError } from '../../util/index'
@Component({
  props: ['favoriteTableData', 'isAccelerated'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      removeFavSql: 'REMOVE_FAVORITE_SQL'
    })
  },
  locales: {
    'en': {delSql: 'Are you sure to delete this sql?', addToBlackList: 'Are you sure add this sql to the Black List'},
    'zh-cn': {delSql: '确定删除这条查询语句吗？', addToBlackList: '确定将这条查询语句加入禁用名单吗？'}
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
    if (row.channel === 'Imported') {
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
    this.$emit('filterFav', this.checkedStatus)
  }

  delFav (uuid, channel) {
    let confirmMsg = ''
    if (channel === 'Imported') {
      confirmMsg = this.$t('delSql')
    } else if (channel === 'Rule-based') {
      confirmMsg = this.$t('addToBlackList')
    }
    kapConfirm(confirmMsg).then(() => {
      this.removeFavSql({project: this.currentSelectedProject, uuid: uuid}).then((res) => {
        handleSuccess(res, (data) => {
          this.filterFav()
        })
      }, (res) => {
        handleError(res)
      })
    })
  }
}
</script>
