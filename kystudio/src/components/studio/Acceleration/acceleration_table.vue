<template>
  <div>
    <div class="btn-groups ky-no-br-space ksd-mb-10">
      <el-button size="small" type="primary" icon="el-icon-ksd-table_delete" plain :disabled="!checkedList.length" :loading="dropLoading" @click="delFav(false)">{{$t('kylinLang.common.drop')}}</el-button>
      <el-button size="small" type="primary" icon="el-icon-ksd-table_discard" plain :disabled="!checkedList.length" @click="delFav(true)">{{$t('kylinLang.common.disable')}}</el-button>
    </div>
    <el-table
      :data="favoriteTableData"
      border
      class="favorite-table"
      ref="favoriteTable"
      :default-sort = "{prop: 'last_query_time', order: 'descending'}"
      @sort-change="sortFavoriteList"
      @selection-change="handleSelectionChange"
      :row-class-name="tableRowClassName"
      style="width: 100%">
      <el-table-column type="selection" align="center" width="44"></el-table-column>
      <el-table-column :label="$t('kylinLang.query.sqlContent_th')" prop="sql_pattern" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('kylinLang.query.lastModefied')" prop="last_query_time" sortable width="218">
        <template slot-scope="props">
          {{transToGmtTime(props.row.last_query_time)}}
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.original')" prop="channel" width="98">
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.frequency')" align="right" prop="total_count" sortable width="115" v-if="tab =='accelerated'"></el-table-column>
      <el-table-column :label="$t('kylinLang.query.avgDuration')" align="right" prop="average_duration" sortable width="155">
        <template slot-scope="props">
          <span v-if="props.row.average_duration < 1000"> &lt; 1s</span>
          <span v-else>{{props.row.average_duration / 1000 | fixed(2)}}s</span>
        </template>
      </el-table-column>
      <el-table-column :renderHeader="renderColumn" prop="status" width="165">
        <template slot-scope="props">
          <div v-if="props.row.status === 'ACCELERATED'">
            <i class="status-icon el-icon-ksd-acclerated"></i>
            <span>{{$t('kylinLang.query.fullyAcce')}}</span>
          </div>
          <div v-if="props.row.status === 'ACCELERATING'">
            <i class="status-icon el-icon-ksd-acclerating"></i>
            <span>{{$t('kylinLang.query.ongoingAcce')}}</span>
          </div>
          <div v-if="props.row.status === 'TO_BE_ACCELERATED'">
            <i class="status-icon el-icon-ksd-to_accelerated"></i>
            <span>{{$t('kylinLang.query.wartingAcce')}}</span>
          </div>
          <div v-if="props.row.status === 'PENDING'">
            <i class="status-icon el-icon-ksd-negative"></i>
            <span>{{$t('kylinLang.query.pending')}}</span>
          </div>
          <el-tooltip class="item" effect="dark" :content="props.row.comment" placement="top-end" v-if="props.row.status === 'FAILED'">
            <i class="status-icon el-icon-ksd-negative"></i>
            <span>{{$t('kylinLang.query.failed')}}</span>
          </el-tooltip>
        </template>
      </el-table-column>
      <!-- <el-table-column :label="$t('kylinLang.common.action')" width="83">
        <template slot-scope="props">
          <span @click="delFav(props.row.uuid, props.row.channel)">
            <common-tip :content="$t('kylinLang.common.drop')">
              <i class="el-icon-ksd-table_delete" v-if="props.row.channel==='Imported'"></i>
            </common-tip>
            <common-tip :content="$t('kylinLang.common.disable')">
              <i class="el-icon-ksd-table_discard" v-if="props.row.channel==='Rule-based'"></i>
            </common-tip>
          </span>
        </template>
      </el-table-column> -->
    </el-table>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccess, transToGmtTime, kapConfirm } from '../../../util/business'
import { handleError } from '../../../util/index'
@Component({
  props: ['favoriteTableData', 'tab'],
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
  statusFilteArr1 = [
    {name: 'el-icon-ksd-to_accelerated', value: 'TO_BE_ACCELERATED', label: 'wartingAcce'},
    {name: 'el-icon-ksd-acclerating', value: 'ACCELERATING', label: 'ongoingAcce'}
  ]
  statusFilteArr2 = [
    {name: 'el-icon-ksd-negative', value: 'PENDING', label: 'pending'},
    {name: 'el-icon-ksd-negative', value: 'FAILED', label: 'failed'}
  ]
  checkedStatus = []
  filterData = {
    sortBy: 'last_query_time',
    reverse: false
  }
  checkedList = []
  dropLoading = false

  renderColumn (h) {
    if (this.tab !== 'accelerated') {
      let items = []
      const statusArr = this.tab === 'waiting' ? this.statusFilteArr1 : this.statusFilteArr2
      for (let i = 0; i < statusArr.length; i++) {
        items.push(<el-checkbox label={statusArr[i].value} key={statusArr[i].value}><i class={statusArr[i].name}></i> <span class="ksd-fs-12" style="position: relative; top:-1.5px;">{this.$t('kylinLang.query.' + statusArr[i].label)}</span></el-checkbox>)
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
          <i class={this.checkedStatus.length > 0 && this.checkedStatus.length < 3 ? 'el-icon-ksd-filter isFilter' : 'el-icon-ksd-filter'} slot="reference"></i>
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

  handleSelectionChange (val) {
    this.checkedList = val.map((i) => {
      return i.uuid
    })
  }

  delFav (isBlock) {
    let confirmMsg = ''
    if (isBlock) {
      confirmMsg = this.$t('addToBlackList')
    } else {
      confirmMsg = this.$t('delSql')
    }
    kapConfirm(confirmMsg).then(() => {
      this.removeFavSql({project: this.currentSelectedProject, uuids: this.checkedList, block: isBlock}).then((res) => {
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
