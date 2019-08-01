<template>
  <div>
    <div class="batch-btn-groups ky-no-br-space ksd-mb-10">
      <el-button type="primary" plain @click="isShowBatch = !isShowBatch">{{batchBtn}}
        <i :class="{'el-icon-arrow-right': !isShowBatch, 'el-icon-arrow-left': isShowBatch}"></i>
      </el-button>
      <transition name="slide">
        <el-button-group class="ksd-btn-groups" v-if="isShowBatch">
          <el-button icon="el-icon-ksd-table_delete" plain :disabled="!checkedList.length" :loading="dropLoading" @click="delFav(false)">{{$t('kylinLang.common.delete')}}</el-button>
          <el-button icon="el-icon-ksd-table_discard" plain :disabled="!checkedList.length" @click="delFav(true)">{{$t('kylinLang.common.disable')}}</el-button>
        </el-button-group>
      </transition>
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
      <el-table-column type="selection" align="center" width="44" v-if="isShowBatch" key="checkbox"></el-table-column>
      <el-table-column type="index" width="44" align="center" v-if="!isShowBatch" key="index"></el-table-column>
      <el-table-column :label="$t('kylinLang.query.sqlContent_th')" prop="sql_pattern" show-overflow-tooltip></el-table-column>
      <el-table-column :label="$t('kylinLang.query.lastModefied')" prop="last_query_time" sortable="custom" width="218">
        <template slot-scope="props">
          {{transToGmtTime(props.row.last_query_time)}}
        </template>
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.original')" prop="channel" width="98">
      </el-table-column>
      <el-table-column :label="$t('kylinLang.query.frequency')" align="right" prop="total_count" sortable="custom" width="115" v-if="tab =='accelerated'"></el-table-column>
      <el-table-column :label="$t('kylinLang.query.avgDuration')" align="right" prop="average_duration" sortable="custom" width="155">
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
          <div v-if="props.row.status === 'PENDING' && !props.row.comment">
            <i class="status-icon el-icon-ksd-negative"></i>
            <span>{{$t('kylinLang.query.pending')}}</span>
          </div>
          <div v-if="props.row.status === 'FAILED' && !props.row.comment">
            <i class="status-icon el-icon-ksd-negative failed"></i>
            <span>{{$t('kylinLang.query.failed')}}</span>
          </div>
          <el-tooltip class="item" effect="dark" :content="props.row.comment" placement="top-end" v-if="props.row.status === 'PENDING' && props.row.comment">
            <span><i class="status-icon el-icon-ksd-negative"></i> {{$t('kylinLang.query.pending')}}</span>
          </el-tooltip>
          <el-tooltip class="item" effect="dark" :content="props.row.comment" placement="top-end" v-if="props.row.status === 'FAILED' && props.row.comment">
            <span><i class="status-icon el-icon-ksd-negative failed"></i> {{$t('kylinLang.query.failed')}}</span>
          </el-tooltip>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccess, transToGmtTime } from '../../../util/business'
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
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    })
  },
  locales: {
    'en': {
      delSql: 'Do you really need to delete {numbers} sql(s)?',
      addToBlackList: 'Do you really need this {numbers} sql(s) to the Black List',
      openBatchBtn: 'Open Batch Actions',
      closeBatchBtn: 'Close Batch Actions'
    },
    'zh-cn': {
      delSql: '您确认要删除 {numbers} 条查询语句吗？',
      addToBlackList: '确定将这 {numbers} 条查询语句加入禁用名单吗？',
      openBatchBtn: '开启批量操作',
      closeBatchBtn: '关闭批量操作'
    }
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
  checkedSqls = []
  dropLoading = false
  isShowBatch = false

  get batchBtn () {
    if (this.isShowBatch) {
      return this.$t('closeBatchBtn')
    } else {
      return this.$t('openBatchBtn')
    }
  }

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
    if (val && val.length) {
      this.$emit('pausePolling')
      this.checkedList = val.map((i) => {
        return i.uuid
      })
      this.checkedSqls = val.map((i) => {
        return i.sql_pattern
      })
    } else {
      this.checkedList = []
      this.checkedSqls = []
      this.$emit('reCallPolling')
    }
  }

  async delFav (isBlock) {
    let confirmMsg = ''
    if (isBlock) {
      confirmMsg = this.$t('addToBlackList', {numbers: this.checkedList.length})
    } else {
      confirmMsg = this.$t('delSql', {numbers: this.checkedList.length})
    }
    await this.callGlobalDetailDialog({
      msg: confirmMsg,
      title: this.$t('kylinLang.common.notice'),
      details: this.checkedSqls,
      dialogType: 'warning',
      theme: 'sql'
    })
    this.removeFavSql({project: this.currentSelectedProject, uuids: this.checkedList, block: isBlock}).then((res) => {
      handleSuccess(res, (data) => {
        this.filterFav()
      })
    }, (res) => {
      handleError(res)
    })
  }
}
</script>
