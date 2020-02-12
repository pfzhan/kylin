<template>
  <div>
    <div class="batch-btn-groups ky-no-br-space ksd-mb-10">
      <el-button @click="isShowBatch = !isShowBatch">{{batchBtn}}
        <i :class="{'el-icon-arrow-right': !isShowBatch, 'el-icon-arrow-left': isShowBatch}"></i>
      </el-button>
      <transition name="slide">
        <el-button-group class="ksd-btn-groups" v-if="isShowBatch">
          <el-button icon="el-icon-ksd-table_delete" :disabled="!checkedList.length" :loading="dropLoading" @click="delFav(false)">{{$t('kylinLang.common.delete')}}</el-button>
          <el-button icon="el-icon-ksd-table_discard" :disabled="!checkedList.length" @click="delFav(true)">{{$t('kylinLang.common.disable')}}</el-button>
        </el-button-group>
      </transition>
    </div>
    <div class="filter-tags" v-show="filterTags.length">
      <div class="filter-tags-layout"><el-tag size="small" closable v-for="(item, index) in filterTags" :key="index" @close="handleClose(item)">{{$t(item.source) + '：' + $t('kylinLang.query.' + item.name)}}</el-tag></div>
      <span class="clear-all-filters" @click="clearAllTags">{{$t('clearAll')}}</span>
    </div>
    <el-table
      :data="favoriteTableData"
      border
      v-loading="loading"
      class="favorite-table"
      ref="favoriteTable"
      :default-sort = "{prop: 'last_query_time', order: 'descending'}"
      @sort-change="sortFavoriteList"
      @selection-change="handleSelectionChange"
      :row-class-name="tableRowClassName"
      style="width: 100%">
      <el-table-column type="selection" align="center" width="50" v-if="isShowBatch" key="checkbox"></el-table-column>
      <el-table-column type="index" :label="$t('order')" width="50" align="center" v-if="!isShowBatch" key="index"></el-table-column>
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
      <el-table-column v-bind="renderFilter" prop="status" width="165">
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
import { Component, Watch } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccess, transToGmtTime } from '../../../util/business'
import { handleError, indexOfObjWithSomeKey, objectClone } from '../../../util/index'
@Component({
  props: ['favoriteTableData', 'tab', 'loading'],
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
      closeBatchBtn: 'Close Batch Actions',
      order: 'No.',
      clearAll: 'Clear All'
    },
    'zh-cn': {
      delSql: '您确认要删除 {numbers} 条查询语句吗？',
      addToBlackList: '确定将这 {numbers} 条查询语句加入禁用名单吗？',
      openBatchBtn: '开启批量操作',
      closeBatchBtn: '关闭批量操作',
      order: '序号',
      clearAll: '清除所有'
    }
  }
})
export default class FavoriteTable extends Vue {
  statusFilteArr1 = [
    {icon: 'el-icon-ksd-to_accelerated', value: 'TO_BE_ACCELERATED', label: 'wartingAcce'},
    {icon: 'el-icon-ksd-acclerating', value: 'ACCELERATING', label: 'ongoingAcce'}
  ]
  statusFilteArr2 = [
    {icon: 'el-icon-ksd-negative', value: 'PENDING', label: 'pending'},
    {icon: 'el-icon-ksd-negative', value: 'FAILED', label: 'failed'}
  ]
  checkedStatus = []
  filterData = {
    sortBy: 'last_query_time',
    reverse: false
  }
  checkedList = []
  dropLoading = false
  isShowBatch = false
  filterTags = []

  @Watch('favoriteTableData')
  onDataChange (val) {
    if (this.checkedList.length) {
      const cloneSelections = objectClone(this.checkedList)
      this.checkedList = []
      cloneSelections.forEach((m) => {
        const index = indexOfObjWithSomeKey(val, 'uuid', m.uuid)
        if (index !== -1) {
          this.$nextTick(() => {
            this.$refs.favoriteTable.toggleRowSelection(val[index])
          })
        }
      })
    }
  }

  @Watch('tab')
  onChangeTab (val, oldVal) {
    if (val !== oldVal) {
      this.checkedList = []
    }
  }

  get batchBtn () {
    if (this.isShowBatch) {
      return this.$t('closeBatchBtn')
    } else {
      return this.$t('openBatchBtn')
    }
  }

  get renderFilter () {
    if (this.tab !== 'accelerated') {
      return {
        'filters': this.tab === 'waiting' ? this.statusFilteArr1.map(item => ({...item, text: this.$t(`kylinLang.query.${item.label}`)})) : this.statusFilteArr2.map(item => ({...item, text: this.$t(`kylinLang.query.${item.label}`)})),
        'label': this.$t('kylinLang.query.acceleration_th'),
        'filtered-value': this.checkedStatus,
        'filter-icon': 'el-icon-ksd-filter',
        'show-multiple-footer': false,
        'filter-change': (v) => this.filterContent(v, 'checkedStatus')
      }
    } else {
      return {'label': this.$t('kylinLang.query.acceleration_th')}
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
    this.checkedList = []
    this.$emit('sortTable', this.filterData)
  }

  filterFav () {
    this.checkedList = []
    this.$emit('filterFav', this.checkedStatus)
  }

  handleSelectionChange (val) {
    if (val && val.length) {
      this.$emit('pausePolling')
      this.checkedList = val
    } else {
      this.checkedList = []
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
    const sqls = this.checkedList.map((i) => {
      return i.sql_pattern
    })
    await this.callGlobalDetailDialog({
      msg: confirmMsg,
      title: this.$t('kylinLang.common.notice'),
      details: sqls,
      dialogType: 'warning',
      theme: 'sql'
    })
    const uuids = this.checkedList.map((i) => {
      return i.uuid
    })
    this.removeFavSql({project: this.currentSelectedProject, uuids: uuids, block: isBlock}).then((res) => {
      handleSuccess(res, (data) => {
        this.filterFav()
      })
    }, (res) => {
      handleError(res)
    })
  }

  // 查询状态过滤回调函数
  filterContent (val, type) {
    const maps = {
      checkedStatus: 'kylinLang.query.acceleration_th'
    }

    this.filterTags = this.filterTags.filter((item, index) => item.key !== type || item.key === type && val.includes(item.label))
    const list = this.filterTags.filter(it => it.key === type).map(it => it.label)
    val.length && val.forEach(item => {
      if (!list.includes(item)) {
        const name = this.tab === 'waiting' ? this.statusFilteArr1.filter(it => it.value === item)[0].label : this.statusFilteArr2.filter(it => it.value === item)[0].label

        this.filterTags.push({label: item, source: maps[type], key: type, name})
      }
    })
    this[type] = val
    this.filterFav()
  }
  // 删除单个筛选条件
  handleClose (tag) {
    const index = this[tag.key].indexOf(tag.label)
    index > -1 && this[tag.key].splice(index, 1)
    this.filterTags = this.filterTags.filter(item => item.key !== tag.key || item.key === tag.key && tag.label !== item.label)
    this.filterFav()
  }
  // 清除所有筛选条件
  clearAllTags () {
    this.checkedStatus.splice(0, this.checkedStatus.length)
    this.filterTags = []
    this.filterFav()
  }
}
</script>
