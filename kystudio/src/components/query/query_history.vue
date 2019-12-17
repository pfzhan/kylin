<template>
  <div id="queryHistory">
    <query_history_table :queryHistoryData="queryHistoryData.query_histories" :queryNodes="queryNodes" v-on:openIndexDialog="openIndexDialog" v-on:loadFilterList="loadFilterList"></query_history_table>
    <kap-pager ref="queryHistoryPager" class="ksd-center ksd-mtb-10" :curPage="queryCurrentPage" :totalSize="queryHistoryData.size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
    <el-dialog
      :title="$t('kylinLang.model.aggregateGroupIndex')"
      top="5vh"
      limited-area
      :visible.sync="aggDetailVisible"
      class="agg-dialog"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      width="1104px">
      <ModelAggregate
        v-if="aggDetailVisible"
        :model="model"
        :project-name="currentSelectedProject"
        :is-show-aggregate-action="false">
      </ModelAggregate>
    </el-dialog>

    <el-dialog
      :title="$t('kylinLang.model.tableIndex')"
      top="5vh"
      limited-area
      :visible.sync="tabelIndexVisible"
      class="agg-dialog"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      width="1104px">
      <TableIndex
        v-if="tabelIndexVisible"
        :model-desc="model"
        :layout-id="tabelIndexLayoutId"
        :is-hide-edit="true">
      </TableIndex>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync } from '../../util/index'
import queryHistoryTable from './query_history_table'
import ModelAggregate from '../studio/StudioModel/ModelList/ModelAggregate/index.vue'
import TableIndex from '../studio/StudioModel/TableIndex/index.vue'
@Component({
  methods: {
    ...mapActions({
      getHistoryList: 'GET_HISTORY_LIST',
      loadOnlineQueryNodes: 'LOAD_ONLINE_QUERY_NODES'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    'query_history_table': queryHistoryTable,
    ModelAggregate,
    TableIndex
  }
})
export default class QueryHistory extends Vue {
  aggDetailVisible = false
  tabelIndexVisible = false
  tabelIndexLayoutId = ''
  queryCurrentPage = 1
  queryHistoryData = {}
  filterData = {
    startTimeFrom: null,
    startTimeTo: null,
    latencyFrom: null,
    latencyTo: null,
    realization: [],
    accelerateStatus: [],
    sql: '',
    query_status: []
  }
  model = {
    uuid: ''
  }
  queryNodes = []
  pageSize = 10
  async openIndexDialog (modelId, layoutId) {
    this.model.uuid = modelId
    if (layoutId) {
      this.tabelIndexLayoutId = layoutId
      this.tabelIndexVisible = true
    } else {
      this.aggDetailVisible = true
    }
  }
  async loadHistoryList (pageIndex) {
    const resData = {
      project: this.currentSelectedProject || null,
      limit: this.pageSize || 10,
      offset: pageIndex || 0,
      start_time_from: this.filterData.startTimeFrom,
      start_time_to: this.filterData.startTimeTo,
      latency_from: this.filterData.latencyFrom,
      latency_to: this.filterData.latencyTo,
      realization: this.filterData.realization,
      server: this.filterData.server,
      sql: this.filterData.sql,
      query_status: this.filterData.query_status
    }
    const res = await this.getHistoryList(resData)
    const data = await handleSuccessAsync(res)
    this.queryHistoryData = data
  }
  loadFilterList (data) {
    this.filterData = data
    this.pageCurrentChange(0, this.pageSize)
  }
  async created () {
    this.currentSelectedProject && this.loadHistoryList()
    const res = await this.loadOnlineQueryNodes()
    this.queryNodes = await handleSuccessAsync(res)
  }
  pageCurrentChange (offset, pageSize) {
    this.pageSize = pageSize
    this.queryCurrentPage = offset + 1
    this.loadHistoryList(offset)
  }
}
</script>

<style lang="less">
@import '../../assets/styles/variables.less';
#queryHistory {
  padding: 0 20px 50px 20px;
}
</style>
