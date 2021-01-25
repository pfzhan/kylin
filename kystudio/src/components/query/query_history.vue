<template>
  <div id="queryHistory">
    <query_history_table :queryHistoryData="queryHistoryData.query_histories" :filterDirectData="filterDirectData" :queryNodes="queryNodes" v-on:openIndexDialog="openIndexDialog" v-on:loadFilterList="loadFilterList"></query_history_table>
    <kap-pager ref="queryHistoryPager" :refTag="pageRefTags.queryHistoryPager" class="ksd-center ksd-mtb-10" :curPage="queryCurrentPage" :perPageSize="20" :totalSize="queryHistoryData.size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
    <el-dialog
      :title="$t('indexOverview')"
      top="5vh"
      limited-area
      :visible.sync="aggDetailVisible"
      class="agg-dialog"
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      width="1200px">
      <ModelAggregate
        v-if="aggDetailVisible"
        :model="model"
        :project-name="currentSelectedProject"
        :layout-id="aggIndexLayoutId"
        :is-show-aggregate-action="false"
        :isShowEditAgg="datasourceActions.includes('editAggGroup')"
        :isShowBulidIndex="datasourceActions.includes('buildIndex')"
        :isShowTableIndexActions="datasourceActions.includes('tableIndexActions')">
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
      width="1200px">
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
import { pageRefTags } from 'config'
@Component({
  beforeRouteEnter (to, from, next) {
    next(vm => {
      if (from.name && from.name === 'Dashboard' && to.params.source && to.params.source === 'homepage-history') {
        let tm = new Date(new Date().toLocaleDateString()).getTime()
        vm.filterDirectData.startTimeFrom = tm - 1000 * 60 * 60 * 24 * 7
        vm.filterDirectData.startTimeTo = tm
        return
      }
      vm.currentSelectedProject && vm.loadHistoryList()
    })
  },
  methods: {
    ...mapActions({
      getHistoryList: 'GET_HISTORY_LIST',
      loadOnlineQueryNodes: 'LOAD_ONLINE_QUERY_NODES'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'datasourceActions'
    ])
  },
  components: {
    'query_history_table': queryHistoryTable,
    ModelAggregate,
    TableIndex
  },
  locales: {
    'en': {
      indexOverview: 'Index Overview'
    },
    'zh-cn': {
      indexOverview: '索引总览'
    }
  }
})
export default class QueryHistory extends Vue {
  pageRefTags = pageRefTags
  aggDetailVisible = false
  tabelIndexVisible = false
  tabelIndexLayoutId = ''
  aggIndexLayoutId = ''
  queryCurrentPage = 1
  queryHistoryData = {}
  filterData = {
    startTimeFrom: null,
    startTimeTo: null,
    latencyFrom: null,
    latencyTo: null,
    realization: [],
    submitter: [],
    accelerateStatus: [],
    sql: '',
    query_status: []
  }
  filterDirectData = {
    startTimeFrom: null,
    startTimeTo: null
  }
  model = {
    uuid: ''
  }
  queryNodes = []
  pageSize = +localStorage.getItem(this.pageRefTags.queryHistoryPager) || 20
  async openIndexDialog ({indexType, modelId, modelAlias, layoutId}, totalList) {
    this.model.uuid = modelId
    let aggLayoutId = totalList.filter(it => it.modelAlias === modelAlias && it.layoutId).map(item => item.layoutId).join(',')
    this.aggIndexLayoutId = aggLayoutId
    this.aggDetailVisible = true
  }
  async loadHistoryList (pageIndex) {
    const resData = {
      project: this.currentSelectedProject || null,
      limit: this.pageSize || 20,
      offset: pageIndex || 0,
      start_time_from: this.filterData.startTimeFrom,
      start_time_to: this.filterData.startTimeTo,
      latency_from: this.filterData.latencyFrom,
      latency_to: this.filterData.latencyTo,
      realization: this.filterData.realization,
      submitter: this.filterData.submitter,
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
