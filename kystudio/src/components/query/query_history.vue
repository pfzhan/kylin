<template>
  <div id="queryHistory">
    <query_history_table :queryHistoryData="queryHistoryData.query_histories" :queryNodes="queryNodes" v-on:openAgg="openAgg" v-on:loadFilterList="loadFilterList"></query_history_table>
    <kap-pager ref="queryHistoryPager" class="ksd-center ksd-mtb-10" :totalSize="queryHistoryData.size"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
    <el-dialog
      title="Aggregate Index"
      top="5vh"
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
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync } from '../../util/index'
import queryHistoryTable from './query_history_table'
import ModelAggregate from '../studio/StudioModel/ModelList/ModelAggregate/index.vue'
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
    ModelAggregate
  }
})
export default class QueryHistory extends Vue {
  aggDetailVisible = false
  queryCurrentPage = 1
  queryHistoryData = {}
  filterData = {
    startTimeFrom: null,
    startTimeTo: null,
    latencyFrom: null,
    latencyTo: null,
    realization: [],
    accelerateStatus: [],
    sql: ''
  }
  model = {
    uuid: ''
  }
  queryNodes = []
  async openAgg (modelId) {
    this.model.uuid = modelId
    this.aggDetailVisible = true
  }
  async loadHistoryList (pageIndex, pageSize) {
    const resData = {
      project: this.currentSelectedProject || null,
      limit: pageSize || 10,
      offset: pageIndex || 0,
      startTimeFrom: this.filterData.startTimeFrom,
      startTimeTo: this.filterData.startTimeTo,
      latencyFrom: this.filterData.latencyFrom,
      latencyTo: this.filterData.latencyTo,
      realization: this.filterData.realization,
      server: this.filterData.server,
      sql: this.filterData.sql
    }
    const res = await this.getHistoryList(resData)
    const data = await handleSuccessAsync(res)
    this.queryHistoryData = data
  }
  loadFilterList (data) {
    this.filterData = data
    this.loadHistoryList()
  }
  async created () {
    this.currentSelectedProject && this.loadHistoryList()
    const res = await this.loadOnlineQueryNodes()
    this.queryNodes = await handleSuccessAsync(res)
  }
  pageCurrentChange (offset, pageSize) {
    this.queryCurrentPage = offset + 1
    this.loadHistoryList(offset, pageSize)
  }
}
</script>

<style lang="less">
@import '../../assets/styles/variables.less';
#queryHistory {
  padding: 0 20px 50px 20px;
}
</style>
