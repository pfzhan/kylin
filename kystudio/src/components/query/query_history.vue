<template>
  <div id="queryHistory">
    <query_history_table :queryHistoryData="queryHistoryData" v-on:openAgg="openAgg" v-on:loadFilterList="loadFilterList"></query_history_table>
    <kap-pager ref="queryHistoryPager" class="ksd-center ksd-mt-20 ksd-mb-20" :totalSize="queryHistoryData.length"  v-on:handleCurrentChange='pageCurrentChange'></kap-pager>
    <el-dialog
      title="Aggregate Index"
      :visible.sync="aggDetailVisible"
      class="agg-dialog"
      width="1104px">
      <el-row :gutter="20">
        <el-col :span="16">
          <div class="cubois-chart-block">
            <div class="ksd-mt-10 ksd-mr-10 ksd-fright agg-amount-block">
              <span>Aggregate Amount</span>
              <el-input v-model.trim="cuboidCount" size="small"></el-input>
            </div>
            <PartitionChart :data="cuboids" @on-click-node="handleClickNode" :search-id="searchCuboidId" />
          </div>
        </el-col>
        <el-col :span="8">
          <el-card class="agg-detail-card">
            <div slot="header" class="clearfix">
              <span>Aggregate Detail</span>
            </div>
            <div class="detail-content">
              <el-row :gutter="5"><el-col :span="11" class="label">ID:</el-col><el-col :span="13">{{cuboidDetail.id}}</el-col></el-row>
              <el-row :gutter="5">
                <el-col :span="11" class="label">Dimension and Order:</el-col>
                <el-col :span="13"><div v-for="item in cuboidDetail.dim" :key="item" class="dim-item">{{item}}</div></el-col>
              </el-row>
              <el-row :gutter="5"><el-col :span="11" class="label">Data Size:</el-col><el-col :span="13">{{cuboidDetail.dataSize}}</el-col></el-row>
              <el-row :gutter="5"><el-col :span="11" class="label">Data Range:</el-col><el-col :span="13">{{cuboidDetail.dateFrom | gmtTime}} To {{cuboidDetail.dateTo | gmtTime}}</el-col></el-row>
              <el-row :gutter="5"><el-col :span="11" class="label">Served Query amount:</el-col><el-col :span="13">{{cuboidDetail.amount}} Query</el-col></el-row>
            </div>
          </el-card>
        </el-col>
        
      </el-row>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapActions, mapGetters } from 'vuex'
import { handleSuccessAsync, transToGmtTime } from '../../util/index'
import queryHistoryTable from './query_history_table'
import PartitionChart from '../common/PartitionChart'
@Component({
  methods: {
    ...mapActions({
      getHistoryList: 'GET_HISTORY_LIST',
      fetchCuboids: 'FETCH_CUBOIDS'
    })
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    'query_history_table': queryHistoryTable,
    PartitionChart
  }
})
export default class QueryHistory extends Vue {
  aggDetailVisible = false
  queryCurrentPage = 1
  queryHistoryData = []
  cuboidCount = 0
  cuboids = []
  cuboidDetail = {
    id: '',
    dim: [],
    dataSize: 0,
    dateFrom: 0,
    dateTo: 0,
    amount: 0
  }
  searchCuboidId = ''
  filterData = {
    startTimeFrom: null,
    startTimeTo: null,
    latencyFrom: null,
    latencyTo: null,
    realization: [],
    accelerateStatus: [],
    sql: ''
  }

  async openAgg (queryHistory) {
    this.aggDetailVisible = true

    const res = await this.fetchCuboids({modelName: queryHistory.model_name, projectName: this.currentSelectedProject})
    const data = await handleSuccessAsync(res)
    this.cuboids = formatFlowerJson(data)
    this.cuboidCount = getCuboidCounts(data)
  }

  async handleClickNode (node) {
    const cuboidId = node.cuboid.id
    const res = await this.fetchCuboid({
      projectName: this.currentSelectedProject,
      modelName: this.model.name,
      cuboidId
    })
    const cuboid = await handleSuccessAsync(res)
    this.cuboidDetail.id = cuboid.id
    this.cuboidDetail.dim = cuboid.dimensions_res
    this.cuboidDetail.dataSize = cuboid.storage_size < 1024 ? `${cuboid.storage_size}KB` : `${(cuboid.storage_size / 1024).toFixed(2)}MB`
    this.cuboidDetail.dateFrom = transToGmtTime(cuboid.start_time)
    this.cuboidDetail.dateTo = transToGmtTime(cuboid.end_time)
    if (this.cuboidDetail.dateFrom) {
      this.cuboidDetail.dateFrom = this.cuboidDetail.dateFrom.split(' GMT')[0]
    }
    if (this.cuboidDetail.dateTo) {
      this.cuboidDetail.dateTo = this.cuboidDetail.dateTo.split(' GMT')[0]
    }
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
      'realization[]': this.filterData.realization.join(','),
      'accelerateStatus[]': this.filterData.accelerateStatus.join(','),
      sql: this.filterData.sql
    }
    const res = await this.getHistoryList(resData)
    const data = await handleSuccessAsync(res)
    this.queryHistoryData = data.query_histories
  }

  loadFilterList (data) {
    this.filterData = data
    this.loadHistoryList()
  }

  created () {
    this.loadHistoryList()
  }

  pageCurrentChange (offset, pageSize) {
    this.queryCurrentPage = offset + 1
    this.loadHistoryList(offset, pageSize)
  }
}

function formatFlowerJson (data) {
  let flowers = []
  let rootLevel = 0

  data.forEach(roots => {
    let maxLevel = 0
    // 获取树的最大level
    Object.values(roots.nodes).forEach(node => {
      node.level > maxLevel && (maxLevel = node.level)
    })
    // 把根节点push进flowers数组
    roots.roots.forEach(root => {
      root = getFlowerData(root, maxLevel, true)
      flowers.push(root)
    })

    if (maxLevel > rootLevel) {
      rootLevel = maxLevel
    }
  })

  if (flowers.length === 1) {
    return flowers
  } else {
    return [{
      name: 'root',
      id: 'root',
      size: (rootLevel + 1) ** 2 * 200 + 2500,
      children: flowers
    }]
  }
}

function getCuboidCounts (data) {
  let count = 0
  data.forEach(item => {
    count += Object.keys(item.nodes).length
  })
  return count
}

function getFlowerData (parent, maxLevel, isRoot) {
  if (isRoot) {
    parent.name = parent.cuboid.id
    parent.size = (maxLevel - parent.level) ** 2 * 200 + 2500
  }
  parent.children = parent.children.map((child) => {
    child.name = child.cuboid.id
    child.size = (maxLevel - child.level) ** 2 * 200 + 2500
    child.children && child.children.length && getFlowerData(child, maxLevel)
    return child
  })

  return parent
}

</script>

<style lang="less">
  @import '../../assets/styles/variables.less';
  #queryHistory {
    padding: 0 20px 50px 20px;
    .agg-dialog {
      #visualization {
        height: 600px;
        width: 100%;
        .node {
          cursor: pointer;
          stroke: #3182bd;
          stroke-width: 1.5px;
        }
        .link {
          fill: none;
          stroke: #9ecae1;
          stroke-width: 1.5px;
        }
        .el-tooltip__popper {
          padding: 5px;
          background: @text-normal-color;
          color: #fff;
        }
      }
      .agg-amount-block {
        .el-input {
          width: 120px;
        }
      }
      .cubois-chart-block {
        border: 1px solid @line-border-color;
        height: 638px;
      }
      .agg-detail-card {
        height: 638px;
        box-shadow: none;
        .el-card__header {
          background-color: @grey-3;
          color: @text-title-color;
          font-size: 16px;
        }
        .el-card__body {
          padding: 10px;
          .detail-content {
            .el-row {
              margin-bottom: 10px;
              .dim-item {
                margin-bottom: 5px;
              }
            }
          }
        }
        .label {
          text-align: right;
        }
      }
    }
  }
</style>
